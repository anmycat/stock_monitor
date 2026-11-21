"""eltdx / AxData 只读接入适配层（D73，2026-11-21）—— 默认关闭，零信号影响

来源：`electkismet/eltdx` v3.3.5（Rust 7709 实现）与后继 `electkismet/AxData`。
两者共享同一套 TDX 公共节点（116.205.x 等 43 台）与协议栈。

**关键实证（research/docs/45）**：
- 内置 43 节点 TCP 43/43 可达、协议握手 pong；真实日K与新浪 OHLC 收盘价差 0.0；
  1/5/15/30/60 分钟K、当日分时、逐笔 tick、7709 原生资金流全部可取。
- 本项目现有 pytdx 客户端在同一批节点上 `get_security_bars/quotes` **恒返回 0 条**
  → 旧裁定「上游停供」根因是 pytdx 实现，不是服务器。

**许可证硬约束**：两个上游均为 Research-Only / Non-Commercial 许可，明文禁止
production service 与 automated trading service。故本适配层默认关闭。

**零信号影响**：
- `ELTDX_ENABLED=false`（默认）或未安装 eltdx 包 → 所有方法返回空 DataFrame / None，
  零请求、零行为变化；
- 不注入 `_PRIORITY`、不写 `df.attrs`、不改评分/门控/Kelly/风控/回测；
- 只提供显式调用的只读取数，供研究脚本 / 手动复核 / 未来裁定后使用。

**失败语义**：任何异常（未安装、连接失败、协议错误）都返回空，不向上抛。
"""
from __future__ import annotations

import threading

import pandas as pd
from loguru import logger

from ...config import Settings, get_settings

# 本项目周期（'1'/'5'/'15'/'30'/'60'/'day'）→ eltdx period（1m/5m/.../day）
_PERIOD_MAP = {
    '1': '1m', '5': '5m', '15': '15m', '30': '30m', '60': '60m',
    '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m', '60m': '60m',
    'day': 'day', 'd': 'day', '101': 'day',
}
_OUT_COLS = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']


def to_eltdx_code(code: str) -> tuple[str, str] | None:
    """项目内码（600519 / 600519.SH）→ (eltdx 码 sh600519, 交易所 SH)。

    eltdx `normalize_code` 只接受 `sh600519`/`sz000001` 8 位前缀或裸 6 位数字；
    直接传 `600519.SH` 会抛 ProtocolError（research/docs/45 §1.2）。
    """
    sym = str(code or '').strip().lower()
    if '.' in sym:
        head, suffix = sym.split('.', 1)
        if suffix not in {'sh', 'sz', 'bj'} or not head.isdigit() or len(head) != 6:
            return None  # 带后缀但市场/代码非法：不回退成裸码（避免 000001.US 误判为深市）
        return f'{suffix}{head}', suffix.upper()
    if not sym.isdigit() or len(sym) != 6:
        return None
    if sym[0] == '6':
        return f'sh{sym}', 'SH'
    if sym[0] in {'4', '8'} or sym.startswith('92'):
        return f'bj{sym}', 'BJ'
    return f'sz{sym}', 'SZ'


def map_period(period: str) -> str | None:
    """项目周期 → eltdx period；不支持返回 None（交上层降级）。"""
    return _PERIOD_MAP.get(str(period).strip().lower())


class EltdxClient:
    """eltdx 只读薄封装：延迟导入 + 优雅降级 + 输出对齐本项目列格式。

    仅在 `ELTDX_ENABLED=true` 时才真正加载 eltdx 并发起请求；否则所有方法
    立即返回空。客户端连接是类级共享 + 锁串行（TDX 单连接非并发安全）。
    """

    def __init__(self, settings: Settings | None = None,
                 client_factory=None):
        self.cfg = settings or get_settings()
        self._factory = client_factory  # 测试注入点：注入假客户端做契约测试
        self._client = None
        self._lock = threading.Lock()

    def _enabled(self) -> bool:
        return bool(self.cfg.ELTDX_ENABLED)

    def _hosts(self) -> list[str] | None:
        raw = (self.cfg.ELTDX_HOSTS or '').strip()
        if not raw:
            return None  # 用 eltdx 内置 43 节点列表
        hosts = [h.strip() for h in raw.split(',') if h.strip()]
        return hosts or None

    def _get_client(self):
        """延迟导入 eltdx；未安装/关闭/连接失败一律返回 None。"""
        if not self._enabled():
            return None
        with self._lock:
            if self._client is not None:
                return self._client
            try:
                if self._factory is not None:
                    self._client = self._factory(self._hosts())
                else:
                    from eltdx import TdxClient  # 延迟导入：未安装时不炸导入
                    kwargs = {
                        'hosts': self._hosts(),
                        'probe_hosts': bool(self.cfg.ELTDX_PROBE_HOSTS or 0) > 0,
                    }
                    # 未指定 hosts 时不传 None 以外的自定义列表（用库默认）
                    self._client = TdxClient(**kwargs)
                return self._client
            except ImportError:
                logger.warning('[ELTDX] eltdx 未安装，适配层降级为空（零请求）')
                return None
            except Exception as e:
                logger.warning(f'[ELTDX] 客户端初始化失败 {type(e).__name__}: {e}')
                return None

    @staticmethod
    def _bars_to_df(bars) -> pd.DataFrame:
        """eltdx KlineBar 元组 → 本项目标准 DataFrame（列严格对齐）。

        单位（research/docs/45 §3 实测）：`amount` 为元；`volume_lots` 为手，
        ×100 = 股（本项目约定 volume=股，amount≈volume×close）。
        """
        records = []
        for b in bars or ():
            try:
                t = getattr(b, 'time', None)
                date = t.strftime('%Y-%m-%d %H:%M') if t is not None else ''
                lots = float(getattr(b, 'volume_lots', 0) or 0)
                records.append({
                    'date': date,
                    'open': float(getattr(b, 'open', 0) or 0),
                    'high': float(getattr(b, 'high', 0) or 0),
                    'low': float(getattr(b, 'low', 0) or 0),
                    'close': float(getattr(b, 'close', 0) or 0),
                    'volume': lots * 100.0,
                    'amount': float(getattr(b, 'amount', 0) or 0),
                })
            except Exception as e:  # 单根 bar 字段异常不应炸整帧
                logger.debug(f'[ELTDX] 跳过异常 bar {type(e).__name__}')
                continue
        if not records:
            return pd.DataFrame(columns=_OUT_COLS)
        df = pd.DataFrame(records)[_OUT_COLS]
        return df.sort_values('date').reset_index(drop=True)

    def get_minute_data(self, code: str, period: str = '5', count: int = 240) -> pd.DataFrame:
        """分钟/日K 只读取数；关闭或未安装时返回空 DataFrame。"""
        client = self._get_client()
        if client is None:
            return pd.DataFrame(columns=_OUT_COLS)
        conv = to_eltdx_code(code)
        per = map_period(period)
        if conv is None or per is None:
            return pd.DataFrame(columns=_OUT_COLS)
        try:
            with self._lock:
                series = client.bars.get(conv[0], period=per, count=int(count), adjust=None)
            return self._bars_to_df(getattr(series, 'bars', None))
        except Exception as e:
            logger.debug(f'[ELTDX] {code} {period} 取数失败 {type(e).__name__}: {e}')
            return pd.DataFrame(columns=_OUT_COLS)

    def probe(self, max_hosts: int | None = None) -> list[dict]:
        """结构化节点探测（借鉴 AxData HostProbeResult）：host/ok/latency_ms/error。

        仅显式调用才发请求；关闭时返回空列表。
        """
        client = self._get_client()
        if client is None:
            return []
        try:
            from eltdx.hosts import FALLBACK_HOSTS, probe_hosts  # 延迟导入
            hosts = self._hosts() or list(FALLBACK_HOSTS)
            if max_hosts:
                hosts = hosts[:int(max_hosts)]
            results = probe_hosts(hosts, timeout=self.cfg.ELTDX_TIMEOUT, persist=False)
            return [
                {'host': r.host, 'ok': bool(r.ok),
                 'latency_ms': r.latency_ms, 'error': r.error}
                for r in results
            ]
        except Exception as e:
            logger.debug(f'[ELTDX] 节点探测失败 {type(e).__name__}: {e}')
            return []


# 单例工厂：与 ths_financial 的 build_ths_context_provider 同款模式
_client: EltdxClient | None = None
_client_lock = threading.Lock()


def get_eltdx_client() -> EltdxClient:
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                _client = EltdxClient()
    return _client
