"""eltdx / AxData 只读接入适配层（D73→D74，2026-11-21）

来源：`electkismet/eltdx` v3.3.5（Rust 7709 实现）与后继 `electkismet/AxData`。
两者共享同一套 TDX 公共节点（116.205.x 等 43 台）与协议栈。

**关键实证（research/docs/45）**：
- 内置 43 节点 TCP 43/43 可达、协议握手 pong；真实日K与新浪 OHLC 收盘价差 0.0；
  1/5/15/30/60 分钟K、当日分时、逐笔 tick、7709 原生资金流全部可取。
- 本项目现有 pytdx 客户端在同一批节点上 `get_security_bars/quotes` **恒返回 0 条**
  → 旧裁定「上游停供」根因是 pytdx 实现，不是服务器。

**许可证裁定（用户 2026-11-21）**：两上游均为 Research-Only / Non-Commercial，
但本项目为个人自用、非商业、不对外提供服务，用户裁定「能用上的全部用上」。
仍保留优雅降级：未安装 eltdx 包 / 连接失败时所有方法返回空，零请求零行为变化。

**零信号影响边界**：
- K 线只进 `_PRIORITY` 末位兜底，主链成功时永不触发；未装包时 `EltdxFallbackSource`
  返回 None（不是空 DF！否则会被 `execute_with_fallback` 当成功而截断兜底链）。
- 分时 / 逐笔 / 资金流 / 连板天梯 / 榜单 / 快照只作 `market_alert` 旁路证据与可观测，
  不改任何行情列/值、不写主链 `df.attrs`、不改评分/门控/Kelly/风控/回测。

**失败语义**：任何异常（未安装、连接失败、协议错误）都返回空 / None，不向上抛。
"""
from __future__ import annotations

import dataclasses as _dc
import threading
from datetime import date, datetime

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


def _to_plain(obj):
    """eltdx dataclass / 容器 → JSON-safe dict（bytes 原始字段丢弃）。"""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return None
    if _dc.is_dataclass(obj):
        return {
            f.name: _to_plain(getattr(obj, f.name))
            for f in _dc.fields(obj)
            if not f.name.endswith('_raw') and f.name != 'record_hex'
        }
    if isinstance(obj, dict):
        return {str(k): _to_plain(v) for k, v in obj.items()
                if not str(k).endswith('_raw') and str(k) != 'record_hex'}
    if isinstance(obj, (list, tuple)):
        return [_to_plain(x) for x in obj]
    return str(obj)


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
        self._warned = False  # 降级告警只打一次，避免全市场扫描日志刷屏（D70 纪律）

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
                    self._client = TdxClient(**kwargs)
                return self._client
            except ImportError:
                if not self._warned:
                    logger.warning('[ELTDX] eltdx 未安装，适配层降级为空（零请求）')
                    self._warned = True
                return None
            except Exception as e:
                if not self._warned:
                    logger.warning(f'[ELTDX] 客户端初始化失败 {type(e).__name__}: {e}')
                    self._warned = True
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
                if isinstance(t, datetime):
                    date_str = t.strftime('%Y-%m-%d %H:%M')
                elif isinstance(t, date):
                    date_str = t.strftime('%Y-%m-%d')
                else:
                    date_str = str(t or '')
                lots = float(getattr(b, 'volume_lots', 0) or 0)
                records.append({
                    'date': date_str,
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

    def _get_bars(self, code: str, period: str, count: int,
                  adjust: str | None) -> pd.DataFrame:
        """K 线取数公共实现；period 用 eltdx 原生值（1m/5m/day…）。"""
        client = self._get_client()
        if client is None:
            return pd.DataFrame(columns=_OUT_COLS)
        conv = to_eltdx_code(code)
        if conv is None:
            return pd.DataFrame(columns=_OUT_COLS)
        try:
            with self._lock:
                series = client.bars.get(conv[0], period=period,
                                         count=int(count), adjust=adjust)
            return self._bars_to_df(getattr(series, 'bars', None))
        except Exception as e:
            logger.debug(f'[ELTDX] {code} {period} 取数失败 {type(e).__name__}: {e}')
            return pd.DataFrame(columns=_OUT_COLS)

    def get_minute_data(self, code: str, period: str = '5',
                        count: int = 240) -> pd.DataFrame:
        """分钟/日K 只读取数；关闭或未安装时返回空 DataFrame。

        `period` 同时接受项目制（'1'/'5'/'day'）与 eltdx 制（'5m'/'day'）。
        """
        per = map_period(period)
        if per is None:
            return pd.DataFrame(columns=_OUT_COLS)
        return self._get_bars(code, per, count, None)

    def get_daily_data(self, code: str, days: int = 120) -> pd.DataFrame:
        """日K 只读取数，前复权对齐主链腾讯 fqkline 口径。"""
        return self._get_bars(code, 'day', int(days),
                              getattr(self.cfg, 'ELTDX_ADJUST', 'qfq'))

    # ── 旁路只读取数（market_alert 证据 / 可观测；不进行情主链）────────────
    def get_intraday(self, code: str) -> list[dict]:
        """当日分时（实测 241 点）：list[{time, price, volume, amount}]。"""
        client = self._get_client()
        if client is None:
            return []
        conv = to_eltdx_code(code)
        if conv is None:
            return []
        try:
            with self._lock:
                series = client.minutes.today(conv[0])
            points = getattr(series, 'points', None) or []
            out = []
            for p in points:
                d = _to_plain(p)
                if isinstance(d, dict):
                    out.append({
                        'time': d.get('time') or d.get('datetime'),
                        'price': d.get('price'),
                        'volume': d.get('volume'),
                        'amount': d.get('amount'),
                    })
            return out
        except Exception as e:
            logger.debug(f'[ELTDX] {code} 分时失败 {type(e).__name__}: {e}')
            return []

    def get_ticks(self, code: str, limit: int = 200) -> list[dict]:
        """逐笔成交（实测 4718 条/日）；limit 只取最新 N 条控制载荷。"""
        client = self._get_client()
        if client is None:
            return []
        conv = to_eltdx_code(code)
        if conv is None:
            return []
        try:
            with self._lock:
                ticks = client.trades.all_today(conv[0])
            seq = list(ticks or [])
            return [_to_plain(t) for t in seq[-int(limit):]]
        except Exception as e:
            logger.debug(f'[ELTDX] {code} 逐笔失败 {type(e).__name__}: {e}')
            return []

    def get_money_flow(self, code: str, days: int = 3) -> list[dict]:
        """7709 原生资金流；绕开东财 push2his 反封禁（D68 敏感点）。"""
        client = self._get_client()
        if client is None:
            return []
        conv = to_eltdx_code(code)
        if conv is None:
            return []
        try:
            with self._lock:
                block = client.money_flow.daily(conv[0])
            recs = list(getattr(block, 'records', None) or [])
            return [_to_plain(r) for r in recs[-int(days):]]
        except Exception as e:
            logger.debug(f'[ELTDX] {code} 资金流失败 {type(e).__name__}: {e}')
            return []

    def get_limit_ladder(self, codes: list[str] | None = None) -> list[dict]:
        """连板天梯（全市场梯队 / 指定票过滤）。"""
        client = self._get_client()
        if client is None:
            return []
        kwargs = {}
        if codes:
            conv = [to_eltdx_code(c) for c in codes]
            kwargs['codes'] = [c[0] for c in conv if c]
        try:
            with self._lock:
                table = client.helpers.limit_ladder(**kwargs) if kwargs \
                    else client.helpers.limit_ladder()
            return [_to_plain(r) for r in (getattr(table, 'rows', None) or [])]
        except Exception as e:
            logger.debug(f'[ELTDX] 连板天梯失败 {type(e).__name__}: {e}')
            return []

    def get_realtime_rank(self) -> list[dict]:
        """实时榜单（涨幅/封单/开盘抢筹，实测字段齐全）。"""
        client = self._get_client()
        if client is None:
            return []
        try:
            with self._lock:
                table = client.helpers.realtime_rank()
            return [_to_plain(r) for r in (getattr(table, 'rows', None) or [])]
        except Exception as e:
            logger.debug(f'[ELTDX] 榜单失败 {type(e).__name__}: {e}')
            return []

    def get_quotes(self, codes: list[str]) -> list[dict]:
        """报价快照（含买卖盘口 / 涨停封单），小批量显式调用。"""
        client = self._get_client()
        if client is None or not codes:
            return []
        conv = [to_eltdx_code(c) for c in codes]
        eltdx_codes = [c[0] for c in conv if c]
        if not eltdx_codes:
            return []
        try:
            with self._lock:
                quotes = client.helpers.full_quotes(eltdx_codes)
            return [_to_plain(q) for q in (quotes or [])]
        except Exception as e:
            logger.debug(f'[ELTDX] 快照失败 {type(e).__name__}: {e}')
            return []

    def market_alert_context(self, codes: list[str] | None = None) -> dict | None:
        """聚合盘面证据包（类 D71）；失败/关闭返回 None 让上层静默降级。

        只含展示用证据字段：榜单、资金流、封单。不含 seal_rate /
        open_stock_count（那是 THS 专属口径，避免混淆来源）。

        性能纪律：连板天梯 `limit_ladder` 即使传 codes 也要 ~6.6s（内部拉全市场
        后过滤），故**不进**每轮告警路径；仅作为显式研究 API `get_limit_ladder()`
        按需调用。本 context 只用已实测轻量的接口（榜单/快照/资金流）。
        """
        if not self._get_client():
            return None
        rank = self.get_realtime_rank()
        ctx = {
            'source': 'eltdx(TDX 7709)',
            'observed_at': datetime.now().isoformat(timespec='seconds'),
            'hot_rank': rank[:20],
        }
        if codes:
            ctx['money_flow'] = {c: self.get_money_flow(c) for c in codes[:10]}
            ctx['seal_quotes'] = self.get_quotes(codes[:10])
        return ctx if (rank or ctx.get('money_flow')) else None

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


class EltdxFallbackSource:
    """降级链专用薄包装：把 EltdxClient 的空 DataFrame 转成 None。

    `source_manager.execute_with_fallback` 用 `if result is not None` 判成功；
    空 DataFrame 不是 None 会被当成功，从而**截断**后续兜底与 stale snapshot。
    未装 eltdx / 关闭 / 取数失败时本包装一律返回 None，降级链无缝跳过 → 零影响。
    """

    def __init__(self, client: EltdxClient | None = None):
        self._client = client or get_eltdx_client()

    def get_daily_data(self, code: str, days: int = 120):
        df = self._client.get_daily_data(code, days)
        return df if df is not None and not df.empty else None

    def get_minute_data(self, code: str, period: str = '5'):
        df = self._client.get_minute_data(code, period)
        return df if df is not None and not df.empty else None


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


def build_eltdx_context_provider(settings: Settings | None = None,
                                 codes: list[str] | None = None):
    """按开关构造 market_alert 证据 provider；关闭返回 None（零请求）。"""
    cfg = settings or get_settings()
    if not getattr(cfg, 'ELTDX_MARKET_ALERT_ENRICH', False):
        return None
    client = get_eltdx_client()
    return lambda: client.market_alert_context(codes)
