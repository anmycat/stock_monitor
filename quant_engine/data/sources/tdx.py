"""通达信数据源（pytdx，TCP 7709 协议）——分钟线专用

2026-10-20 调研实测结论（PRD 02 §3.9）：
- 30 台候选服务器 TCP 全通，但协议级可用仅 117.34.114.x 集群（本机 2/20 可靠），
  且存在"TCP 通但握手超时/返空"的抖动——**服务器池 + 自动探活 + 重试是刚需**
- 5/15/30/60 分钟K 完整（category 0/1/2/3，5 分钟实测 48 根/天）；
  1 分钟K 不支持（category 4 每天仅返 1 根收盘价）
- 成交量单位实测为**股**（600000 日线 605132 ≈ 腾讯 605133，无需换算）
- 北交所不支持（920819 两市场均空）；证券列表分页无效（不可作 stock_list）

定位：minute 第一线——gates._check_minute_trigger 恰需 get_minute_data(code, "5")，
本源正好补齐该断链（接入前无任何源实现该方法，akshare 亦未安装）。
任何失败都返回空 DataFrame 交降级链处理，绝不向上抛异常。
"""
import threading
import time
from typing import List, Dict, Optional

import pandas as pd
from loguru import logger

from ...utils.code_utils import split_code, MARKET_SH, MARKET_SZ, MARKET_BJ


# 候选服务器池（2026-10-20 实测：117.34.114.x 集群协议级可用，置顶优先；
# 其余为 rust-tdx 2026-07 / autoxd 2026-09 公开列表，TCP 通但协议多不可用，
# 保留作探活轮换候选——集群整体抖动时增加恢复机会）
TDX_SERVERS = [
    ('117.34.114.13', 7709),
    ('117.34.114.14', 7709),
    ('117.34.114.15', 7709),
    ('117.34.114.16', 7709),
    ('117.34.114.17', 7709),
    ('117.34.114.18', 7709),
    ('117.34.114.20', 7709),
    ('117.34.114.27', 7709),
    ('119.147.212.81', 7709), ('218.85.139.19', 7709), ('218.85.139.20', 7709),
    ('58.23.131.163', 7709), ('218.6.170.47', 7709), ('123.125.108.14', 7709),
    ('180.153.18.170', 7709), ('202.108.253.130', 7709), ('60.191.117.167', 7709),
    ('115.238.56.198', 7709), ('218.75.126.9', 7709), ('115.238.90.165', 7709),
    ('124.160.88.183', 7709), ('60.12.136.250', 7709), ('218.108.98.244', 7709),
    ('218.108.47.69', 7709), ('223.94.89.115', 7709), ('218.57.11.101', 7709),
    ('58.58.33.123', 7709), ('14.17.75.71', 7709), ('114.80.63.12', 7709),
    ('114.80.63.35', 7709), ('180.153.39.51', 7709), ('221.231.141.60', 7709),
    ('101.227.73.20', 7709), ('101.227.77.254', 7709), ('14.215.128.18', 7709),
    ('59.36.5.11', 7709),
]

# 周期 → TDX K线 category（1 分钟 category=4 实测不可用，不支持）
PERIOD_TO_CATEGORY = {'5': 0, '15': 1, '30': 2, '60': 3}

# 市场 → TDX market id（TDX 协议：0=深，1=沪；北交所无 market，不支持）
MARKET_TO_TDX = {MARKET_SZ: 0, MARKET_SH: 1}

# 单次取数根数：5 分钟 240 根 = 5 个交易日，覆盖 gates 所需的 ≥10 根余量
DEFAULT_BAR_COUNT = 240

# 探活验证码（600000 沪市，协议级可用性判据）
_PROBE_MARKET, _PROBE_CODE = 1, '600000'


class TdxSource:
    """通达信分钟线数据源（类级共享连接 + 服务器池探活，线程安全）"""

    # 类级共享状态：worker / api 多线程共用一条 TCP 连接（TDX 连接非并发安全，
    # 用锁串行化；连接跨调用复用避免每次探活）
    _lock = threading.Lock()
    _api = None                 # TdxHq_API 实例
    _server: Optional[str] = None   # 当前可用服务器 "ip:port"
    _probe_time: float = 0.0    # 上次探活时间戳
    _pytdx_checked = False      # 是否已做过 pytdx 导入检查
    _pytdx_available = False

    # 探活结果缓存时长：集群稳定期不重复探活（每次调用都探会拖慢降级链）
    PROBE_TTL = 60.0
    CONNECT_TIMEOUT = 4.0

    # ---------------------------------------------------------------- 连接管理

    @classmethod
    def _load_pytdx(cls):
        """懒加载 pytdx（未安装时不炸导入链，仅记一次 warning）"""
        if not cls._pytdx_checked:
            cls._pytdx_checked = True
            try:
                from pytdx.hq import TdxHq_API  # noqa: F401
                cls._pytdx_available = True
            except ImportError:
                cls._pytdx_available = False
                logger.warning(
                    "TdxSource: pytdx 未安装（pip install pytdx），minute 链路将降级")
        return cls._pytdx_available

    @classmethod
    def _disconnect(cls):
        """断开并清空当前连接（调用方需持有 _lock）"""
        if cls._api is not None:
            try:
                cls._api.disconnect()
            except Exception:
                pass
        cls._api = None
        cls._server = None

    @classmethod
    def _try_server(cls, server: tuple) -> bool:
        """尝试连接单台服务器并做协议级验证（TCP 通≠协议可用，必须取一次数）"""
        from pytdx.hq import TdxHq_API
        ip, port = server
        api = TdxHq_API(heartbeat=False)
        try:
            if not api.connect(ip, port, time_out=cls.CONNECT_TIMEOUT):
                return False
            q = api.get_security_quotes([(_PROBE_MARKET, _PROBE_CODE)])
            if not q or not q[0].get('price', 0) > 0:
                return False
            # 验证通过：替换共享连接
            cls._disconnect()
            cls._api = api
            cls._server = f"{ip}:{port}"
            cls._probe_time = time.time()
            logger.info(f"TdxSource: 服务器探活成功 {cls._server}")
            return True
        except Exception as e:
            logger.debug(f"TdxSource: {ip}:{port} 探活失败 {type(e).__name__}")
            try:
                api.disconnect()
            except Exception:
                pass
            return False

    @classmethod
    def _get_api(cls):
        """获取可用连接：缓存复用 → 优先当前服务器 → 池内轮换探活

        Returns:
            TdxHq_API 实例或 None（全部不可用）
        """
        with cls._lock:
            if not cls._load_pytdx():
                return None
            # 1) 连接在且探活未过期 → 复用（不逐次验证，省请求）
            if cls._api is not None and time.time() - cls._probe_time < cls.PROBE_TTL:
                return cls._api
            # 2) 探活已过期但连接还在：轻量心跳验证一次
            if cls._api is not None:
                try:
                    if cls._api.get_security_quotes([(_PROBE_MARKET, _PROBE_CODE)]):
                        cls._probe_time = time.time()
                        return cls._api
                except Exception:
                    pass
                cls._disconnect()
            # 3) 连接不可用：当前服务器优先重连，失败则池内轮换
            if cls._server:
                ip, port = cls._server.split(':')
                if cls._try_server((ip, int(port))):
                    return cls._api
            for server in TDX_SERVERS:
                if cls._try_server(server):
                    return cls._api
            logger.warning("TdxSource: 服务器池全部不可用")
            return None

    # ---------------------------------------------------------------- 数据接口

    @staticmethod
    def _convert_code(code: str) -> Optional[tuple]:
        """标准代码 → (tdx_market, symbol)；北交所/无法解析返回 None"""
        parts = split_code(code)
        if not parts:
            logger.debug(f"TdxSource: 代码解析失败 {code!r}")
            return None
        symbol, market = parts
        tdx_market = MARKET_TO_TDX.get(market)
        if tdx_market is None:
            logger.debug(f"TdxSource: 不支持的市场 {code}（TDX 无北交所）")
            return None
        return tdx_market, symbol

    @staticmethod
    def get_minute_data(code: str, period: str = "5") -> pd.DataFrame:
        """获取分钟K线（默认 5 分钟）

        Args:
            code: 标准代码，如 '600000.SH'
            period: '5'/'15'/'30'/'60'（'1' 分钟 TDX 不支持，返回空）

        Returns:
            DataFrame[date, open, high, low, close, volume, amount]，按时间升序；
            任何失败返回空 DataFrame（不抛异常，交降级链）
        """
        category = PERIOD_TO_CATEGORY.get(str(period))
        if category is None:
            logger.debug(f"TdxSource: 不支持的周期 {period}（可用 5/15/30/60）")
            return pd.DataFrame()

        converted = TdxSource._convert_code(code)
        if converted is None:
            return pd.DataFrame()
        tdx_market, symbol = converted

        # 两次尝试：首次失败 → 强制重探服务器 → 再试一次
        for attempt in range(2):
            api = TdxSource._get_api()
            if api is None:
                return pd.DataFrame()
            try:
                with TdxSource._lock:
                    bars = api.get_security_bars(
                        category, tdx_market, symbol, 0, DEFAULT_BAR_COUNT)
                if not bars:
                    logger.debug(f"TdxSource: {code} {period}分钟无数据")
                    return pd.DataFrame()
                return TdxSource._to_dataframe(bars)
            except Exception as e:
                logger.warning(
                    f"TdxSource: {code} {period}分钟取数失败（第{attempt + 1}次）"
                    f"{type(e).__name__}: {e}")
                with TdxSource._lock:
                    TdxSource._disconnect()
        return pd.DataFrame()

    @staticmethod
    def _to_dataframe(bars: List[Dict]) -> pd.DataFrame:
        """TDX bar 列表 → 标准 DataFrame（datetime 'YYYY-MM-DD HH:MM' 字符串）

        实测（2026-10-20 真实 pytdx）：get_security_bars 返回为**旧→新升序**
        （bars[-1] 即最新根）。此处仍按日期显式排序，不依赖上游顺序假设
        （date 为定长格式，字典序 = 时间序）。
        """
        records = [{
            'date': bar['datetime'],
            'open': float(bar['open']),
            'high': float(bar['high']),
            'low': float(bar['low']),
            'close': float(bar['close']),
            'volume': float(bar['vol']),    # TDX 单位实测为股，无需换算
            'amount': float(bar['amount']),
        } for bar in bars]
        df = pd.DataFrame(records)
        return df.sort_values('date').reset_index(drop=True)
