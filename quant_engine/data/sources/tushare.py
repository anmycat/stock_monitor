"""Tushare 数据源适配器（B7，prd/07 §15）

定位：**兜底线**——排全部免费源（腾讯/新浪/东财）之后，仅在免费源
整体不可用时经 SourceManager 自动切换。tushare 免费积分有频控
（如 daily 单次上限），不适合作主链。

依赖策略（对齐 akshare 兜底切除先例，prd/15 §P0）：
- tushare 包未安装：所有方法返回 None/空 DataFrame，**绝不向上抛异常**
- token 未配置：同上（free 层无凭据不可用）
- 网络/接口错误：记 warning 返空，交降级链

接口契约与其余源一致（CompositeSource 经 SourceManager 调度）：
- get_daily_data(code, days) -> DataFrame[date, open, high, low, close, volume, amount]
- get_stock_list() -> DataFrame[code, name, industry]
- get_realtime_quote(codes) -> None（tushare 实时行情需高积分，不实现）
"""

from __future__ import annotations

import pandas as pd
from loguru import logger

# 日期格式：Tushare 用 'YYYYMMDD'，项目内部用 'YYYY-MM-DD'
_DATE_FMT = '%Y%m%d'


def _to_internal_date(s: str) -> str:
    s = str(s)
    return f'{s[:4]}-{s[4:6]}-{s[6:8]}' if len(s) == 8 and s.isdigit() else s


class TushareSource:
    """Tushare 适配器。全静态方法，与 TencentSource/SinaSource 同风格。"""

    NAME = 'tushare'

    @staticmethod
    def _get_api():
        """延迟初始化 pro_api；不可用时返回 None（不抛异常）。"""
        try:
            import tushare as ts  # 延迟导入：未安装时不炸主链
        except ImportError:
            logger.debug('[Tushare] 包未安装，跳过')
            return None
        try:
            from ...config import get_settings
            token = getattr(get_settings(), 'TUSHARE_TOKEN', '') or ''
        except Exception as e:  # 配置读取失败按未配置处理
            logger.debug(f'[Tushare] token 读取失败: {e}')
            return None
        if not token:
            logger.debug('[Tushare] token 未配置，跳过')
            return None
        try:
            return ts.pro_api(token)
        except Exception as e:  # 初始化失败（网络/凭据）按不可用
            logger.warning(f'[Tushare] pro_api 初始化失败: {e}')
            return None

    @staticmethod
    def get_daily_data(code: str, days: int = 120) -> pd.DataFrame | None:
        """日线前复权（qfq 需积分，免费档用不复权 daily）。"""
        api = TushareSource._get_api()
        if api is None:
            return None
        try:
            end = pd.Timestamp.now().strftime(_DATE_FMT)
            start = (pd.Timestamp.now() - pd.Timedelta(days=days * 2)).strftime(_DATE_FMT)
            df = api.daily(ts_code=code, start_date=start, end_date=end)
            if df is None or df.empty:
                return None
            out = pd.DataFrame({
                'date': df['trade_date'].map(_to_internal_date),
                'open': pd.to_numeric(df['open'], errors='coerce'),
                'high': pd.to_numeric(df['high'], errors='coerce'),
                'low': pd.to_numeric(df['low'], errors='coerce'),
                'close': pd.to_numeric(df['close'], errors='coerce'),
                'volume': pd.to_numeric(df['vol'], errors='coerce'),
                'amount': pd.to_numeric(df['amount'], errors='coerce'),
            })
            # tushare daily 返回按 trade_date 倒序（最新在前），统一为正序
            # （与腾讯/新浪口径一致：最新在最后一行），再按 days 截断
            out = out.sort_values('date').reset_index(drop=True)
            return out.dropna(subset=['close']).tail(days).reset_index(drop=True)
        except Exception as e:  # 接口/网络错误按不可用
            logger.warning(f'[Tushare] daily {code} 失败: {e}')
            return None

    @staticmethod
    def get_stock_list() -> pd.DataFrame | None:
        """上市股票列表（L=上市，不含退市）。"""
        api = TushareSource._get_api()
        if api is None:
            return None
        try:
            df = api.stock_basic(
                exchange='', list_status='L',
                fields='ts_code,name,industry',
            )
            if df is None or df.empty:
                return None
            # ts_code 000001.SZ 与项目内部 code 同构，直接可用
            out = df.rename(columns={'ts_code': 'code'})[['code', 'name', 'industry']]
            return out.reset_index(drop=True)
        except Exception as e:  # 同上
            logger.warning(f'[Tushare] stock_basic 失败: {e}')
            return None

    @staticmethod
    def get_realtime_quote(codes: list[str]) -> pd.DataFrame | None:
        """实时行情：tushare 免费积分不可用，恒返 None 交降级链。"""
        return None
