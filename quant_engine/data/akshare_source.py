"""AKShare数据源 - 基于最新akshare文档优化"""
import time
from datetime import datetime
import pandas as pd
from typing import List, Dict, Optional, Callable
from loguru import logger


class AKShareSource:
    """AKShare数据源 - 使用专用接口获取数据"""

    _max_retries = 2
    _retry_sleep = 0.5
    _failure_threshold = 3
    _cooldown_seconds = 120

    def __init__(self):
        try:
            import akshare as ak
            self.ak = ak
            self._available = True
            self._failure_count = 0
            self._cooldown_until = 0.0
            self._last_error_log = 0.0
            logger.info("AKShare initialized")
        except ImportError:
            self._available = False
            logger.warning("AKShare not installed")

    def _call(self, action: str, func: Callable[[], pd.DataFrame]) -> Optional[pd.DataFrame]:
        if not self._available:
            return None

        now = time.time()
        if now < self._cooldown_until:
            logger.debug(f"AKShare {action} skipped during cooldown")
            return None

        last_error: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 1):
            try:
                result = func()
                if result is None:
                    raise ValueError("empty response")
                if hasattr(result, "empty") and getattr(result, "empty"):
                    raise ValueError("empty dataframe")
                self._failure_count = 0
                return result
            except Exception as exc:
                last_error = exc
                if attempt < self._max_retries:
                    time.sleep(self._retry_sleep * attempt)

        self._failure_count += 1
        if now - self._last_error_log >= 60 or self._failure_count == 1:
            logger.warning(f"AKShare {action} unavailable: {type(last_error).__name__}: {last_error}")
            self._last_error_log = now
        else:
            logger.debug(f"AKShare {action} unavailable: {type(last_error).__name__}: {last_error}")

        if self._failure_count >= self._failure_threshold:
            self._cooldown_until = now + self._cooldown_seconds
            self._failure_count = 0
            logger.warning(f"AKShare entering cooldown for {self._cooldown_seconds}s")
        return None

    def get_all_stocks(self) -> Optional[pd.DataFrame]:
        """获取所有A股实时行情"""
        df = self._call("get_all_stocks", lambda: self.ak.stock_zh_a_spot_em())
        if df is None:
            return None
        try:
            df = df.rename(columns={
                '代码': 'code',
                '名称': 'name',
                '最新价': 'price',
                '涨跌幅': 'change_pct',
                '成交量': 'volume',
                '成交额': 'amount',
                '换手率': 'turnover',
                '市盈率-动态': 'pe',
                '市净率': 'pb',
                '总市值': 'market_cap',
            })
            df['code'] = df['code'].apply(lambda x: f"{x}.SZ" if str(x).startswith(('0', '3')) else f"{x}.SH")
            return df
        except Exception as e:
            logger.warning(f"AKShare get_all_stocks transform error: {e}")
            return None

    def get_etf_quotes(self) -> Optional[pd.DataFrame]:
        """获取ETF实时行情"""
        df = self._call("get_etf_quotes", lambda: self.ak.fund_etf_spot_em())
        if df is None:
            return None
        try:
            df = df.rename(columns={
                '代码': 'code',
                '名称': 'name',
                '最新价': 'price',
                '涨跌幅': 'change_pct',
                '成交量': 'volume',
            })
            df['code'] = df['code'].apply(lambda x: f"{x}.SZ" if str(x).startswith(('0', '1')) else f"{x}.SH")
            return df
        except Exception as e:
            logger.warning(f"AKShare get_etf_quotes transform error: {e}")
            return None

    def get_zt_pool(self) -> Optional[pd.DataFrame]:
        """获取涨停股池"""
        return self._call("get_zt_pool", lambda: self.ak.stock_zt_pool_em(date=datetime.now().strftime("%Y%m%d")))

    def get_dt_pool(self) -> Optional[pd.DataFrame]:
        """获取跌停股池"""
        return self._call("get_dt_pool", lambda: self.ak.stock_zt_pool_dtgc_em(date=datetime.now().strftime("%Y%m%d")))

    def get_zb_pool(self) -> Optional[pd.DataFrame]:
        """获取炸板股池"""
        return self._call("get_zb_pool", lambda: self.ak.stock_zt_pool_zbgc_em(date=datetime.now().strftime("%Y%m%d")))

    def get_industry_boards(self) -> Optional[pd.DataFrame]:
        """获取行业板块数据"""
        return self._call("get_industry_boards", lambda: self.ak.stock_board_industry_name_em())

    def get_concept_boards(self) -> Optional[pd.DataFrame]:
        """获取概念板块数据"""
        return self._call("get_concept_boards", lambda: self.ak.stock_board_concept_name_em())

    def get_fund_flow(self, code: str) -> Optional[pd.DataFrame]:
        """获取个股资金流向"""
        symbol = code.split('.')[0]
        market = "sh" if code.endswith('.SH') else "sz"
        return self._call(
            "get_fund_flow",
            lambda: self.ak.stock_individual_fund_flow(stock=symbol, market=market),
        )

    def get_daily_data(self, code: str, days: int = 120) -> Optional[pd.DataFrame]:
        """获取日线数据"""
        symbol = code.split('.')[0]
        df = self._call(
            "get_daily_data",
            lambda: self.ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq"),
        )
        if df is None:
            return None
        try:
            df = df.rename(columns={
                '日期': 'trade_date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
            })
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
            return df[['trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount']].tail(days)
        except Exception as e:
            logger.warning(f"AKShare get_daily_data transform error: {e}")
            return None

    def get_minute_data(self, code: str, period: str = "5") -> Optional[pd.DataFrame]:
        """获取分钟线数据"""
        symbol = code.split('.')[0]
        df = self._call(
            "get_minute_data",
            lambda: self.ak.stock_zh_a_hist_min_em(symbol=symbol, period=period, adjust="qfq"),
        )
        if df is None:
            return None
        try:
            df = df.rename(columns={
                '时间': 'time',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
            })
            return df[['time', 'open', 'high', 'low', 'close', 'volume']]
        except Exception as e:
            logger.warning(f"AKShare get_minute_data transform error: {e}")
            return None

    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """获取股票列表"""
        df = self._call("get_stock_list", lambda: self.ak.stock_zh_a_spot_em())
        if df is None:
            return None
        try:
            result = df[['代码', '名称']].copy()
            result.columns = ['code', 'name']
            result['code'] = result['code'].apply(lambda x: f"{x}.SZ" if str(x).startswith(('0', '3')) else f"{x}.SH")
            result['industry'] = '未知'
            return result
        except Exception as e:
            logger.warning(f"AKShare get_stock_list transform error: {e}")
            return None
