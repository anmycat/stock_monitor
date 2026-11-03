"""AKShare 数据源（D53 遗留修复 #3：AKShare 接入）

定位与纪律（依据 research/docs/29 免费数据源调研）：

  AKShare **不进盘中主循环**，只做"夜间批量 + 补充能力"：
  - 依赖重（16 个，含 curl_cffi TLS 伪装 + mini-racer 跑 JS），安装慢
  - 底层多为爬虫接口：`_em`（东财）= 与现有源同源（不构成降级）、
    `_sina` 自带"大量抓取容易封 IP"警告、`_ths` 反爬严
  - 字段名随版本变（如 stock_zh_a_hist_tx 把成交量叫 amount）
  → 因此：① 全部方法带限速与失败降级 ② 取数即落库（落 CSV/JSON）
    ③ 不注册进 _PRIORITY 主链（避免盘中触发风控）

  可选依赖：未安装时所有方法返回空 DataFrame / None（不抛异常），
  与项目 TushareSource 的既有约定一致。

  生产实测：ECS 已装 akshare 1.18.64（python3.14）；本地 venv 为 py3.9，
  低于 akshare 的 requires-python>=3.11 → 本地只能走"可选依赖"路径。
"""
from __future__ import annotations

import random
import threading
import time

import pandas as pd
from loguru import logger

# akshare 未安装时的可用性标记（与 hmm_detector 的 HMM_AVAILABLE 同风格）
try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
    AKSHARE_VERSION = getattr(ak, '__version__', 'unknown')
except ImportError:  # pragma: no cover - 环境相关
    ak = None
    AKSHARE_AVAILABLE = False
    AKSHARE_VERSION = ''


class _Throttle:
    """AKShare 限速：请求间隔 ≥0.5s + 随机抖动（调研：<=30 次/分钟、<=500 次/天）"""

    MIN_INTERVAL = 0.5
    JITTER_RANGE = (0.1, 0.5)

    def __init__(self):
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._last
            base = max(0.0, self.MIN_INTERVAL - elapsed)
            time.sleep(base + random.uniform(*self.JITTER_RANGE))
            self._last = time.monotonic()


_THROTTLE = _Throttle()


class AkshareSource:
    """AKShare 适配层（可选依赖，夜间批量用）"""

    @staticmethod
    def available() -> bool:
        """akshare 是否可用"""
        return AKSHARE_AVAILABLE

    @staticmethod
    def _call(func_name: str, **kwargs):
        """调用 akshare 函数（带限速 + 失败降级）

        Returns:
            DataFrame 或 None（失败/未安装时）
        """
        if not AKSHARE_AVAILABLE:
            logger.debug(f'akshare 未安装，跳过 {func_name}')
            return None
        func = getattr(ak, func_name, None)
        if func is None:
            logger.warning(f'akshare 无接口 {func_name}（版本 {AKSHARE_VERSION}）')
            return None
        _THROTTLE.wait()
        try:
            df = func(**kwargs)
            if df is None or len(df) == 0:
                logger.info(f'akshare {func_name} 返回空')
                return None
            return df
        except Exception as e:  # 外部库异常统一降级
            logger.warning(f'akshare {func_name} 失败: {type(e).__name__}: {e}')
            return None

    # ---------------- 涨停/跌停/炸板池（补充东财 push2ex 的非交易时段盲区）----------------

    @staticmethod
    def get_limit_up_pool(date: str = '') -> pd.DataFrame:
        """涨停池（akshare stock_zt_pool_em）

        Args:
            date: YYYYMMDD（**必填**：akshare 该接口默认值是过时的 20241008，
                  不传会取到空/历史日期的数据）
        Returns:
            DataFrame[code/name/price/change_pct/amount/turnover_rate/seal_fund/
            first_seal_time/last_seal_time/broken_times/limit_up_stat]

        实测列（2026-10-30，47 行）：序号/代码/名称/涨跌幅/最新价/成交额/
        流通市值/总市值/换手率/封板资金/首次封板时间/最后封板时间/炸板次数/
        涨停统计 —— **无「连板数」与「所属行业」列**（连板信息在「涨停统计」，
        精确连板数与行业请用东财 push2ex 的 em_pool 源）
        """
        kwargs = {'date': date} if date else {}
        df = AkshareSource._call('stock_zt_pool_em', **kwargs)
        if df is None:
            return pd.DataFrame()
        out = pd.DataFrame()
        out['code'] = df.get('代码', pd.Series(dtype=str)).astype(str)
        out['name'] = df.get('名称', pd.Series(dtype=str)).astype(str)
        out['price'] = pd.to_numeric(df.get('最新价', pd.Series(dtype=float)), errors='coerce')
        out['change_pct'] = pd.to_numeric(df.get('涨跌幅', pd.Series(dtype=float)), errors='coerce')
        out['amount'] = pd.to_numeric(df.get('成交额', pd.Series(dtype=float)), errors='coerce')
        out['turnover_rate'] = pd.to_numeric(df.get('换手率', pd.Series(dtype=float)), errors='coerce')
        out['seal_fund'] = pd.to_numeric(df.get('封板资金', pd.Series(dtype=float)), errors='coerce')
        out['first_seal_time'] = df.get('首次封板时间', pd.Series(dtype=str)).astype(str)
        out['last_seal_time'] = df.get('最后封板时间', pd.Series(dtype=str)).astype(str)
        out['broken_times'] = pd.to_numeric(df.get('炸板次数', pd.Series(dtype=float)), errors='coerce')
        # akshare 的涨停池无精确连板数列（该列在东财 push2ex 源 em_pool 中）
        out['limit_up_times'] = None
        # 「涨停统计」是描述性文本（如 '3天2板' / '天'）
        out['limit_up_stat'] = df.get('涨停统计', pd.Series(dtype=str)).astype(str)
        out['industry'] = ''
        # 代码补市场后缀（沪 60/68/科创 688 → SH；深 00/30 → SZ；8/4/92 → BJ）
        def _suffix(code: str) -> str:
            c = str(code)
            if c.startswith('6'):
                return f'{c}.SH'
            if c.startswith(('4', '8', '92')):
                return f'{c}.BJ'
            return f'{c}.SZ'
        out['code'] = out['code'].map(_suffix)
        return out

    @staticmethod
    def get_limit_down_pool(date: str = '') -> pd.DataFrame:
        """跌停池（akshare stock_dt_pool_em）"""
        kwargs = {'date': date} if date else {}
        df = AkshareSource._call('stock_dt_pool_em', **kwargs)
        if df is None:
            return pd.DataFrame()
        out = pd.DataFrame()
        out['code'] = df.get('代码', pd.Series(dtype=str)).astype(str)
        out['name'] = df.get('名称', pd.Series(dtype=str)).astype(str)
        out['price'] = pd.to_numeric(df.get('最新价', pd.Series(dtype=float)), errors='coerce')
        out['change_pct'] = pd.to_numeric(df.get('涨跌幅', pd.Series(dtype=float)), errors='coerce')
        out['seal_fund'] = pd.to_numeric(df.get('封单资金', pd.Series(dtype=float)), errors='coerce')
        out['open_times'] = pd.to_numeric(df.get('板上连续跌停', pd.Series(dtype=float)), errors='coerce')
        out['industry'] = df.get('所属行业', pd.Series(dtype=str)).astype(str)
        if out.empty:
            return out
        return out

    # ---------------- 行业 / 指数成分 ----------------

    @staticmethod
    def get_stock_industry() -> pd.DataFrame:
        """全市场行业分类（akshare stock_board_industry_name_em 逐板块取成分较慢）"""
        df = AkshareSource._call('stock_industry_category_cninfo')
        if df is None:
            return pd.DataFrame()
        out = pd.DataFrame()
        out['code'] = df.get('代码', pd.Series(dtype=str)).astype(str)
        out['name'] = df.get('简称', pd.Series(dtype=str)).astype(str)
        out['industry'] = df.get('行业分类', df.get('行业', pd.Series(dtype=str))).astype(str)
        return out

    @staticmethod
    def get_index_cons(index_code: str = '000300') -> pd.DataFrame:
        """指数成分股（akshare index_stock_cons）

        Args:
            index_code: 指数代码，如 '000300'（沪深300）/ '000001'（上证指数）
        """
        df = AkshareSource._call('index_stock_cons', symbol=index_code)
        if df is None:
            return pd.DataFrame()
        out = pd.DataFrame()
        out['code'] = df.get('成分券代码', pd.Series(dtype=str)).astype(str)
        out['name'] = df.get('成分券名称', pd.Series(dtype=str)).astype(str)
        out['weight'] = pd.to_numeric(df.get('权重', pd.Series(dtype=float)), errors='coerce')
        return out

    @staticmethod
    def get_all_stocks() -> pd.DataFrame:
        """全市场 A 股列表（akshare stock_info_a_code_name）"""
        df = AkshareSource._call('stock_info_a_code_name')
        if df is None:
            return pd.DataFrame()
        out = pd.DataFrame()
        out['code'] = df.get('code', pd.Series(dtype=str)).astype(str)
        out['name'] = df.get('name', pd.Series(dtype=str)).astype(str)
        return out

    @staticmethod
    def get_trade_calendar() -> pd.DataFrame:
        """A 股交易日历（akshare tool_trade_date_hist_sina，无参数）

        实测返回 8797 个交易日、覆盖到 2026-12-31 —— 可作为项目 2026 静态
        休市日表的**独立交叉校验**（D53 已用其验证：项目 19 个休市工作日
        与权威日历零冲突）。
        """
        df = AkshareSource._call('tool_trade_date_hist_sina')
        if df is None:
            return pd.DataFrame()
        out = pd.DataFrame()
        out['date'] = df.get('trade_date', pd.Series(dtype=str)).astype(str)
        return out
