"""东财 push2ex 涨停/炸板/跌停池（D53，2026-11-01）

调研实测（research/docs/17 免费数据源降级备路调研报告）：

  同源换 host 救回盘面特色数据——生产 ECS 从 push2/push2his 取行情/资金流
  会被 IP 风控（capital_flow_5d 仅 3/33 非空），但 **push2ex** 的三个池
  仍可正常返回：

    涨停池 GET https://push2ex.eastmoney.com/getTopicZTPool
    炸板池 GET https://push2ex.eastmoney.com/getTopicZBPool
    跌停池 GET https://push2ex.eastmoney.com/getTopicDTPool

  公共参数：ut=7eea3edcaed734bea9cbfc24409ed989&dpt=wz.ztzt
           &Pageindex=0&pagesize=N&sort=fbt:asc&date=YYYYMMDD

  pool 元素字段：c=代码 m=市场(1沪/0深) n=名称 p=最新价(×1000)
                 zdp=涨跌幅% amount=成交额 ltsz=流通市值 hs=换手%
                 lbc=连板数 fbt=首次封板 lbt=最后封板 fund=封板资金
                 zbc=炸板次数 hybk=所属行业 zttj={days,ct}

风控对策：与现有东财源同一体系 → 复用 eastmoney 的限速（≥1s）+ 随机抖动
+ 连接复用（同一 Session / 同一 _THROTTLE），并计入 SourceManager 的
冷却与熔断语义。
"""
from __future__ import annotations

import pandas as pd
from loguru import logger

from .eastmoney import EastMoneySource

_UT = '7eea3edcaed734bea9cbfc24409ed989'
_DPT = 'wz.ztzt'


class EastMoneyPoolSource:
    """东财涨停/炸板/跌停池（同源换 host，独立于行情主链路）"""

    HOST = 'https://push2ex.eastmoney.com'

    @staticmethod
    def _market_suffix(m: int) -> str:
        """东财市场字段 → 市场后缀（m=1 沪，m=0 深/北）"""
        return 'SH' if int(m) == 1 else 'SZ'

    @classmethod
    def _fetch_pool(cls, endpoint: str, date_str: str, page_size: int = 200) -> list[dict]:
        """抓取指定池（返回原始 pool 列表；失败返空）"""
        params = {
            'ut': _UT, 'dpt': _DPT,
            'Pageindex': '0', 'pagesize': str(page_size),
            'sort': 'fbt:asc', 'date': date_str,
        }
        data = EastMoneySource._get_json(f'{cls.HOST}/{endpoint}', params)
        if not data:
            return []
        payload = data.get('data') or {}
        return payload.get('pool') or []

    @classmethod
    def _to_frame(cls, pool: list[dict]) -> pd.DataFrame:
        """pool 元素 → 标准 DataFrame"""
        records = []
        for item in pool:
            try:
                code = str(item.get('c') or '')
                if not code:
                    continue
                price = float(item.get('p') or 0)
                records.append({
                    'code': f'{code}.{cls._market_suffix(item.get("m", 1))}',
                    'name': item.get('n', ''),
                    'price': price / 1000.0 if price else 0.0,   # 东财价格 ×1000
                    'change_pct': float(item.get('zdp') or 0),
                    'amount': float(item.get('amount') or 0),
                    'float_market_cap': float(item.get('ltsz') or 0),
                    'turnover_rate': float(item.get('hs') or 0),
                    'limit_up_times': int(item.get('lbc') or 0),      # 连板数
                    'first_seal_time': int(item.get('fbt') or 0),    # 首次封板
                    'last_seal_time': int(item.get('lbt') or 0),     # 最后封板
                    'seal_fund': float(item.get('fund') or 0),       # 封板资金
                    'broken_times': int(item.get('zbc') or 0),       # 炸板次数
                    'industry': item.get('hybk', ''),
                })
            except (TypeError, ValueError):
                continue
        return pd.DataFrame(records)

    @classmethod
    def get_limit_up_pool(cls, date_str: str) -> pd.DataFrame:
        """涨停池（含连板数/封板资金/炸板次数/行业）"""
        try:
            return cls._to_frame(cls._fetch_pool('getTopicZTPool', date_str))
        except Exception as e:  # 盘面池失败不影响主链
            logger.warning(f"EM limit-up pool error: {e}")
            return pd.DataFrame()

    @classmethod
    def get_broken_board_pool(cls, date_str: str) -> pd.DataFrame:
        """炸板池（曾涨停但打开）"""
        try:
            return cls._to_frame(cls._fetch_pool('getTopicZBPool', date_str))
        except Exception as e:
            logger.warning(f"EM broken-board pool error: {e}")
            return pd.DataFrame()

    @classmethod
    def get_limit_down_pool(cls, date_str: str) -> pd.DataFrame:
        """跌停池"""
        try:
            return cls._to_frame(cls._fetch_pool('getTopicDTPool', date_str))
        except Exception as e:
            logger.warning(f"EM limit-down pool error: {e}")
            return pd.DataFrame()
