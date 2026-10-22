"""新浪财经数据源"""
import json
import re
from typing import Dict, List, Optional

import pandas as pd
import requests
from loguru import logger

from ...utils.code_utils import (
    MARKET_BJ,
    MARKET_SH,
    MARKET_SZ,
    normalize_code,
    to_sina_code,
)


class SinaSource:
    """新浪财经数据源"""

    BASE_URL = "https://hq.sinajs.cn/list="
    # 全市场列表/快照（Market_Center，node=hs_a 覆盖沪深北 A 股，2026-10-20 实测）
    LIST_URL = ("https://vip.stock.finance.sina.com.cn/quotes_service/api/"
                "json_v2.php/Market_Center.getHQNodeData")
    # 日线 K 线（2026-10-20 实测可用，字段 day/open/high/low/close/volume）
    DAILY_URL = ("https://quotes.sina.cn/cn/api/json_v2.php/"
                 "CN_MarketDataService.getKLineData")
    # 列表分页：每页条数与最大页数（5400+ 只全市场 ≈ 55 页）
    LIST_PAGE_SIZE = 100
    LIST_MAX_PAGES = 60

    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://finance.sina.com.cn/',
    }

    # 行首市场前缀 → 标准市场后缀（新浪返回 var hq_str_bj920819="..."
    # 自带市场前缀，实测支持 bj；历史 bug 是 else 一律判 SZ）
    _MARKET_PREFIX_MAP = {
        'sh': MARKET_SH,
        'sz': MARKET_SZ,
        'bj': MARKET_BJ,
    }

    @staticmethod
    def _convert_code(code: str) -> str:
        """转换代码格式 000001.SZ -> sz000001（支持 BJ）"""
        converted = to_sina_code(code)
        return converted if converted else str(code)

    @staticmethod
    def _parse_quote(data: str) -> Optional[Dict]:
        """解析新浪行情数据"""
        try:
            match = re.search(r'var hq_str_(\w+)="(.+)"', data)
            if not match:
                return None
            code_raw = match.group(1)
            values = match.group(2).split(',')
            if len(values) < 32:
                return None
            market_prefix = code_raw[:2].lower()
            market = SinaSource._MARKET_PREFIX_MAP.get(market_prefix)
            if not market:
                logger.debug(f"Sina quote: unknown market prefix {code_raw!r}")
                return None
            symbol = code_raw[2:]
            code = f"{symbol}.{market}"
            return {
                'code': code,
                'name': values[0],
                'price': float(values[3]) if values[3] else 0,
                'change_pct': float(values[32]) if len(values) > 32 and values[32] else 0,
                'volume': float(values[8]) if values[8] else 0,
            }
        except Exception:
            return None

    @staticmethod
    def get_realtime_quote(codes: List[str]) -> pd.DataFrame:
        """获取指定股票实时行情"""
        try:
            sina_codes = [SinaSource._convert_code(c) for c in codes]
            url = SinaSource.BASE_URL + ','.join(sina_codes)
            response = requests.get(url, headers=SinaSource.HEADERS, timeout=15)
            # 新浪行情返回 GBK 编码，不显式设置会导致中文名称乱码
            response.encoding = 'gbk'
            if response.status_code == 200:
                content = response.text
                results = []
                for line in content.strip().split('\n'):
                    if line.strip():
                        quote = SinaSource._parse_quote(line)
                        if quote:
                            results.append(quote)
                if results:
                    return pd.DataFrame(results)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Sina get realtime quote error: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # 全市场列表 / 快照（第二线数据源，2026-10-20 按用户决策新增）
    # 东财出口 IP 经常被风控（ECS 实测 22 连败），降为备路；
    # 腾讯（第一）+ 新浪（第二）互为配合。
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_list_item(item: Dict) -> Optional[Dict]:
        """解析 Market_Center 列表项 → 标准行情记录

        字段（实测）：symbol=bj920000/sh600000/sz000001, code, name,
        trade(现价), changepercent, volume(股), amount(元),
        open/high/low/settlement(昨收), ticktime
        """
        try:
            symbol = str(item.get('symbol') or '')
            if not symbol:
                return None
            market = SinaSource._MARKET_PREFIX_MAP.get(symbol[:2].lower())
            if not market:
                return None
            code = f"{symbol[2:]}.{market}"
            if not normalize_code(code):
                return None
            return {
                'code': code,
                'name': str(item.get('name') or ''),
                'price': float(item.get('trade') or 0),
                'change_pct': float(item.get('changepercent') or 0),
                'volume': float(item.get('volume') or 0),
                'amount': float(item.get('amount') or 0),
            }
        except Exception:
            return None

    @staticmethod
    def get_all_stocks() -> pd.DataFrame:
        """获取全市场 A 股实时快照（分页拉取，node=hs_a 含北交所）

        既是股票列表源（stock_list 第一线），也是实时行情第二线
        （CompositeSource.get_all_realtime_quotes 降级路径）。
        """
        records = []
        try:
            for page in range(1, SinaSource.LIST_MAX_PAGES + 1):
                params = {
                    'page': page,
                    'num': SinaSource.LIST_PAGE_SIZE,
                    'sort': 'symbol',
                    'asc': 1,
                    'node': 'hs_a',
                }
                response = requests.get(
                    SinaSource.LIST_URL, params=params,
                    headers=SinaSource.HEADERS, timeout=15)
                if response.status_code != 200:
                    logger.warning(
                        f"Sina list page {page} HTTP {response.status_code}")
                    break
                # 接口返回 JSON（UTF-8，中文为 \u 转义，无需 GBK）
                items = json.loads(response.text)
                if not items:
                    break
                for item in items:
                    rec = SinaSource._parse_list_item(item)
                    if rec:
                        records.append(rec)
                if len(items) < SinaSource.LIST_PAGE_SIZE:
                    break
            if records:
                return pd.DataFrame(records)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Sina get all stocks error: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_stock_list() -> pd.DataFrame:
        """股票列表（code/name；新浪无行业字段，industry 留空）

        stock_list 第一线：修复此前仅有 akshare 一条路且未安装
        导致退化为 5 只兜底的问题（2026-10-20）。
        """
        df = SinaSource.get_all_stocks()
        if df.empty:
            return df
        out = df[['code', 'name']].copy()
        out['industry'] = ''
        return out

    @staticmethod
    def get_daily_data(code: str, days: int = 120) -> pd.DataFrame:
        """获取日线数据（getKLineData，scale=240 日线）

        修复日线链路断裂：原 _PRIORITY['daily'] 实际只有 akshare
        一条路（东财/腾讯彼时未实现该方法）且 akshare 未安装，
        导致 signals 全空（2026-10-20 实测根因）。
        返回列：date/open/high/low/close/volume（接口无成交额字段，
        下游指标与策略只用 close/high/low/volume，实测够用）。
        """
        try:
            symbol = SinaSource._convert_code(code)
            params = {
                'symbol': symbol,
                'scale': 240,
                'ma': 'no',
                'datalen': min(int(days), 1024),
            }
            response = requests.get(
                SinaSource.DAILY_URL, params=params,
                headers=SinaSource.HEADERS, timeout=15)
            if response.status_code != 200:
                logger.warning(f"Sina daily {code} HTTP {response.status_code}")
                return pd.DataFrame()
            items = json.loads(response.text)
            if not items:
                return pd.DataFrame()
            records = []
            for it in items:
                records.append({
                    'date': str(it.get('day') or ''),
                    'open': float(it.get('open') or 0),
                    'high': float(it.get('high') or 0),
                    'low': float(it.get('low') or 0),
                    'close': float(it.get('close') or 0),
                    'volume': float(it.get('volume') or 0),
                })
            df = pd.DataFrame(records)
            df = df[df['close'] > 0].sort_values('date').reset_index(drop=True)
            return df
        except Exception as e:
            logger.error(f"Sina get daily data error [{code}]: {e}")
            return pd.DataFrame()
