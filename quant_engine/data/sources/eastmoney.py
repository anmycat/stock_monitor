"""东方财富数据源"""
import re
import time
import requests
import pandas as pd
from typing import List, Dict, Optional
from loguru import logger

from ...utils.code_utils import to_secid, market_by_symbol, MARKET_BJ


class EastMoneySource:
    """东方财富数据源"""

    BASE_URL = "https://push2.eastmoney.com/api/qt/clist/get"

    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://quote.eastmoney.com/',
    }

    # 全市场列表 fs 过滤（深主板+创业板+沪主板+科创）
    # 注意：此 fs 不含 ETF/LOF 基金（m:1+t:3）与北交所（m:2+t:8）。
    # 覆盖率扩展需实测确认后启用；代码解析的正确性已由 _market_from_em 保证，
    # 北交所代码不会因本 fs 缺失而被错判（查不到即不返回，不会错标市场）。
    CLIST_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"

    @staticmethod
    def _market_from_em(f12: str, f13=None) -> Optional[str]:
        """由东财字段推断标准市场后缀

        优先使用 f13（东财市场字段：1=沪市, 0=深市场组）；但北交所在东财
        体系 f13 同样为 0（920819 实测），因此 f13=0 时须按代码前缀
        规则细分 BJ/SZ，不能一概判 SZ（历史 bug：920819 被错判）。
        f13 缺失时直接走前缀规则兜底（ETF 5 开头→SH、科创 688→SH）。
        """
        symbol = str(f12 or '').strip()
        if not re.match(r'^\d{6}$', symbol):
            return None
        by_prefix = market_by_symbol(symbol)
        if f13 is None:
            return by_prefix
        try:
            f13i = int(f13)
        except (TypeError, ValueError):
            return by_prefix
        if f13i == 1:
            return 'SH'
        if f13i == 0:
            # 北交所 4/8/92 开头在东财归 0 组，按前缀规则细分 SZ/BJ
            return by_prefix
        return by_prefix

    @staticmethod
    def _get_json(url: str, params: dict, tries: int = 3):
        """带重试的 GET+JSON 解析（代理下 push2 偶发 RemoteDisconnected，
        实测重试 1-2 次即恢复；M2' 备路依赖本方法稳定性）"""
        last_err = None
        for i in range(tries):
            try:
                response = requests.get(
                    url, params=params,
                    headers=EastMoneySource.HEADERS, timeout=15)
                if response.status_code == 200:
                    return response.json()
                last_err = f"HTTP {response.status_code}"
            except Exception as e:  # noqa: BLE001 - 重试循环内统一记录
                last_err = e
                time.sleep(0.5 * (i + 1))
        logger.warning(f"EM request failed after {tries} tries: {last_err}")
        return None

    @staticmethod
    def get_all_stocks() -> pd.DataFrame:
        """获取所有A股实时行情"""
        try:
            params = {
                "pn": "1",
                "pz": "5000",
                "po": "1",
                "np": "1",
                "fltt": "2",
                "invt": "2",
                "fid": "f3",
                "fs": EastMoneySource.CLIST_FS,
                "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18",
            }
            data = EastMoneySource._get_json(
                "https://push2.eastmoney.com/api/qt/clist/get", params)
            if data and 'data' in data and data['data'] is not None:
                items = data['data'].get('diff', [])
                records = []
                for item in items:
                    f12 = item.get('f12', '')
                    market = EastMoneySource._market_from_em(f12, item.get('f13'))
                    if not market:
                        logger.warning(f"EM clist: unrecognized code {f12!r}, skipped")
                        continue
                    records.append({
                        'code': f"{f12}.{market}",
                        'name': item.get('f14', ''),
                        'price': item.get('f2', 0),
                        'change_pct': item.get('f3', 0),
                        'volume': item.get('f5', 0),
                        'amount': item.get('f6', 0),
                    })
                return pd.DataFrame(records)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"EastMoney get all stocks error: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_realtime_quote(codes: List[str]) -> pd.DataFrame:
        """获取指定股票实时行情"""
        try:
            # 转换代码格式（统一走 code_utils，支持 SH/SZ/BJ）
            em_codes = []
            for code in codes:
                secid = to_secid(code)
                if not secid:
                    logger.warning(f"EM realtime: invalid code {code!r}, skipped")
                    continue
                em_codes.append(secid)

            if not em_codes:
                return pd.DataFrame()

            url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
            params = {
                "fltt": "2",
                "fields": "f2,f3,f4,f5,f6,f12,f13,f14",
                "secids": ",".join(em_codes),
            }
            response = requests.get(url, params=params, headers=EastMoneySource.HEADERS, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and data['data'] is not None:
                    items = data['data'].get('diff', [])
                    records = []
                    for item in items:
                        f12 = item.get('f12', '')
                        market = EastMoneySource._market_from_em(f12, item.get('f13'))
                        if not market:
                            logger.warning(f"EM ulist: unrecognized code {f12!r}, skipped")
                            continue
                        records.append({
                            'code': f"{f12}.{market}",
                            'name': item.get('f14', ''),
                            'price': item.get('f2', 0),
                            'change_pct': item.get('f3', 0),
                            'volume': item.get('f5', 0),
                        })
                    return pd.DataFrame(records)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"EastMoney get realtime quote error: {e}")
            return pd.DataFrame()
