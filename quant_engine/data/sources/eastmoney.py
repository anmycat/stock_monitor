"""东方财富数据源"""
import requests
import pandas as pd
from typing import List, Dict, Optional
from loguru import logger


class EastMoneySource:
    """东方财富数据源"""

    BASE_URL = "https://push2.eastmoney.com/api/qt/clist/get"

    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://quote.eastmoney.com/',
    }

    @staticmethod
    def get_all_stocks() -> pd.DataFrame:
        """获取所有A股实时行情"""
        try:
            url = "https://push2.eastmoney.com/api/qt/clist/get"
            params = {
                "pn": "1",
                "pz": "5000",
                "po": "1",
                "np": "1",
                "fltt": "2",
                "invt": "2",
                "fid": "f3",
                "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
                "fields": "f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f14,f15,f16,f17,f18",
            }
            response = requests.get(url, params=params, headers=EastMoneySource.HEADERS, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and data['data'] is not None:
                    items = data['data'].get('diff', [])
                    records = []
                    for item in items:
                        records.append({
                            'code': item.get('f12', ''),
                            'name': item.get('f14', ''),
                            'price': item.get('f2', 0),
                            'change_pct': item.get('f3', 0),
                            'volume': item.get('f5', 0),
                            'amount': item.get('f6', 0),
                        })
                    df = pd.DataFrame(records)
                    if not df.empty:
                        df['code'] = df['code'].apply(lambda x: f"{x}.SZ" if x.startswith(('0', '3')) else f"{x}.SH")
                    return df
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"EastMoney get all stocks error: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_realtime_quote(codes: List[str]) -> pd.DataFrame:
        """获取指定股票实时行情"""
        try:
            # 转换代码格式
            em_codes = []
            for code in codes:
                symbol = code.split('.')[0]
                if code.endswith('.SH'):
                    em_codes.append(f"1.{symbol}")
                else:
                    em_codes.append(f"0.{symbol}")

            url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
            params = {
                "fltt": "2",
                "fields": "f2,f3,f4,f5,f6,f12,f14",
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
                        if f12.startswith(('0', '3')):
                            code = f"{f12}.SZ"
                        else:
                            code = f"{f12}.SH"
                        records.append({
                            'code': code,
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
