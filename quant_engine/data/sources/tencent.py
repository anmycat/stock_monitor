"""腾讯财经数据源"""
import requests
import random
import pandas as pd
from typing import List, Dict, Optional
from loguru import logger


class TencentSource:
    """腾讯财经数据源 - qt.gtimg.cn"""

    BASE_URL = "https://qt.gtimg.cn/q="

    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
    ]

    @staticmethod
    def _get_headers() -> dict:
        return {
            'User-Agent': random.choice(TencentSource.USER_AGENTS),
            'Referer': 'https://stockapp.finance.qq.com',
        }

    @staticmethod
    def _convert_code(code: str) -> str:
        """转换代码格式 000001.SZ -> sz000001"""
        if '.' in code:
            parts = code.split('.')
            symbol = parts[0]
            market = parts[1]
            if market == 'SH':
                return f"sh{symbol}"
            elif market == 'SZ':
                return f"sz{symbol}"
        return code

    @staticmethod
    def _parse_quote(data: str) -> Optional[Dict]:
        """解析腾讯行情数据"""
        try:
            parts = data.split('~')
            if len(parts) < 45:
                return None
            code = parts[2]
            if code.startswith('6'):
                code = f"{code}.SH"
            else:
                code = f"{code}.SZ"
            return {
                'code': code,
                'name': parts[1],
                'price': float(parts[3]) if parts[3] else 0,
                'change_pct': float(parts[32]) if parts[32] else 0,
                'volume': float(parts[6]) if parts[6] else 0,
            }
        except Exception:
            return None

    @staticmethod
    def get_realtime_quote(codes: List[str]) -> pd.DataFrame:
        """获取指定股票实时行情"""
        try:
            qt_codes = [TencentSource._convert_code(c) for c in codes]
            url = TencentSource.BASE_URL + ','.join(qt_codes)
            response = requests.get(url, headers=TencentSource._get_headers(), timeout=15)
            if response.status_code == 200:
                content = response.text
                results = []
                for line in content.strip().split('\n'):
                    if '=' in line:
                        data = line.split('=')[1].strip(';').strip('"')
                        quote = TencentSource._parse_quote(data)
                        if quote:
                            results.append(quote)
                if results:
                    return pd.DataFrame(results)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Tencent get realtime quote error: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_all_stocks() -> pd.DataFrame:
        """获取所有股票行情（批量）"""
        try:
            # 获取主要股票代码列表
            codes = TencentSource._get_main_stock_codes()
            batch_size = 50
            all_quotes = []
            
            for i in range(0, min(len(codes), 500), batch_size):
                batch = codes[i:i + batch_size]
                qt_codes = [TencentSource._convert_code(c) for c in batch]
                url = TencentSource.BASE_URL + ','.join(qt_codes)
                try:
                    response = requests.get(url, headers=TencentSource._get_headers(), timeout=15)
                    if response.status_code == 200:
                        content = response.text
                        for line in content.strip().split('\n'):
                            if '=' in line:
                                data = line.split('=')[1].strip(';').strip('"')
                                quote = TencentSource._parse_quote(data)
                                if quote:
                                    all_quotes.append(quote)
                except Exception:
                    continue
            
            if all_quotes:
                df = pd.DataFrame(all_quotes)
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Tencent get all stocks error: {e}")
            return pd.DataFrame()

    @staticmethod
    def _get_main_stock_codes() -> List[str]:
        """获取主要股票代码"""
        codes = []
        # 上海主板
        for i in range(600000, 600200):
            codes.append(f"{i}.SH")
        for i in range(600500, 600700):
            codes.append(f"{i}.SH")
        for i in range(601000, 601200):
            codes.append(f"{i}.SH")
        for i in range(601300, 601500):
            codes.append(f"{i}.SH")
        # 深圳主板
        for i in range(0, 200):
            codes.append(f"{i:06d}.SZ")
        for i in range(1000, 1200):
            codes.append(f"{i:06d}.SZ")
        # 创业板
        for i in range(300000, 300200):
            codes.append(f"{i:06d}.SZ")
        return codes
