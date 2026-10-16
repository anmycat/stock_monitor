"""新浪财经数据源"""
import requests
import re
import pandas as pd
from typing import List, Dict, Optional
from loguru import logger


class SinaSource:
    """新浪财经数据源"""

    BASE_URL = "https://hq.sinajs.cn/list="

    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://finance.sina.com.cn/',
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
        """解析新浪行情数据"""
        try:
            match = re.search(r'var hq_str_(\w+)="(.+)"', data)
            if not match:
                return None
            code_raw = match.group(1)
            values = match.group(2).split(',')
            if len(values) < 32:
                return None
            symbol = code_raw[2:]
            if code_raw.startswith('sh'):
                code = f"{symbol}.SH"
            else:
                code = f"{symbol}.SZ"
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
