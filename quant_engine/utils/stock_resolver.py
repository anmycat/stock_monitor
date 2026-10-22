"""股票代码解析器 - 支持代码/名称/拼音搜索"""
from typing import Dict, List, Optional

from loguru import logger

try:
    from pypinyin import lazy_pinyin
    HAS_PINYIN = True
except ImportError:
    HAS_PINYIN = False
    logger.warning("pypinyin not installed, pinyin search disabled")


class StockResolver:
    """股票代码解析器"""

    def __init__(self):
        self._stock_cache = []
        self._pinyin_cache = {}

    def update_cache(self, stocks: List[Dict]):
        """更新股票缓存"""
        self._stock_cache = stocks
        self._pinyin_cache = {}
        if HAS_PINYIN:
            for stock in stocks:
                name = stock.get('name', '')
                pinyin = ''.join(lazy_pinyin(name))
                self._pinyin_cache[pinyin] = stock

    def resolve(self, query: str, limit: int = 10) -> List[Dict]:
        """
        解析股票查询

        Args:
            query: 查询字符串（代码/名称/拼音）
            limit: 返回结果数量

        Returns:
            匹配的股票列表
        """
        if not query:
            return []

        query = query.strip().upper()
        results = []

        # 1. 精确匹配代码
        exact_match = self._match_code(query)
        if exact_match:
            results.append(exact_match)

        # 2. 前缀匹配代码
        code_matches = self._match_code_prefix(query)
        results.extend(code_matches)

        # 3. 名称匹配
        name_matches = self._match_name(query)
        results.extend(name_matches)

        # 4. 拼音匹配
        if HAS_PINYIN:
            pinyin_matches = self._match_pinyin(query.lower())
            results.extend(pinyin_matches)

        # 去重
        seen = set()
        unique_results = []
        for r in results:
            code = r.get('code', '')
            if code not in seen:
                seen.add(code)
                unique_results.append(r)

        return unique_results[:limit]

    def _match_code(self, code: str) -> Optional[Dict]:
        """精确匹配股票代码"""
        # 处理带后缀的代码
        if '.' in code:
            for stock in self._stock_cache:
                if stock.get('code', '').upper() == code:
                    return stock
        else:
            # 尝试补全后缀（含北交所）
            for suffix in ['.SH', '.SZ', '.BJ']:
                full_code = code + suffix
                for stock in self._stock_cache:
                    if stock.get('code', '') == full_code:
                        return stock
        return None

    def _match_code_prefix(self, prefix: str) -> List[Dict]:
        """前缀匹配股票代码"""
        results = []
        for stock in self._stock_cache:
            code = stock.get('code', '')
            # 去掉后缀比较
            symbol = code.split('.')[0] if '.' in code else code
            if symbol.startswith(prefix):
                results.append(stock)
        return results

    def _match_name(self, name: str) -> List[Dict]:
        """名称模糊匹配"""
        results = []
        for stock in self._stock_cache:
            stock_name = stock.get('name', '')
            if name in stock_name or stock_name in name:
                results.append(stock)
        return results

    def _match_pinyin(self, pinyin: str) -> List[Dict]:
        """拼音匹配"""
        results = []
        for py, stock in self._pinyin_cache.items():
            if py.startswith(pinyin) or pinyin in py:
                results.append(stock)
        return results

    def get_stock_info(self, code: str) -> Optional[Dict]:
        """获取股票信息"""
        for stock in self._stock_cache:
            if stock.get('code', '') == code:
                return stock
        return None


# 全局实例
resolver = StockResolver()