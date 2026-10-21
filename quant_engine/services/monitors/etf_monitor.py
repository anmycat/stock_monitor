"""ETF监控模块 - 腾讯第一的实时行情路（2026-10-20 起）"""
import time
from typing import List, Dict
from loguru import logger
from ...data.repositories import MarketDataRepository


# 热门ETF监控清单（行情监控与持仓监控共用，新增/删除需同步两侧文档）
HOT_ETFS = [
    {"code": "510880.SH", "name": "红利ETF", "category": "价值"},
    {"code": "510050.SH", "name": "上证50ETF", "category": "大盘"},
    {"code": "510300.SH", "name": "沪深300ETF", "category": "大盘"},
    {"code": "510500.SH", "name": "中证500ETF", "category": "中盘"},
    {"code": "512100.SH", "name": "中证1000ETF", "category": "小盘"},
    {"code": "159915.SZ", "name": "创业板ETF", "category": "成长"},
    {"code": "588000.SH", "name": "科创50ETF", "category": "科创"},
    {"code": "512010.SH", "name": "医药ETF", "category": "行业"},
    {"code": "512660.SH", "name": "军工ETF", "category": "行业"},
    {"code": "512880.SH", "name": "证券ETF", "category": "行业"},
    {"code": "515030.SH", "name": "新能源ETF", "category": "行业"},
    {"code": "512690.SH", "name": "酒ETF", "category": "消费"},
    {"code": "159995.SZ", "name": "芯片ETF", "category": "科技"},
    {"code": "516510.SH", "name": "云计算ETF", "category": "科技"},
]


class ETFMonitor:
    def __init__(self, datacenter: MarketDataRepository):
        self.datacenter = datacenter
        self.target_etfs = HOT_ETFS
        self._cache = {}
        self._cache_time = 0

    def get_etf_status(self) -> List[Dict]:
        """获取ETF状态 - 腾讯第一的实时行情路（2026-10-20 改造）

        原实现直调 AKShareSource().get_etf_quotes()（akshare 未安装，
        ETF 监控全量"数据获取中"）。现改为按 target_codes 直查
        CompositeSource.get_realtime_quote（腾讯批量第一、新浪第二、
        东财备路），顺带修复"拉全市场再过滤"的冗余（见 PRD 02 §3.8）。
        """
        current_time = time.time()

        # 缓存5分钟
        if self._cache and current_time - self._cache_time < 300:
            return self._cache.get('data', [])

        results = []
        target_codes = {etf['code'] for etf in self.target_etfs}
        etf_dict = {etf['code']: etf for etf in self.target_etfs}

        # 按目标代码直查（腾讯源原生支持 ETF 5 开头代码）
        try:
            df = self.datacenter.get_realtime_quote(
                [etf['code'] for etf in self.target_etfs])
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    code = row.get('code', '')
                    if code in target_codes:
                        etf_info = etf_dict.get(code, {})
                        price = float(row.get('price', 0) or 0)
                        change_pct = float(row.get('change_pct', 0) or 0)
                        volume = float(row.get('volume', 0) or 0)
                        results.append({
                            'code': code,
                            'name': etf_info.get('name', row.get('name', '')),
                            'category': etf_info.get('category', ''),
                            'price': round(price, 3),
                            'change_pct': round(change_pct, 2),
                            'volume': volume,
                            'trend': self._analyze_trend(change_pct),
                            'volume_desc': self._format_volume(volume),
                        })
        except Exception as e:
            logger.error(f"Get ETF error: {e}")
        
        # 对于未获取到数据的ETF，用"--"标识
        existing_codes = {r['code'] for r in results}
        for etf in self.target_etfs:
            if etf['code'] not in existing_codes:
                results.append({
                    'code': etf['code'],
                    'name': etf['name'],
                    'category': etf['category'],
                    'price': 0,
                    'change_pct': 0,
                    'volume': 0,
                    'trend': '数据获取中',
                    'volume_desc': '--',
                    'no_data': True,
                })
        
        if results:
            self._cache = {'data': results}
            self._cache_time = current_time
        
        # 按照红利ETF优先，然后按涨跌幅排序
        def sort_key(x):
            if x['code'] == '510880.SH':  # 红利ETF
                return (0, 0)
            return (1, abs(x.get('change_pct', 0)))
        
        results.sort(key=sort_key)
        return results

    def _analyze_trend(self, change_pct: float) -> str:
        if change_pct > 2:
            return "强势上涨"
        elif change_pct > 1:
            return "明显上涨"
        elif change_pct > 0.5:
            return "温和上涨"
        elif change_pct > -0.5:
            return "横盘整理"
        elif change_pct > -1:
            return "温和下跌"
        elif change_pct > -2:
            return "明显下跌"
        else:
            return "大幅下跌"

    def _format_volume(self, volume: float) -> str:
        if volume >= 1e8:
            return f"{volume/1e8:.2f}亿"
        elif volume >= 1e4:
            return f"{volume/1e4:.0f}万"
        else:
            return f"{volume:.0f}"

    def get_market_sentiment(self) -> Dict:
        etf_status = self.get_etf_status()
        valid_etfs = [e for e in etf_status if not e.get('no_data')]
        if not valid_etfs:
            return {"sentiment": "neutral", "score": 50, "details": []}
        up_count = sum(1 for e in valid_etfs if e.get('change_pct', 0) > 0)
        down_count = sum(1 for e in valid_etfs if e.get('change_pct', 0) < 0)
        avg_change = sum(e.get('change_pct', 0) for e in valid_etfs) / len(valid_etfs)
        if avg_change > 1:
            sentiment = "bullish"
            score = 70
        elif avg_change > 0:
            sentiment = "slightly_bullish"
            score = 60
        elif avg_change > -1:
            sentiment = "neutral"
            score = 50
        elif avg_change > -2:
            sentiment = "slightly_bearish"
            score = 40
        else:
            sentiment = "bearish"
            score = 30
        return {
            "sentiment": sentiment,
            "score": score,
            "avg_change": round(avg_change, 2),
            "up_count": up_count,
            "down_count": down_count,
            "details": etf_status,
        }
