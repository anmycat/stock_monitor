"""新闻舆情分析服务"""
import httpx
from typing import List, Dict
from loguru import logger
from ...config import get_settings


class NewsItem:
    def __init__(self, title: str, url: str, source: str, time: str = ""):
        self.title = title
        self.url = url
        self.source = source
        self.time = time


class NewsService:
    """新闻搜索服务 - 支持多个新闻源"""

    def __init__(self):
        settings = get_settings()
        self.serpapi_key = settings.SERPAPI_API_KEY
        self.tavily_key = settings.TAVILY_API_KEY
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

    def get_stock_news(self, code: str, name: str, limit: int = 5) -> List[Dict]:
        """获取股票相关新闻"""
        news = []

        # 尝试SerpAPI
        if self.serpapi_key:
            try:
                serp_news = self._search_serpapi(name)
                news.extend(serp_news)
            except Exception as e:
                logger.error(f"SerpAPI search error: {e}")

        # 尝试Tavily
        if self.tavily_key and len(news) < limit:
            try:
                tavily_news = self._search_tavily(name)
                news.extend(tavily_news)
            except Exception as e:
                logger.error(f"Tavily search error: {e}")

        # 如果没有配置搜索API，使用东方财富新闻
        if not news:
            news = self._search_eastmoney(code)

        return news[:limit]

    def get_market_news(self, limit: int = 8) -> List[Dict]:
        """获取市场热点新闻（A股/宏观经济）"""
        news = []
        keywords = ["A股", "央行", "经济数据"]
        for kw in keywords:
            try:
                items = self._search_eastmoney_keyword(kw, count=3)
                news.extend(items)
            except Exception as e:
                logger.error(f"Market news '{kw}' error: {e}")
            if len(news) >= limit:
                break
        # 去重（按标题）
        seen = set()
        unique = []
        for n in news:
            t = n.get('title', '')
            if t and t not in seen:
                seen.add(t)
                unique.append(n)
        return unique[:limit]

    def _search_serpapi(self, query: str) -> List[Dict]:
        """使用SerpAPI搜索新闻"""
        url = "https://serpapi.com/search"
        params = {
            "q": f"{query} 股票 新闻",
            "tbm": "nws",
            "api_key": self.serpapi_key,
            "num": 5,
            "hl": "zh-CN",
            "gl": "cn",
        }
        try:
            with httpx.Client(timeout=10) as client:
                response = client.get(url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    news = []
                    for item in data.get('news_results', []):
                        news.append({
                            'title': item.get('title', ''),
                            'url': item.get('link', ''),
                            'source': item.get('source', 'SerpAPI'),
                            'time': item.get('date', ''),
                            'snippet': item.get('snippet', ''),
                        })
                    return news
        except Exception as e:
            logger.error(f"SerpAPI error: {e}")
        return []

    def _search_tavily(self, query: str) -> List[Dict]:
        """使用Tavily搜索新闻"""
        url = "https://api.tavily.com/search"
        payload = {
            "api_key": self.tavily_key,
            "query": f"{query} 股票 最新消息",
            "search_depth": "basic",
            "max_results": 5,
            "include_answer": False,
        }
        try:
            with httpx.Client(timeout=10) as client:
                response = client.post(url, json=payload)
                if response.status_code == 200:
                    data = response.json()
                    news = []
                    for item in data.get('results', []):
                        news.append({
                            'title': item.get('title', ''),
                            'url': item.get('url', ''),
                            'source': 'Tavily',
                            'time': '',
                            'snippet': item.get('content', '')[:200],
                        })
                    return news
        except Exception as e:
            logger.error(f"Tavily error: {e}")
        return []

    def _search_eastmoney(self, code: str) -> List[Dict]:
        """使用东方财富搜索新闻（个股）"""
        symbol = code.split('.')[0]
        return self._search_eastmoney_keyword(symbol)

    def _search_eastmoney_keyword(self, keyword: str, count: int = 5) -> List[Dict]:
        """使用东方财富搜索新闻（按关键词）"""
        url = "https://search-api-web.eastmoney.com/search/jsonp"
        params = {
            "cb": "jQuery",
            "param": f'{{"uid":"","keyword":"{keyword}","type":["cmsArticleWebOld"],"client":"web","clientType":"web","clientVersion":"curr","param":{{"cmsArticleWebOld":{{"searchScope":"default","sort":"default","pageIndex":1,"pageSize":{count},"preTag":"<em>","postTag":"</em>"}}}}}}',
        }
        try:
            with httpx.Client(timeout=10) as client:
                response = client.get(url, params=params, headers=self.headers)
                if response.status_code == 200:
                    text = response.text
                    # 解析JSONP响应
                    json_str = text[text.index('(') + 1:text.rindex(')')]
                    import json
                    data = json.loads(json_str)
                    news = []
                    for item in data.get('result', {}).get('cmsArticleWebOld', []):
                        news.append({
                            'title': item.get('title', '').replace('<em>', '').replace('</em>', ''),
                            'url': item.get('url', ''),
                            'source': '东方财富',
                            'time': item.get('showTime', ''),
                            'snippet': item.get('content', '')[:200] if item.get('content') else '',
                        })
                    return news
        except Exception as e:
            logger.error(f"Eastmoney news error: {e}")
        return []

    def analyze_sentiment(self, news: List[Dict]) -> Dict:
        """分析新闻舆情情绪"""
        if not news:
            return {
                'sentiment': 'neutral',
                'score': 50,
                'positive_count': 0,
                'negative_count': 0,
                'summary': '暂无新闻数据',
            }

        # 简单的关键词分析
        positive_keywords = ['利好', '上涨', '增长', '突破', '新高', '加仓', '买入', '看多']
        negative_keywords = ['利空', '下跌', '亏损', '破位', '新低', '减仓', '卖出', '看空']

        positive_count = 0
        negative_count = 0

        for item in news:
            title = item.get('title', '')
            snippet = item.get('snippet', '')
            text = title + snippet

            for kw in positive_keywords:
                if kw in text:
                    positive_count += 1
                    break

            for kw in negative_keywords:
                if kw in text:
                    negative_count += 1
                    break

        total = positive_count + negative_count
        if total == 0:
            sentiment = 'neutral'
            score = 50
        elif positive_count > negative_count:
            sentiment = 'positive'
            score = 50 + (positive_count / total) * 50
        elif negative_count > positive_count:
            sentiment = 'negative'
            score = 50 - (negative_count / total) * 50
        else:
            sentiment = 'neutral'
            score = 50

        return {
            'sentiment': sentiment,
            'score': round(score),
            'positive_count': positive_count,
            'negative_count': negative_count,
            'news_count': len(news),
            'summary': self._generate_sentiment_summary(sentiment, positive_count, negative_count),
        }

    def _generate_sentiment_summary(self, sentiment: str, positive: int, negative: int) -> str:
        if sentiment == 'positive':
            return f"舆情偏积极，利好消息{positive}条，利空消息{negative}条"
        elif sentiment == 'negative':
            return f"舆情偏消极，利空消息{negative}条，利好消息{positive}条"
        else:
            return f"舆情中性，利好消息{positive}条，利空消息{negative}条"