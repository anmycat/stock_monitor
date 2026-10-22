"""候选级上下文 - 为Top候选抓取新闻/公告/资金流"""

from loguru import logger

from ..monitors.news_service import NewsService


class CandidateContext:
    """候选级上下文"""

    def __init__(self):
        self.news_service = NewsService()

    def enrich_candidates(self, candidates: list[dict], max_candidates: int = 5) -> list[dict]:
        """
        为候选股票添加上下文信息
        
        Args:
            candidates: 候选股票列表
            max_candidates: 最多处理的候选数量
        
        Returns:
            添加上下文后的候选列表
        """
        enriched = []
        
        for i, candidate in enumerate(candidates):
            if i >= max_candidates:
                enriched.append(candidate)
                continue
            
            code = candidate.get('code', '')
            name = candidate.get('name', '')
            
            try:
                # 获取新闻
                news = self.news_service.get_stock_news(code, name, limit=3)
                
                # 分析舆情
                sentiment = self.news_service.analyze_sentiment(news)
                
                # 添加上下文
                candidate['context'] = {
                    'news': news,
                    'sentiment': sentiment,
                    'news_count': len(news),
                }
                
                # 根据舆情调整分数
                sentiment_score = sentiment.get('score', 50)
                if sentiment_score > 60:
                    candidate['score'] = candidate.get('score', 0) * 1.05
                elif sentiment_score < 40:
                    candidate['score'] = candidate.get('score', 0) * 0.95
                
                enriched.append(candidate)
                
            except Exception as e:  # noqa: BLE001 外部调用
                logger.error(f"Enrich candidate {code} error: {e}")
                enriched.append(candidate)
        
        # 重新排序
        enriched.sort(key=lambda x: x.get('score', 0), reverse=True)
        
        return enriched

    def get_candidate_summary(self, candidate: dict) -> str:
        """获取候选摘要"""
        context = candidate.get('context', {})
        news = context.get('news', [])
        sentiment = context.get('sentiment', {})
        
        summary_parts = []
        
        # 舆情摘要
        sentiment_text = sentiment.get('summary', '')
        if sentiment_text:
            summary_parts.append(f"舆情: {sentiment_text}")
        
        # 新闻摘要
        if news:
            news_titles = [n.get('title', '') for n in news[:3]]
            summary_parts.append(f"最新动态: {'; '.join(news_titles)}")
        
        return ' | '.join(summary_parts) if summary_parts else "暂无上下文信息"

    def format_context_for_llm(self, candidates: list[dict]) -> str:
        """格式化上下文供LLM使用"""
        lines = []
        
        for candidate in candidates[:5]:
            code = candidate.get('code', '')
            name = candidate.get('name', '')
            context = candidate.get('context', {})
            news = context.get('news', [])
            sentiment = context.get('sentiment', {})
            
            lines.append(f"## {name} ({code})")
            lines.append(f"- 舆情: {sentiment.get('summary', '无')}")
            
            if news:
                lines.append("- 最新动态:")
                for n in news[:3]:
                    lines.append(f"  - {n.get('title', '')}")
            
            lines.append("")
        
        return "\n".join(lines)
