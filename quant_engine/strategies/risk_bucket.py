"""组合风险桶 - 对标幻方CVaR风控：行业集中度+最大回撤+连续亏损熔断"""
from typing import List, Dict
from loguru import logger


class RiskBucket:
    def __init__(self):
        self.max_same_industry = 3
        self.max_same_concept = 2
        self.industry_penalty = 0.1
        self.concept_penalty = 0.05
        # CVaR风格 — 单策略最大回撤2%
        self.max_drawdown_pct = 0.02
        self.consecutive_loss_limit = 3
        self._drawdowns = {}
        self._consecutive_losses = {}
        self._frozen = set()  # 熔断冻结池

    def apply_risk_bucket(self, candidates: List[Dict]) -> List[Dict]:
        """
        应用风险桶扣分
        
        Args:
            candidates: 候选股票列表
        
        Returns:
            调整后的候选列表
        """
        if not candidates:
            return candidates
        
        # 统计行业和概念
        industry_count = {}
        concept_count = {}
        
        for candidate in candidates:
            industry = candidate.get('industry', '未知')
            concepts = candidate.get('concepts', [])
            
            industry_count[industry] = industry_count.get(industry, 0) + 1
            for concept in concepts:
                concept_count[concept] = concept_count.get(concept, 0) + 1
        
        # 应用扣分
        for candidate in candidates:
            industry = candidate.get('industry', '未知')
            concepts = candidate.get('concepts', [])
            
            # 行业扣分
            if industry_count.get(industry, 0) > self.max_same_industry:
                penalty = (industry_count[industry] - self.max_same_industry) * self.industry_penalty
                candidate['score'] = candidate.get('score', 0) * (1 - penalty)
                candidate.setdefault('risk_notes', []).append(f"行业集中: {industry}")
            
            # 概念扣分
            for concept in concepts:
                if concept_count.get(concept, 0) > self.max_same_concept:
                    penalty = (concept_count[concept] - self.max_same_concept) * self.concept_penalty
                    candidate['score'] = candidate.get('score', 0) * (1 - penalty)
                    candidate.setdefault('risk_notes', []).append(f"概念集中: {concept}")
        
        # 重新排序
        candidates.sort(key=lambda x: x.get('score', 0), reverse=True)
        
        return candidates

    def get_industry_distribution(self, candidates: List[Dict]) -> Dict:
        """获取行业分布"""
        distribution = {}
        for candidate in candidates:
            industry = candidate.get('industry', '未知')
            if industry not in distribution:
                distribution[industry] = []
            distribution[industry].append(candidate.get('code', ''))
        return distribution

    def get_concept_distribution(self, candidates: List[Dict]) -> Dict:
        """获取概念分布"""
        distribution = {}
        for candidate in candidates:
            concepts = candidate.get('concepts', [])
            for concept in concepts:
                if concept not in distribution:
                    distribution[concept] = []
                distribution[concept].append(candidate.get('code', ''))
        return distribution

    def check_concentration_risk(self, candidates: List[Dict]) -> Dict:
        """检查集中度风险"""
        industry_dist = self.get_industry_distribution(candidates)
        concept_dist = self.get_concept_distribution(candidates)
        
        max_industry_count = max(len(v) for v in industry_dist.values()) if industry_dist else 0
        max_industry = max(industry_dist.items(), key=lambda x: len(x[1])) if industry_dist else None
        
        max_concept_count = max(len(v) for v in concept_dist.values()) if concept_dist else 0
        max_concept = max(concept_dist.items(), key=lambda x: len(x[1])) if concept_dist else None
        
        return {
            'total_candidates': len(candidates),
            'industry_count': len(industry_dist),
            'concept_count': len(concept_dist),
            'max_industry': max_industry[0] if max_industry else None,
            'max_industry_count': max_industry_count,
            'max_concept': max_concept[0] if max_concept else None,
            'max_concept_count': max_concept_count,
            'is_concentrated': max_industry_count > self.max_same_industry or max_concept_count > self.max_same_concept,
        }

    def check_drawdown_breach(self, strategy_id: str, pnl: float) -> Dict:
        """CVaR风格回撤检测 — 对标幻方"单策略最大回撤2%"
        
        Returns:
            {'breached': bool, 'frozen': bool, 'reason': str}
        """
        if pnl >= 0:
            self._consecutive_losses[strategy_id] = 0
            return {'breached': False, 'frozen': False, 'reason': '盈利'}

        # 更新连续亏损
        self._consecutive_losses[strategy_id] = self._consecutive_losses.get(strategy_id, 0) + 1

        # 连续亏损熔断
        if self._consecutive_losses[strategy_id] >= self.consecutive_loss_limit:
            self._frozen.add(strategy_id)
            logger.warning(f"Strategy {strategy_id} frozen: {self.consecutive_loss_limit} consecutive losses")
            return {'breached': True, 'frozen': True,
                    'reason': f'连续{self.consecutive_loss_limit}次亏损，熔断'}

        return {'breached': False, 'frozen': False, 'reason': '单次亏损'}

    def is_frozen(self, strategy_id: str) -> bool:
        """检查策略是否被熔断"""
        return strategy_id in self._frozen

    def unfreeze(self, strategy_id: str):
        """手动解除熔断"""
        self._frozen.discard(strategy_id)
        self._consecutive_losses[strategy_id] = 0
        logger.info(f"Strategy {strategy_id} unfrozen")