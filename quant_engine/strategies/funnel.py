"""三层漏斗选股引擎 - 从全市场筛选候选股票"""
from typing import Dict, List

import pandas as pd
from loguru import logger

from ..models.market import MarketState, MarketType
from ..utils.indicators import calculate_indicators
from .factor_scorer import FactorScorer
from .yaml_executor import YamlStrategyExecutor
from .yaml_loader import get_yaml_loader


class ThreeLayerFunnel:
    """三层漏斗选股引擎

    llm_ranker/context_enricher 为可选增强层（P4 参数注入，替代原函数内
    from ..services import 的违宪依赖，prd/15 §3.2 P4 清单 #3/#4）；
    None 时跳过对应增强（与原"构造/调用失败透传"输出一致）。
    """

    def __init__(self, datacenter, llm_ranker=None, context_enricher=None):
        self.datacenter = datacenter
        self.yaml_loader = get_yaml_loader()
        self.factor_scorer = FactorScorer()
        self.llm_ranker = llm_ranker
        self.context_enricher = context_enricher

    def screen(self, strategy_id: str, market: MarketState, max_output: int = 10) -> List[Dict]:
        """
        三层漏斗筛选
        
        Args:
            strategy_id: 策略ID
            market: 市场状态
            max_output: 最大输出数量
        
        Returns:
            候选股票列表
        """
        strategy = self.yaml_loader.get_strategy(strategy_id)
        if not strategy:
            logger.error(f"Strategy not found: {strategy_id}")
            return []

        logger.info(f"Starting screening with strategy: {strategy_id}")

        # L1: 硬筛（快速过滤）
        candidates = self._l1_hard_filter(strategy, market)
        logger.info(f"L1 hard filter: {len(candidates)} candidates")

        if not candidates:
            return []

        # L2: 评分排序（多因子）
        scored_candidates = self._l2_score_filter(candidates, strategy, market)
        logger.info(f"L2 score filter: {len(scored_candidates)} candidates")

        if not scored_candidates:
            return []

        # L2.5: LLM排序层（可选，注入缺失或调用失败时透传）
        if self.llm_ranker is not None:
            try:
                scored_candidates = self.llm_ranker.rank(scored_candidates, max_candidates=10)
                logger.info(f"L2.5 LLM ranking: {len(scored_candidates)} candidates")
            except Exception as e:  # noqa: BLE001 外部调用
                logger.warning(f"LLM ranking skipped: {e}")

        # L3: 深度分析（Top N）
        final_candidates = self._l3_deep_analysis(scored_candidates[:max_output], strategy, market)

        # 候选股上下文增强（新闻/舆情，可选）
        if self.context_enricher is not None:
            try:
                final_candidates = self.context_enricher.enrich_candidates(final_candidates, max_candidates=5)
            except Exception as e:  # noqa: BLE001 外部调用
                logger.warning(f"Candidate context enrichment failed: {e}")

        logger.info(f"L3 deep analysis: {len(final_candidates)} candidates")

        return final_candidates

    def _l1_hard_filter(self, strategy: Dict, market: MarketState) -> List[Dict]:
        """L1: 硬筛 - 快速过滤不符合基本条件的股票"""
        stocks = self.datacenter.get_stock_list()
        candidates = []

        for _, stock in stocks.iterrows():
            code = stock['code']
            try:
                df = self.datacenter.get_daily_data(code, 60)

                if df is None or df.empty:
                    continue

                # 快速检查基本条件
                if self._quick_check(df, strategy, market):
                    candidates.append({
                        'code': code,
                        'name': stock.get('name', code),
                        'df': df,
                    })
            except Exception as e:  # noqa: BLE001 单项跳过
                logger.debug(f"L1 filter error for {code}: {e}")

        return candidates

    def _quick_check(self, df: pd.DataFrame, strategy: Dict, market: MarketState) -> bool:
        """快速检查基本条件"""
        # 数据长度检查
        if len(df) < 20:
            return False

        # 价格检查
        if df['close'].iloc[-1] <= 0:
            return False

        # 成交量检查
        if df['volume'].iloc[-1] <= 0:
            return False

        # 熊市检查
        if market.type == MarketType.BEAR:
            return False

        return True

    def _l2_score_filter(self, candidates: List[Dict], strategy: Dict, market: MarketState) -> List[Dict]:
        """L2: 评分排序 - 多因子评分"""
        scored = []

        for candidate in candidates:
            try:
                df = candidate['df']
                indicators = calculate_indicators(df)

                # 计算多因子分数
                factors = self.factor_scorer.calculate_factors(df, indicators)

                # 执行YAML策略
                executor = YamlStrategyExecutor(strategy)
                passed, score, reasons = executor.execute(df, indicators)

                if passed:
                    candidate['factors'] = factors
                    candidate['score'] = score
                    candidate['reasons'] = reasons
                    candidate['indicators'] = indicators
                    scored.append(candidate)
            except Exception as e:  # noqa: BLE001 单项跳过
                logger.debug(f"L2 score error for {candidate['code']}: {e}")

        # 按分数排序
        scored.sort(key=lambda x: x['score'], reverse=True)
        return scored

    def _l3_deep_analysis(self, candidates: List[Dict], strategy: Dict, market: MarketState) -> List[Dict]:
        """L3: 深度分析 - 对Top候选做详细分析"""
        results = []

        for candidate in candidates:
            try:
                df = candidate['df']
                factors = candidate.get('factors', {})
                score = candidate.get('score', 0)

                # 确定信号级别
                levels = strategy.get('levels', {})
                if score >= levels.get('S', 85):
                    level = 'S'
                elif score >= levels.get('A', 75):
                    level = 'A'
                elif score >= levels.get('B', 65):
                    level = 'B'
                else:
                    level = None

                # 确定操作
                min_score = strategy.get('scoring', {}).get('min_score', 60)
                if score >= min_score:
                    action = 'BUY'
                elif score >= min_score * 0.8:
                    action = 'WATCH'
                else:
                    action = 'SKIP'

                # 计算仓位
                position = self._calculate_position(score, market, strategy)

                # 获取出场条件
                exit_config = strategy.get('exit', {})
                stop_loss = exit_config.get('stop_loss', 0.05)
                take_profit = exit_config.get('take_profit', 0.15)

                # 构建结果
                analysis = {
                    'code': candidate['code'],
                    'name': candidate['name'],
                    'score': round(score, 1),
                    'level': level,
                    'action': action,
                    'position': position,
                    'factors': factors,
                    'reasons': candidate.get('reasons', []),
                    'strategy': strategy.get('meta', {}).get('name', ''),
                    'price': df['close'].iloc[-1],
                    'target_price': df['close'].iloc[-1] * (1 + take_profit),
                    'stop_loss_price': df['close'].iloc[-1] * (1 - stop_loss),
                }
                results.append(analysis)
            except Exception as e:  # noqa: BLE001 单项跳过
                logger.error(f"L3 analysis error for {candidate['code']}: {e}")

        return results

    def _calculate_position(self, score: float, market: MarketState, strategy: Dict) -> float:
        """计算仓位"""
        # 基础仓位
        base = 0.3

        # 市场因子
        market_factor = market.factor

        # 分数因子
        score_factor = score / 100

        # 计算仓位
        position = base * market_factor * score_factor

        # 限制仓位范围
        return max(0.1, min(0.8, position))

    def list_strategies(self) -> List[Dict]:
        """列出所有可用策略"""
        return self.yaml_loader.list_strategies()
