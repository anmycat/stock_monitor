import pandas as pd
from typing import Optional, List, Dict
from loguru import logger
from .engine import StrategyEngine
from .trend import TrendStrategy
from .dip import DipStrategy
from .defense import DefenseStrategy
from .yaml_loader import get_yaml_loader
from .yaml_executor import YamlStrategyExecutor
from .factor_scorer import FactorScorer
from ..utils.indicators import calculate_indicators
from ..models.market import MarketState, MarketType
from ..models.signal import Signal, SignalAction, SignalLevel


class StrategyDispatcher:
    def __init__(self, datacenter=None):
        self.engine = StrategyEngine(datacenter)
        self.trend_strategy = TrendStrategy()
        self.dip_strategy = DipStrategy()
        self.defense_strategy = DefenseStrategy()
        self.yaml_loader = get_yaml_loader()
        self.factor_scorer = FactorScorer()

    def dispatch(self, df: pd.DataFrame, market: MarketState, trades: List[Dict] = None, equity_curve: List[float] = None, strategy_id: str = None) -> Optional[Signal]:
        """
        策略调度
        
        Args:
            df: 股票数据
            market: 市场状态
            trades: 交易记录
            equity_curve: 资金曲线
            strategy_id: YAML策略ID（可选）
        """
        if trades is None:
            trades = []
        if equity_curve is None:
            equity_curve = []

        # 防守检查
        defense_check = self.defense_strategy.check_market(market)
        if defense_check['action'] == 'STOP':
            logger.info(f"Defense STOP: {defense_check['reason']}")
            return None

        # 获取股票代码
        code = df['code'].iloc[0] if 'code' in df.columns else None

        # 如果指定了YAML策略，使用YAML执行器
        if strategy_id:
            return self._dispatch_yaml(df, market, strategy_id, trades, equity_curve)

        # 否则使用原有策略引擎
        engine_result = self.engine.process(df, market, code)
        if not engine_result['passed']:
            logger.debug(f"Engine filter: {engine_result['reasons']}")
            return None

        indicators = engine_result['indicators']

        # 根据市场状态选择策略
        signal = self._run_builtin_strategies(df, indicators, market)

        if signal is None:
            return None

        # FactorScorer多因子融合: 对内置策略信号做二次校验
        signal = self._apply_factor_scoring(signal, df, indicators)
        if signal is None:
            return None

        # 防守检查
        loss_check = self.defense_strategy.check_consecutive_losses(trades)
        if loss_check['action'] == 'STOP':
            logger.info(f"Defense STOP: {loss_check['reason']}")
            return None

        dd_check = self.defense_strategy.check_drawdown(equity_curve)
        if dd_check['action'] == 'STOP':
            logger.info(f"Defense STOP: {dd_check['reason']}")
            return None

        # 调整仓位
        position_factor = self.defense_strategy.get_position_factor(market, trades, equity_curve)
        signal.position *= position_factor

        return signal

    def _dispatch_yaml(self, df: pd.DataFrame, market: MarketState, strategy_id: str, trades: List[Dict], equity_curve: List[float]) -> Optional[Signal]:
        """使用YAML策略调度"""
        strategy = self.yaml_loader.get_strategy(strategy_id)
        if not strategy:
            logger.error(f"YAML strategy not found: {strategy_id}")
            return None

        # 计算指标
        indicators = calculate_indicators(df)

        # 执行YAML策略
        executor = YamlStrategyExecutor(strategy)
        passed, score, reasons = executor.execute(df, indicators)

        if not passed:
            logger.debug(f"YAML strategy filter: {reasons}")
            return None

        # 计算多因子分数
        factors = self.factor_scorer.calculate_factors(df, indicators)

        # 确定信号级别
        levels = strategy.get('levels', {})
        if score >= levels.get('S', 85):
            level = SignalLevel.S
        elif score >= levels.get('A', 75):
            level = SignalLevel.A
        elif score >= levels.get('B', 65):
            level = SignalLevel.B
        else:
            level = None

        # 确定操作
        min_score = strategy.get('scoring', {}).get('min_score', 60)
        if score >= min_score:
            action = SignalAction.BUY
        elif score >= min_score * 0.8:
            action = SignalAction.WATCH
        else:
            action = SignalAction.SKIP

        # 计算仓位
        exit_config = strategy.get('exit', {})
        position = self._calculate_position(score, market, strategy)

        # 防守检查
        loss_check = self.defense_strategy.check_consecutive_losses(trades)
        if loss_check['action'] == 'STOP':
            logger.info(f"Defense STOP: {loss_check['reason']}")
            return None

        dd_check = self.defense_strategy.check_drawdown(equity_curve)
        if dd_check['action'] == 'STOP':
            logger.info(f"Defense STOP: {dd_check['reason']}")
            return None

        # 调整仓位
        position_factor = self.defense_strategy.get_position_factor(market, trades, equity_curve)
        position *= position_factor

        # 构建信号
        code = df['code'].iloc[0] if 'code' in df.columns else 'unknown'
        name = df['name'].iloc[0] if 'name' in df.columns else 'unknown'

        signal = Signal(
            code=code,
            name=name,
            action=action,
            level=level,
            score=score,
            position=position,
            reason=reasons,
            timestamp=pd.Timestamp.now(),
            strategy=strategy.get('meta', {}).get('name', 'YAML策略'),
            price=df['close'].iloc[-1],
            target_price=df['close'].iloc[-1] * (1 + exit_config.get('take_profit', 0.15)),
            stop_loss=df['close'].iloc[-1] * (1 - exit_config.get('stop_loss', 0.05)),
        )

        return signal

    def _calculate_position(self, score: float, market: MarketState, strategy: Dict) -> float:
        """计算仓位"""
        base = 0.3
        market_factor = market.factor
        score_factor = score / 100
        position = base * market_factor * score_factor
        return max(0.1, min(0.8, position))

    def list_yaml_strategies(self) -> List[Dict]:
        """列出所有YAML策略"""
        return self.yaml_loader.list_strategies()

    def _run_builtin_strategies(self, df: pd.DataFrame, indicators: dict, market: MarketState) -> Optional[Signal]:
        """运行内置策略(趋势/低吸)，同时尝试两种策略取最优"""
        candidates = []
        # 不再严格按市场类型单选，而是两种都试，取评分更高的
        if market.type in [MarketType.BULL, MarketType.BULL_WEAK]:
            s = self.trend_strategy.analyze(df, indicators, market)
            if s:
                candidates.append(s)
        if market.type in [MarketType.SIDEWAYS, MarketType.BULL_WEAK]:
            s = self.dip_strategy.analyze(df, indicators, market)
            if s:
                candidates.append(s)
        # 在震荡市中两种策略都尝试
        if market.type in [MarketType.BULL]:
            s = self.dip_strategy.analyze(df, indicators, market)
            if s and s.action == SignalAction.BUY:
                candidates.append(s)
        if not candidates:
            return None
        # 返回评分最高的信号
        return max(candidates, key=lambda s: s.score)

    def _apply_factor_scoring(self, signal: Signal, df: pd.DataFrame, indicators: dict) -> Optional[Signal]:
        """
        FactorScorer多因子融合校验
        
        策略评分与多因子评分加权融合:
        - 因子分 >= 55: 正常通过，融合加分
        - 因子分 40-55: 评分打9折，降级风险
        - 因子分 < 40: 直接拒绝（多因子不支持）
        """
        try:
            factors = self.factor_scorer.calculate_factors(df, indicators)
            factor_score = factors.get('composite_score', 50)
        except Exception as e:
            logger.warning(f"FactorScorer failed: {e}")
            return signal
        
        # 融合策略分与因子分: 策略分占70%，因子分占30%
        strategy_score = signal.score
        blended = strategy_score * 0.7 + factor_score * 0.3
        
        # 因子分太低则直接拒绝（多因子不认可策略信号）
        if factor_score < 40 and signal.action == SignalAction.BUY:
            logger.info(f"FactorScorer reject: factor={factor_score:.1f} < 40, strategy={strategy_score:.1f}")
            return None
        
        # 因子分偏低时，信号降级
        if factor_score < 55 and signal.action == SignalAction.BUY:
            signal.action = SignalAction.WATCH
            signal.level = None
            logger.info(f"FactorScorer downgrade: factor={factor_score:.1f}, BUY->WATCH")
        
        # 用融合分替换原始分
        signal.score = round(blended, 1)
        
        # 添加因子亮点到理由
        top_factors = self._get_top_factors(factors)
        if top_factors:
            signal.reason = (signal.reason or []) + [f"因子: {top_factors}"]
        
        return signal

    def _get_top_factors(self, factors: Dict) -> str:
        """获取因子得分最高的2个因子作为摘要"""
        factor_names = {
            'value_score': '估值', 'momentum_score': '动量', 'liquidity_score': '流动性',
            'stability_score': '稳定性', 'volume_score': '量能', 'trend_score': '趋势',
            'fund_flow_score': '资金流', 'technical_score': '技术'
        }
        scored = []
        for key, label in factor_names.items():
            val = factors.get(key, 0)
            if val >= 70:
                scored.append((label, val))
        scored.sort(key=lambda x: x[1], reverse=True)
        top = scored[:2]
        if not top:
            return ''
        return ', '.join(f"{label}{val:.0f}" for label, val in top)