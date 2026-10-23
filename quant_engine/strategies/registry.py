"""统一策略注册中心 - 整合 Dispatcher + Funnel 双路径"""
from __future__ import annotations

from datetime import datetime

from loguru import logger

from ..models.market import MarketState
from ..models.signal import Signal, SignalAction, SignalLevel
from .dispatcher import StrategyDispatcher
from .funnel import ThreeLayerFunnel
from .yaml_loader import get_yaml_loader


class StrategyRegistry:
    """
    统一策略注册中心
    
    整合两条策略路径:
    - 单股策略路径: StrategyDispatcher (Trend/Dip/YAML)
    - 全市场漏斗路径: ThreeLayerFunnel (纯YAML三层筛选)
    """

    def __init__(self, datacenter, funnel=None, llm_ranker=None, context_enricher=None):
        self.datacenter = datacenter
        self.dispatcher = StrategyDispatcher(datacenter)
        # funnel/增强器可注入（api 层 L0 注入 services 实例，L2 不再反向依赖 L1，
        # prd/15 §3.2 P4 清单）；缺省自建 + 无增强器，向后兼容既有调用方
        self.funnel = funnel or ThreeLayerFunnel(datacenter, llm_ranker, context_enricher)
        self.yaml_loader = get_yaml_loader()
        self._strategies = {}
        self._auto_register_builtins()

    def _auto_register_builtins(self):
        """自动注册内置策略"""
        # 内置单股策略
        for strategy_id, info in [
            ('trend', {'name': '趋势突破策略', 'type': 'single_stock', 'category': '趋势'}),
            ('dip', {'name': '抄底策略', 'type': 'single_stock', 'category': '抄底'}),
        ]:
            self._strategies[strategy_id] = info

        # 注册所有YAML策略
        for s in self.yaml_loader.list_strategies():
            self._strategies[s['id']] = {
                'name': s['name'],
                'type': 'market_scan',  # YAML策略走漏斗
                'category': s.get('category', s['id']),
                'description': s.get('description', ''),
            }

    def register(self, strategy_id: str, strategy_type: str, config: dict):
        """手动注册策略"""
        self._strategies[strategy_id] = config
        self._strategies[strategy_id]['type'] = strategy_type

    def get_available_strategies(self) -> list[dict]:
        """列出所有可用策略"""
        result = []
        for sid, info in self._strategies.items():
            result.append({
                'id': sid,
                'name': info.get('name', sid),
                'category': info.get('category', '未知'),
                'description': info.get('description', ''),
                'type': info.get('type', 'single_stock'),
            })
        return result

    def scan_stock(self, code: str, name: str, market: MarketState,
                   trades: list[dict] | None = None, equity_curve: list[float] | None = None,
                   strategy_id: str | None = None) -> Signal | None:
        """
        单股扫描（走 Dispatcher 路径）
        
        Args:
            code: 股票代码
            name: 股票名称
            market: 市场状态
            trades: 交易记录
            equity_curve: 资金曲线
            strategy_id: 可选YAML策略ID
        """
        df = self.datacenter.get_daily_data(code, 120)
        if df is None or df.empty:
            return None
        df['code'] = code
        df['name'] = name
        return self.dispatcher.dispatch(df, market, trades, equity_curve, strategy_id)

    def screen(self, strategy_id: str, market: MarketState,
               max_output: int = 10) -> list[dict]:
        """
        全市场漏斗筛选（走 Funnel 路径）
        
        Returns:
            候选股票 Dict 列表
        """
        return self.funnel.screen(strategy_id, market, max_output)

    def screen_as_signals(self, strategy_id: str, market: MarketState,
                          max_output: int = 10) -> list[Signal]:
        """
        全市场漏斗筛选，结果转为 Signal 对象
        
        Returns:
            Signal 对象列表
        """
        results = self.funnel.screen(strategy_id, market, max_output)
        signals = []
        for r in results:
            try:
                signal = self._dict_to_signal(r)
                signals.append(signal)
            except (KeyError, TypeError, ValueError) as e:
                logger.debug(f"Dict->Signal conversion error: {e}")
        return signals

    def scan_all(self, market: MarketState, max_signals: int = 3,
                 trades: list[dict] | None = None, equity_curve: list[float] | None = None) -> list[Signal]:
        """
        遍历所有启用的单股策略扫描信号
        
        注意: scan_all 的实际实现在 SignalGenerator 中（需要遍历股票列表）
        此方法仅作为 Registry 的便捷接口，内部委托给 dispatcher
        """
        logger.warning("StrategyRegistry.scan_all 应由 SignalGenerator.scan_all 调用")
        return []

    def list_strategies(self) -> list[dict]:
        """列出所有策略（兼容旧接口）"""
        return self.get_available_strategies()

    @staticmethod
    def _dict_to_signal(d: dict) -> Signal:
        """Funnel 返回的 Dict 转 Signal 对象"""
        action_map = {
            'BUY': SignalAction.BUY,
            'WATCH': SignalAction.WATCH,
            'SKIP': SignalAction.SKIP,
        }
        level_map = {
            'S': SignalLevel.S,
            'A': SignalLevel.A,
            'B': SignalLevel.B,
        }
        action = action_map.get(d.get('action', 'SKIP'), SignalAction.SKIP)
        level = level_map.get(d.get('level'), None)

        return Signal(
            code=d['code'],
            name=d['name'],
            action=action,
            level=level,
            score=d.get('score', 0),
            position=d.get('position', 0.1),
            reason=d.get('reasons', []),
            timestamp=datetime.now(),
            strategy=d.get('strategy', ''),
            price=d.get('price', 0),
            target_price=d.get('target_price'),
            stop_loss=d.get('stop_loss_price'),
        )
