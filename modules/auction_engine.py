"""
集合竞价分析引擎
- 简化版：基于qtimg数据（当前可用）
- 专业版：基于Level-2数据（预留接口）
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from datetime import datetime, time
from enum import Enum
import os
import logging
import requests

logger = logging.getLogger("guardian")


class AuctionPhase(Enum):
    PRE_OPEN = "09:15-09:20"
    LOCK_OPEN = "09:20-09:25"
    MATCH_OPEN = "09:25"
    PRE_CLOSE = "14:57-15:00"


@dataclass
class AuctionStock:
    """竞价股票数据"""

    code: str
    name: str
    price: float
    pct_change: float
    volume: int
    amount: float
    turnover_rate: float
    sector: Optional[str] = None
    source: str = "qtimg"


@dataclass
class AuctionSignal:
    """竞价信号"""

    code: str
    name: str
    timestamp: datetime
    score: float
    price: float = 0.0
    turnover_rate: float = 0.0
    pct_change: float = 0.0
    amount: float = 0.0
    volume: float = 0.0
    tags: List[str] = field(default_factory=list)
    suggestion: str = ""


class AuctionAnalyzer:
    """集合竞价分析器 - 简化版（基于qtimg）"""

    def __init__(self):
        self.cache_ttl = 300  # 5分钟缓存
        self.last_fetch = 0

    def get_auction_top(self, limit: int = 10) -> List[AuctionStock]:
        """获取竞价Top10（当前通过qtimg）"""
        from .utils import request_with_throttle
        from .market import to_float

        codes = self._build_top_codes(limit)
        if not codes:
            return []

        url = f"http://qt.gtimg.cn/q={','.join(codes)}"
        try:
            response = request_with_throttle(url, timeout=10)
            if response.status_code != 200:
                return []

            text = response.text.strip()
            if not text or "v_pv_none_match" in text:
                return []

            stocks = text.split(";")
            results = []

            for stock_data in stocks:
                if not stock_data or "=" not in stock_data:
                    continue
                try:
                    parts = stock_data.split("=")
                    raw_code = parts[0].replace("v_", "")
                    data = parts[1].split("~")

                    if len(data) < 32:
                        continue

                    code = raw_code.replace("sz", "").replace("sh", "")
                    name = data[1] if len(data) > 1 else ""
                    price = to_float(data[3])
                    pct_change = to_float(data[31], scale=100)
                    volume = to_float(data[5])
                    amount = to_float(data[6])
                    turnover = to_float(data[38])

                    if code and name and price:
                        results.append(
                            AuctionStock(
                                code=raw_code,
                                name=name,
                                price=price,
                                pct_change=pct_change,
                                volume=volume or 0,
                                amount=amount or 0,
                                turnover_rate=turnover or 0,
                            )
                        )
                except (ValueError, IndexError):
                    continue

                if len(results) >= limit:
                    break

            return results

        except Exception as exc:
            logger.warning("auction_top_fetch_failed err=%s", exc)
            return []

    def _build_top_codes(self, limit: int) -> List[str]:
        """构建需要获取的股票代码列表"""
        codes = []
        per_market = max(limit // 4, 5)

        # 沪市主板 (600000-603999)
        for i in range(600000, 600000 + per_market * 4):
            codes.append(f"sh{i}")

        # 深市主板 (000001-000999)
        for i in range(1, per_market + 1):
            codes.append(f"sz{i:06d}")

        # 中小板 (002000-002999)
        for i in range(2000, 2000 + per_market):
            codes.append(f"sz{i:06d}")

        # 创业板 (300000-300999)
        for i in range(300000, 300000 + per_market):
            codes.append(f"sz{i}")

        return codes[: limit * 4]

    def analyze_stocks(self, stocks: List[AuctionStock]) -> List[AuctionSignal]:
        """分析股票生成信号"""
        signals = []

        for stock in stocks:
            score = self._calculate_score(stock)
            tags = self._generate_tags(stock)
            suggestion = self._generate_suggestion(score, stock)

            signals.append(
                AuctionSignal(
                    code=stock.code,
                    name=stock.name,
                    timestamp=datetime.now(),
                    score=score,
                    price=stock.price or 0,
                    turnover_rate=stock.turnover_rate or 0,
                    pct_change=stock.pct_change or 0,
                    amount=stock.amount or 0,
                    volume=stock.volume or 0,
                    tags=tags,
                    suggestion=suggestion,
                )
            )

        return sorted(signals, key=lambda x: x.score, reverse=True)

    def _calculate_score(self, stock: AuctionStock) -> float:
        """计算竞价评分"""
        score = 50

        # 涨幅评分 (0-25)
        pct = stock.pct_change or 0
        if 3 <= pct <= 6:
            score += 25
        elif 0 <= pct < 3:
            score += 15
        elif 6 < pct <= 9:
            score += 20
        elif pct < 0:
            score += 0
        else:
            score += 10

        # 换手率评分 (0-15)
        turnover = stock.turnover_rate or 0
        if turnover > 5:
            score += 15
        elif turnover > 2:
            score += 10
        elif turnover > 0.5:
            score += 5

        # 量能评分 (0-10)
        amount = stock.amount or 0
        if amount > 500000000:  # 5亿+
            score += 10
        elif amount > 100000000:  # 1亿+
            score += 5

        return min(100, score)

    def _generate_tags(self, stock: AuctionStock) -> List[str]:
        """生成标签"""
        tags = []
        pct = stock.pct_change or 0

        if pct > 7:
            tags.append("大幅高开")
        elif pct > 4:
            tags.append("温和高开")
        elif pct > 0:
            tags.append("红盘")
        else:
            tags.append("低开")

        turnover = stock.turnover_rate or 0
        if turnover > 5:
            tags.append("抢筹")
        elif turnover > 2:
            tags.append("放量")

        amount = stock.amount or 0
        if amount > 100000000:
            tags.append("资金活跃")

        return tags

    def _generate_suggestion(self, score: float, stock: AuctionStock) -> str:
        """生成建议"""
        if score >= 85:
            return "强势关注，开盘跟进"
        elif score >= 70:
            return "重点关注，观察确认"
        elif score >= 50:
            return "适度关注，需结合板块"
        else:
            return "观望为主"


class AuctionStrategyEngine:
    """
    专业竞价策略引擎（预留接口）
    需要Level-2数据才能启用
    """

    def __init__(self):
        self.tick_history: Dict[str, List] = {}
        self.signals: List[AuctionSignal] = []

        self.weights = {
            "price_trend": 0.25,
            "volume_burst": 0.20,
            "unmatched_power": 0.20,
            "gap_rational": 0.15,
            "order_structure": 0.15,
        }

        self.enabled = False  # 默认关闭，需Level-2数据
        logger.info("AuctionStrategyEngine initialized (Level-2 required)")

    def enable_professional_mode(self):
        """启用专业模式（需接入Level-2数据源）"""
        self.enabled = True
        logger.info("AuctionStrategyEngine: professional mode enabled")

    def is_enabled(self) -> bool:
        return self.enabled


class AuctionScanner:
    """竞价扫描器（预留接口）"""

    def __init__(self):
        self.watch_list = []
        self.is_running = False

    async def start(self, watch_list: List[str]):
        """启动扫描"""
        self.watch_list = watch_list
        self.is_running = True
        logger.info(f"AuctionScanner started: {len(watch_list)} stocks")

    async def scan_once(self):
        """单次扫描"""
        pass


def get_auction_signals(limit: int = 15) -> List[AuctionSignal]:
    """获取竞价信号（主入口）"""
    analyzer = AuctionAnalyzer()
    stocks = analyzer.get_auction_top(limit)
    return analyzer.analyze_stocks(stocks)


def get_auction_index_summary() -> List[Dict]:
    """获取竞价时段指数行情"""
    from .utils import request_with_throttle

    codes = ["sh000001", "sz399001", "sz399006"]  # 上证、深证、创业板
    url = f"http://qt.gtimg.cn/q={','.join(codes)}"

    try:
        response = request_with_throttle(url, timeout=10)
        if response.status_code != 200:
            return []

        text = response.text.strip()
        if not text or "v_pv_none_match" in text:
            return []

        stocks = text.split(";")
        results = []

        for stock_data in stocks:
            if not stock_data or "=" not in stock_data:
                continue
            try:
                parts = stock_data.split("=")
                raw_code = parts[0].replace("v_", "")
                data = parts[1].split("~")

                if len(data) < 32:
                    continue

                name = data[1] if len(data) > 1 else ""
                price = float(data[3]) if data[3] and data[3] != "-" else None
                pct_change = (
                    float(data[31]) / 100 if data[31] and data[31] != "-" else None
                )

                if name and price is not None:
                    results.append(
                        {
                            "code": raw_code,
                            "name": name,
                            "price": price,
                            "pct_change": pct_change,
                        }
                    )
            except (ValueError, IndexError):
                continue

        return results

    except Exception as e:
        logger.warning("auction_index_fetch_failed err=%s", e)
        return []


def format_auction_alert(signals: List[AuctionSignal]) -> str:
    """格式化竞价提醒"""
    lines = []

    # 1. 指数行情
    index_data = get_auction_index_summary()
    if index_data:
        lines.append("【指数竞价情况】")
        for idx in index_data:
            name = idx.get("name", "")
            code = idx.get("code", "")
            price = idx.get("price", 0)
            pct = idx.get("pct_change", 0) or 0
            ball = "+" if pct >= 0 else ""
            lines.append(f"  {name}({code}) {price:.2f} {ball}{pct:+.2f}%")
        lines.append("")

    # 2. 竞价股票列表
    if not signals:
        lines.append("【竞价排行】暂无数据")
        return "\n".join(lines)

    lines.append("【全市场竞价Top10】")
    for i, sig in enumerate(signals[:10], 1):
        name = sig.name[:12] if sig.name else ""
        code = sig.code[-6:] if sig.code else ""
        pct = sig.pct_change or 0
        turnover = sig.turnover_rate or 0
        amount = sig.amount or 0
        amount_wan = amount / 10000 if amount > 0 else 0

        sign = "+" if pct >= 0 else ""
        pct_str = f"{sign}{pct:.2f}%"
        amount_str = f"成交{amount_wan:.0f}万" if amount_wan > 0 else ""
        turnover_str = f"换手{turnover:.1f}%" if turnover > 0 else ""

        extra = " ".join([amount_str, turnover_str]).strip()
        if extra:
            lines.append(f"  {i}. {name}({code}) {pct_str} {extra}")
        else:
            lines.append(f"  {i}. {name}({code}) {pct_str}")

    return "\n".join(lines)


# ============ 专业版：自适应参数策略系统 ============
# 需要Level-2数据才能启用


class MarketRegime(Enum):
    """市场状态"""

    BULL_EXTREME = "极端牛市"
    BULL_NORMAL = "正常牛市"
    BALANCE = "震荡平衡"
    BEAR_WEAK = "弱势震荡"
    BEAR_EXTREME = "极端熊市"


class StockType(Enum):
    """个股类型"""

    LEADER_ACCEL = "龙头-加速"
    LEADER_DIVERGE = "龙头-分歧"
    FOLLOWER_STRONG = "跟风-强势"
    FOLLOWER_WEAK = "跟风-弱势"
    BREAKOUT_NEW_HIGH = "突破-历史新高"
    BREAKOUT_PLATFORM = "突破-平台"
    REVERSAL_OVERSOLD = "反转-超跌"
    IPO_NEW = "新股"
    GENERAL = "普通"


@dataclass
class DynamicParams:
    """动态参数包"""

    score_threshold: float = 80.0
    gap_min: float = 2.0
    gap_max: float = 6.0
    volume_ratio_min: float = 2.5
    unmatched_ratio_min: float = 40.0
    time_cutoff: str = "09:23:30"
    position_pct: float = 10.0
    stop_loss: float = -3.0
    take_profit: float = 6.0
    max_daily: int = 5
    allow_limit_up: bool = False
    need_sector: bool = True
    enable_contrarian: bool = False


class AdaptiveParamEngine:
    """自适应参数引擎"""

    def __init__(self):
        self.current_regime = MarketRegime.BALANCE
        self.param_library = self._init_param_library()
        self.active_params = self.param_library[self.current_regime]
        self.regime_history = []

    def _init_param_library(self) -> Dict:
        return {
            MarketRegime.BULL_EXTREME: DynamicParams(
                score_threshold=72,
                gap_min=0,
                gap_max=10,
                volume_ratio_min=1.8,
                unmatched_ratio_min=25,
                time_cutoff="09:22:00",
                position_pct=15,
                stop_loss=-2.5,
                take_profit=10,
                max_daily=10,
                allow_limit_up=True,
                need_sector=False,
                enable_contrarian=False,
            ),
            MarketRegime.BULL_NORMAL: DynamicParams(
                score_threshold=78,
                gap_min=2,
                gap_max=7,
                volume_ratio_min=2.2,
                unmatched_ratio_min=35,
                time_cutoff="09:22:30",
                position_pct=12,
                stop_loss=-3,
                take_profit=8,
                max_daily=6,
                allow_limit_up=True,
                need_sector=True,
                enable_contrarian=False,
            ),
            MarketRegime.BALANCE: DynamicParams(
                score_threshold=85,
                gap_min=2,
                gap_max=5,
                volume_ratio_min=3.0,
                unmatched_ratio_min=50,
                time_cutoff="09:24:00",
                position_pct=8,
                stop_loss=-3,
                take_profit=5,
                max_daily=3,
                allow_limit_up=False,
                need_sector=True,
                enable_contrarian=True,
            ),
            MarketRegime.BEAR_WEAK: DynamicParams(
                score_threshold=90,
                gap_min=3,
                gap_max=5,
                volume_ratio_min=4.0,
                unmatched_ratio_min=60,
                time_cutoff="09:24:30",
                position_pct=5,
                stop_loss=-2,
                take_profit=4,
                max_daily=2,
                allow_limit_up=False,
                need_sector=True,
                enable_contrarian=True,
            ),
            MarketRegime.BEAR_EXTREME: DynamicParams(
                score_threshold=95,
                gap_min=5,
                gap_max=8,
                volume_ratio_min=5.0,
                unmatched_ratio_min=80,
                time_cutoff="09:25:00",
                position_pct=0,
                stop_loss=-1,
                take_profit=2,
                max_daily=0,
                allow_limit_up=False,
                need_sector=True,
                enable_contrarian=False,
            ),
        }

    def detect_regime(self, market_data: Dict) -> MarketRegime:
        limit_up_ratio = market_data.get("limit_up_count", 0) / max(
            market_data.get("total_stocks", 5000), 1
        )
        limit_down_ratio = market_data.get("limit_down_count", 0) / max(
            market_data.get("total_stocks", 5000), 1
        )
        auction_bull = market_data.get("auction_up_ratio", 0.5)
        index_5d = market_data.get("index_5d_change", 0)

        if limit_up_ratio > 0.08 and auction_bull > 0.65:
            return MarketRegime.BULL_EXTREME
        elif limit_down_ratio > 0.03 or (index_5d < -4 and auction_bull < 0.35):
            return MarketRegime.BEAR_EXTREME
        elif index_5d > 1.5 and auction_bull > 0.55:
            return MarketRegime.BULL_NORMAL
        elif index_5d < -1.5 or auction_bull < 0.42:
            return MarketRegime.BEAR_WEAK
        return MarketRegime.BALANCE

    def update(self, market_data: Dict) -> DynamicParams:
        new_regime = self.detect_regime(market_data)
        if new_regime != self.current_regime:
            logger.info(f"[状态切换] {self.current_regime.value} -> {new_regime.value}")
            self.current_regime = new_regime
            self.active_params = self.param_library[new_regime]
        return self.active_params

    def get_current_regime(self) -> str:
        return self.current_regime.value


class AuctionStrategySystem:
    """专业竞价策略系统（需要Level-2数据）"""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self.param_engine = AdaptiveParamEngine()
        self.enabled = False
        self.signals = []
        logger.info("AuctionStrategySystem initialized (Level-2 required)")

    def enable(self):
        """启用专业模式"""
        self.enabled = True
        logger.info("AuctionStrategySystem: professional mode enabled")

    def disable(self):
        """禁用专业模式"""
        self.enabled = False
        logger.info("AuctionStrategySystem: professional mode disabled")

    def is_enabled(self) -> bool:
        return self.enabled

    def process(self, stock_data: Dict, market_data: Dict) -> Optional[Dict]:
        """处理数据并生成信号"""
        if not self.enabled:
            return None

        params = self.param_engine.update(market_data)
        regime = self.param_engine.get_current_regime()

        signal = {
            "regime": regime,
            "params": {
                "threshold": params.score_threshold,
                "gap_range": f"{params.gap_min}-{params.gap_max}%",
                "volume_min": params.volume_ratio_min,
                "position": f"{params.position_pct}%",
            },
        }
        return signal

    def get_regime(self) -> str:
        return self.param_engine.get_current_regime()


def get_professional_system() -> AuctionStrategySystem:
    """获取专业策略系统实例"""
    return AuctionStrategySystem()
