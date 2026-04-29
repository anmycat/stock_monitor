"""
专业交易员决策引擎
像受过严格训练的高级交易员一样思考和决策
"""
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from .market import get_quote
from .human_thinking import (
    get_market_context,
    get_time_pattern,
    TradingReason,
)


class ProfessionalTrader:
    """
    专业交易员决策引擎
    
    决策流程（严格按照优先级）:
    1. 市场环境评估 - 最重要，决定整体仓位
    2. 时间窗口判断 - 根据时段调整策略
    3. 板块轮动分析 - 把握市场方向
    4. 个股筛选 - 精选标的
    5. 风险评估 - 最终确认
    """
    
    def __init__(self):
        self.reason_gen = TradingReason()
        self.decision_log = []
        self.cache = {}
        self.cache_ttl = 300  # 5分钟缓存
        
    def _get_cache(self, key: str) -> Optional[Any]:
        """获取缓存"""
        if key in self.cache:
            cached = self.cache[key]
            if time.time() - cached['ts'] < self.cache_ttl:
                return cached['data']
        return None
    
    def _set_cache(self, key: str, data: Any):
        """设置缓存"""
        self.cache[key] = {'data': data, 'ts': time.time()}
    
    def assess_market(self, force: bool = False) -> Dict:
        """
        第一步：市场环境评估（最重要）
        像专业交易员一样，先判断大势
        """
        # 检查缓存
        if not force:
            cached = self._get_cache('market_assessment')
            if cached:
                self._log("市场评估", "使用缓存数据", cached)
                return cached
        
        self._log("市场评估", "开始评估市场环境", {})
        
        # 获取市场数据
        market_context = get_market_context()
        time_pattern = get_time_pattern()
        
        # 综合评估
        ctx = market_context.get("context", "UNKNOWN")
        
        if ctx == "BULL":
            assessment = {
                "trend": "上涨趋势",
                "signal": "积极做多",
                "confidence": 0.85,
                "position": "8成仓",
                "action": "持股待涨，可适度加仓",
            }
        elif ctx == "BULL_WEAK":
            assessment = {
                "trend": "震荡上行",
                "signal": "谨慎做多",
                "confidence": 0.65,
                "position": "5成仓",
                "action": "精选个股，快进快出",
            }
        elif ctx == "BEAR":
            assessment = {
                "trend": "下跌趋势",
                "signal": "观望为主",
                "confidence": 0.85,
                "position": "2成仓",
                "action": "持币观望，不抄底",
            }
        elif ctx == "BEAR_WEAK":
            assessment = {
                "trend": "震荡下行",
                "signal": "防守为主",
                "confidence": 0.70,
                "position": "3成仓",
                "action": "轻仓观望，逆势不抄底",
            }
        elif ctx == "CONSOLIDATION":
            assessment = {
                "trend": "横盘震荡",
                "signal": "高抛低吸",
                "confidence": 0.60,
                "position": "5成仓",
                "action": "区间操作，越涨越卖",
            }
        else:
            assessment = {
                "trend": "方向不明",
                "signal": "观望",
                "confidence": 0.50,
                "position": "3成仓",
                "action": "保持观察，等待信号",
            }
        
        # 添加上下文信息
        assessment.update({
            "context": ctx,
            "description": market_context.get("description", ""),
            "time_period": time_pattern.get("period", ""),
            "time_advice": time_pattern.get("advice", ""),
            "timestamp": time.time(),
        })
        
        self._set_cache('market_assessment', assessment)
        self._log("市场评估", assessment['trend'], assessment)
        
        return assessment
    
    def analyze_sectors(self, force: bool = False) -> Dict:
        """
        第三步：板块轮动分析
        把握市场主线
        """
        if not force:
            cached = self._get_cache('sector_analysis')
            if cached:
                return cached
        
        self._log("板块分析", "开始分析板块轮动", {})
        
        # 简化板块分析 - 使用qtimg获取板块数据
        try:
            from .sector_analysis import get_sector_top_stocks
            
            # 热门板块
            hot_sectors = ["人工智能", "新能源", "半导体", "医药", "银行"]
            sector_data = {}
            
            for sector in hot_sectors[:3]:
                try:
                    stocks = get_sector_top_stocks(sector, 5)
                    if stocks:
                        avg_pct = sum(s.get('pct_change', 0) for s in stocks) / len(stocks)
                        sector_data[sector] = {
                            'count': len(stocks),
                            'avg_pct': avg_pct,
                            'top_stock': stocks[0] if stocks else {}
                        }
                except Exception:
                    pass
            
            # 判断主线
            leading_sectors = [k for k, v in sector_data.items() if v.get('avg_pct', 0) > 2]
            
            result = {
                "sectors": sector_data,
                "leading": leading_sectors,
                "strategy": "追随主线" if leading_sectors else "观望",
                "timestamp": time.time(),
            }
            
        except Exception as e:
            result = {
                "sectors": {},
                "leading": [],
                "strategy": "无法分析",
                "error": str(e),
                "timestamp": time.time(),
            }
        
        self._set_cache('sector_analysis', result)
        self._log("板块分析", result.get('strategy', 'N/A'), result)
        
        return result
    
    def scan_watchlist(self, watch_stocks: List[Dict]) -> List[Dict]:
        """
        第四步：自选股扫描
        对自选股进行多维度分析
        """
        if not watch_stocks:
            return []
        
        self._log("自选股扫描", f"开始扫描{len(watch_stocks)}只股票", {})
        
        results = []
        
        for stock in watch_stocks:
            code = stock.get('code')
            name = stock.get('name', code)
            
            try:
                # 获取行情数据
                quote = get_quote(code)
                if not quote:
                    continue
                
                pct = quote.get('pct_change', 0) or 0
                turnover = quote.get('turnover_rate', 0) or 0
                
                # 快速筛选条件
                signal = "观察"
                priority = 0
                
                # 涨跌幅判断
                if 3 <= pct <= 9:
                    signal = "关注"
                    priority += 2
                elif pct > 9:
                    signal = "警惕"
                    priority -= 1
                elif pct < -5:
                    signal = "观望"
                    priority -= 2
                
                # 换手率判断
                if turnover >= 5:
                    priority += 1
                elif turnover < 1:
                    priority -= 1
                
                results.append({
                    'code': code,
                    'name': name,
                    'price': quote.get('price'),
                    'pct_change': pct,
                    'turnover_rate': turnover,
                    'signal': signal,
                    'priority': priority,
                    'sector': stock.get('sector', ''),
                })
                
            except Exception as e:
                self._log("扫描", f"{code}失败: {e}", {})
        
        # 按优先级排序
        results.sort(key=lambda x: x.get('priority', 0), reverse=True)
        
        self._log("自选股扫描", f"找到{len(results)}只重点关注", results[:3])
        
        return results
    
    def make_decision(self, watch_stocks: List[Dict], news: List[Dict] = None) -> Dict:
        """
        综合决策 - 像专业交易员一样做决定
        """
        decision = {
            "timestamp": datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S"),
            "steps": [],
            "final": {},
        }
        
        # Step 1: 市场环境评估
        market = self.assess_market()
        decision["steps"].append({
            "step": "1. 市场环境",
            "conclusion": f"{market['trend']} - {market['action']}",
            "details": market,
        })
        
        # Step 2: 时间窗口
        time_info = get_time_pattern()
        decision["steps"].append({
            "step": "2. 时间窗口",
            "conclusion": time_info.get('description', ''),
            "details": time_info,
        })
        
        # Step 3: 板块轮动
        sectors = self.analyze_sectors()
        decision["steps"].append({
            "step": "3. 板块方向",
            "conclusion": f"{'，'.join(sectors.get('leading', ['无']))} - {sectors.get('strategy', '')}",
            "details": sectors,
        })
        
        # Step 4: 自选股扫描
        watch_results = self.scan_watchlist(watch_stocks)
        top_picks = watch_results[:5] if watch_results else []
        decision["steps"].append({
            "step": "4. 个股筛选",
            "conclusion": f"重点关注{len(top_picks)}只",
            "details": top_picks,
        })
        
        # Step 5: 综合决策
        # 根据市场环境调整
        position = market.get('position', '3成仓')
        confidence = market.get('confidence', 0.5)
        
        # 如果板块有明确主线，增加信心
        if sectors.get('leading'):
            confidence = min(0.95, confidence + 0.1)
        
        # 如果有强势股，增加信心
        strong_stocks = [s for s in top_picks if s.get('signal') == '关注']
        if strong_stocks:
            confidence = min(0.95, confidence + 0.05)
        
        decision["final"] = {
            "recommended_position": position,
            "confidence": confidence,
            "top_picks": top_picks,
            "action": self._generate_action(market, sectors, top_picks),
            "reasons": self._generate_reasons(market, sectors, top_picks),
        }
        
        self._log("综合决策", f"仓位建议:{position}", decision["final"])
        
        return decision
    
    def _generate_action(self, market: Dict, sectors: Dict, picks: List[Dict]) -> str:
        """生成操作建议"""
        ctx = market.get('context', 'UNKNOWN')
        
        if ctx == "BEAR":
            return "持币观望，不开新仓"
        elif ctx == "BULL" and picks:
            return f"积极做多，重点关注{picks[0].get('name', '')}"
        elif picks:
            return f"精选个股，关注{picks[0].get('name', '')}"
        else:
            return "保持观察，等待机会"
    
    def _generate_reasons(self, market: Dict, sectors: Dict, picks: List[Dict]) -> List[str]:
        """生成决策理由"""
        reasons = []
        
        # 市场理由
        reasons.append(f"市场{market.get('trend')}，{market.get('action')}")
        
        # 板块理由
        if sectors.get('leading'):
            reasons.append(f"板块轮动至{sectors['leading'][0]}，存在机会")
        
        # 个股理由
        if picks:
            top = picks[0]
            reasons.append(f"自选股{top.get('name')}涨幅{top.get('pct_change', 0):.1f}%")
        
        return reasons
    
    def _log(self, stage: str, message: str, data: Any):
        """记录决策日志"""
        self.decision_log.append({
            "stage": stage,
            "message": message,
            "data": data,
            "ts": time.time(),
        })
    
    def get_report(self) -> str:
        """生成人类可读的交易报告"""
        if not self.decision_log:
            return "暂无决策记录"
        
        lines = []
        lines.append("=" * 50)
        lines.append("专业交易员决策报告")
        lines.append("=" * 50)
        
        for log in self.decision_log[-10:]:  # 最近10条
            lines.append(f"\n【{log['stage']}】")
            lines.append(f"  {log['message']}")
        
        return "\n".join(lines)


# 全局实例
_trader_instance = None

def get_professional_trader() -> ProfessionalTrader:
    """获取专业交易员实例"""
    global _trader_instance
    if _trader_instance is None:
        _trader_instance = ProfessionalTrader()
    return _trader_instance


def quick_trade_decision(watch_stocks: List[Dict]) -> Dict:
    """快速交易决策"""
    trader = get_professional_trader()
    return trader.make_decision(watch_stocks)
