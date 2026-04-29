import os
import time
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from .utils import request_with_throttle, to_float


_MARKET_CONTEXT_CACHE = {"data": None, "ts": 0.0}


def _now_bj():
    return datetime.now(ZoneInfo("Asia/Shanghai"))


class TradingReason:
    """交易决策理由生成器"""
    
    @staticmethod
    def market_context_reason(context: Dict) -> List[str]:
        """市场环境判断理由"""
        reasons = []
        ctx = context.get("context", "UNKNOWN")
        desc = context.get("description", "")
        
        if ctx == "BULL":
            reasons.append(f"✅ 市场强势：{desc}，持股待涨")
        elif ctx == "BULL_WEAK":
            reasons.append(f"📈 市场偏多：{desc}，可适度参与")
        elif ctx == "BEAR":
            reasons.append(f"🔴 市场弱势：{desc}，建议观望")
        elif ctx == "BEAR_WEAK":
            reasons.append(f"⚠️ 市场偏弱：{desc}，控制仓位")
        elif ctx == "CONSOLIDATION":
            reasons.append(f"➡️ 市场震荡：{desc}，高抛低吸")
        else:
            reasons.append(f"❓ 市场不明：保持观察")
        
        return reasons
    
    @staticmethod
    def time_period_reason(period_info: Dict) -> List[str]:
        """时间节点判断理由"""
        reasons = []
        period = period_info.get("period", "")
        advice = period_info.get("advice", "")
        
        if "09:30" in period:
            reasons.append(f"🕘 开盘初期：波动较大，{advice}")
        elif "10:00" in period:
            reasons.append(f"🕐 上午趋势形成期：{advice}")
        elif "11:00" in period:
            reasons.append(f"🕚 午盘前：{advice}")
        elif "13:00" in period:
            reasons.append(f"🕑 下午盘：{advice}")
        elif "14:00" in period:
            reasons.append(f"🕒 尾盘临近：{advice}")
        else:
            reasons.append(f"🕕 收盘期：{advice}")
        
        if period_info.get("is_monday"):
            reasons.append("📅 周一效应：建议观望或轻仓")
        elif period_info.get("is_friday"):
            reasons.append("📅 周五效应：建议减仓过周末")
        
        return reasons
    
    @staticmethod
    def factor_reason(factors: Dict, score_result: Dict) -> List[str]:
        """因子评分判断理由"""
        reasons = []
        details = score_result.get("details", {})
        
        pe_detail = details.get("pe", {})
        if pe_detail.get("value") is not None:
            pe_val = pe_detail["value"]
            pe_score = pe_detail["score"]
            if pe_score >= 70:
                reasons.append(f"📊 PE={pe_val:.1f} 估值合理")
            elif pe_score >= 50:
                reasons.append(f"📊 PE={pe_val:.1f} 估值适中")
            else:
                reasons.append(f"📊 PE={pe_val:.1f} 估值偏高")
        
        turnover_detail = details.get("turnover", {})
        if turnover_detail.get("value") is not None:
            turnover = turnover_detail["value"]
            if turnover >= 8:
                reasons.append(f"🔥 换手率{turnover:.1f}% 高度活跃")
            elif turnover >= 3:
                reasons.append(f"🔥 换手率{turnover:.1f}% 活跃度适中")
            else:
                reasons.append(f"❄️ 换手率{turnover:.1f}% 活跃度较低")
        
        flow_detail = details.get("fund_flow", {})
        if flow_detail.get("value") is not None:
            flow = flow_detail["value"]
            if flow > 10000:
                reasons.append(f"💰 主力净流入{flow/10000:.1f}亿 资金抢入")
            elif flow > 0:
                reasons.append(f"💰 主力净流入{flow/10000:.1f}亿 温和")
            else:
                reasons.append(f"💸 主力净流出{abs(flow)/10000:.1f}亿 谨慎")
        
        momentum_detail = details.get("momentum", {})
        if momentum_detail.get("value") is not None:
            pct = momentum_detail["value"]
            if 0 < pct <= 3:
                reasons.append(f"📈 涨幅{pct:.1f}% 稳步上涨")
            elif pct > 3:
                reasons.append(f"🚀 涨幅{pct:.1f}% 强势上涨")
            else:
                reasons.append(f"📉 跌幅{pct:.1f}% 关注风险")
        
        return reasons
    
    @staticmethod
    def news_reason(news_impact: Dict) -> List[str]:
        """新闻影响判断理由"""
        reasons = []
        impact = news_impact.get("impact", "NEUTRAL")
        summary = news_impact.get("summary", "")
        
        if impact == "POSITIVE":
            reasons.append(f"✅ 新闻利好：{summary}，市场情绪偏多")
        elif impact == "POSITIVE_WEAK":
            reasons.append(f"📈 新闻略利好：{summary}，保持关注")
        elif impact == "NEGATIVE":
            reasons.append(f"🔴 新闻利空：{summary}，注意风险")
        elif impact == "NEGATIVE_WEAK":
            reasons.append(f"⚠️ 新闻略利空：{summary}，谨慎操作")
        else:
            reasons.append(f"➡️ 新闻中性：{summary}")
        
        return reasons
    
    @staticmethod
    def sector_reason(sector_data: Dict) -> List[str]:
        """板块判断理由"""
        reasons = []
        
        leading = sector_data.get("leading_sectors", [])
        if leading:
            top = leading[0]
            reasons.append(f"🔥 领涨板块：{top.get('name')} +{top.get('pct_change'):.1f}%")
        
        rotation = sector_data.get("rotation", "UNKNOWN")
        if rotation == "IN":
            reasons.append("🔄 资金流入板块，可关注")
        elif rotation == "OUT":
            reasons.append("🔄 资金流出板块，观望为主")
        
        return reasons


def _load_market_rows(sample_size: int) -> List[Dict]:
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    page_size = int(os.getenv("MARKET_CONTEXT_PAGE_SIZE", "200"))
    page_size = max(50, min(500, page_size))
    pages = max(1, (sample_size + page_size - 1) // page_size)
    rows: List[Dict] = []
    for pn in range(1, pages + 1):
        params = {
            "pn": pn,
            "pz": page_size,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fid": "f3",
            "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
            "fields": "f2,f3,f6,f12,f14",
        }
        response = request_with_throttle(url, timeout=10, params=params)
        response.raise_for_status()
        payload = response.json()
        diff = ((payload.get("data") or {}).get("diff")) or []
        if not diff:
            break
        for row in diff:
            pct = to_float(row.get("f3"), scale=100)
            if pct is None:
                continue
            rows.append(
                {
                    "pct_change": pct,
                    "amount": to_float(row.get("f6")),
                }
            )
        if len(rows) >= sample_size:
            break
    return rows


def _percentile(sorted_values: List[float], p: float) -> Optional[float]:
    if not sorted_values:
        return None
    if p <= 0:
        return sorted_values[0]
    if p >= 1:
        return sorted_values[-1]
    k = (len(sorted_values) - 1) * p
    f = int(k)
    c = min(len(sorted_values) - 1, f + 1)
    if f == c:
        return sorted_values[f]
    d = k - f
    return sorted_values[f] * (1 - d) + sorted_values[c] * d


def _fallback_context_from_breadth() -> Dict:
    try:
        from .sector_analysis import get_market_breadth
    except Exception:
        return {}

    breadth = get_market_breadth(sample_size=int(os.getenv("MARKET_BREADTH_SAMPLE_SIZE", "5000")))
    if not breadth:
        return {}
    up = int(breadth.get("up", 0))
    down = int(breadth.get("down", 0))
    total = max(1, up + down + int(breadth.get("flat", 0)))
    rising_pct = up / total * 100.0
    if rising_pct >= 62:
        context = "BULL"
        description = "上涨家数占优，市场宽度偏强"
    elif rising_pct >= 54:
        context = "BULL_WEAK"
        description = "上涨家数略占优，市场偏多"
    elif rising_pct <= 35:
        context = "BEAR"
        description = "下跌家数明显占优，市场偏弱"
    elif rising_pct <= 45:
        context = "BEAR_WEAK"
        description = "下跌家数偏多，防守为主"
    else:
        context = "CONSOLIDATION"
        description = "涨跌接近，市场震荡"
    return {
        "context": context,
        "description": description,
        "model": "breadth_fallback",
        "avg_change": 0.0,
        "weighted_avg": None,
        "p25": None,
        "p50": None,
        "p75": None,
        "rising_pct": round(rising_pct, 1),
        "total_stocks": total,
        "timestamp": time.time(),
    }


def _fallback_context_from_indices() -> Dict:
    try:
        from .market import get_quote
    except Exception:
        return {}

    changes = []
    for code in ("sh000001", "sz399001", "sh000300"):
        try:
            quote = get_quote(code)
        except Exception:
            continue
        pct = quote.get("pct_change")
        if pct is not None:
            changes.append(float(pct))
    if not changes:
        return {}
    avg_change = sum(changes) / len(changes)
    if avg_change >= 1.0:
        context, description = "BULL", "主要指数同步走强"
    elif avg_change >= 0.3:
        context, description = "BULL_WEAK", "指数温和偏强"
    elif avg_change <= -1.0:
        context, description = "BEAR", "主要指数同步走弱"
    elif avg_change <= -0.3:
        context, description = "BEAR_WEAK", "指数温和偏弱"
    else:
        context, description = "CONSOLIDATION", "指数震荡整理"
    return {
        "context": context,
        "description": description,
        "model": "index_fallback",
        "avg_change": round(avg_change, 2),
        "weighted_avg": round(avg_change, 2),
        "p25": None,
        "p50": None,
        "p75": None,
        "rising_pct": 0.0,
        "total_stocks": 0,
        "timestamp": time.time(),
    }


def get_market_context() -> Dict:
    """获取市场整体上下文"""
    cache_ttl = int(os.getenv("MARKET_CONTEXT_CACHE_SECONDS", "300"))
    now = time.time()
    
    if _MARKET_CONTEXT_CACHE["data"] is not None and (now - _MARKET_CONTEXT_CACHE["ts"]) < cache_ttl:
        return _MARKET_CONTEXT_CACHE["data"]
    
    try:
        default_size = os.getenv("MARKET_BREADTH_SAMPLE_SIZE", "5000")
        sample_size = int(os.getenv("MARKET_CONTEXT_SAMPLE_SIZE", default_size))
        sample_size = max(200, sample_size)

        rows = _load_market_rows(sample_size)
        if not rows:
            fallback = _fallback_context_from_breadth() or _fallback_context_from_indices()
            if fallback:
                _MARKET_CONTEXT_CACHE["data"] = fallback
                _MARKET_CONTEXT_CACHE["ts"] = now
                return fallback
            return {"context": "UNKNOWN", "avg_change": 0, "rising_pct": 0}
        changes = [r["pct_change"] for r in rows if r.get("pct_change") is not None]
        amounts = [r.get("amount") for r in rows]
        if not changes:
            fallback = _fallback_context_from_breadth() or _fallback_context_from_indices()
            if fallback:
                _MARKET_CONTEXT_CACHE["data"] = fallback
                _MARKET_CONTEXT_CACHE["ts"] = now
                return fallback
            return {"context": "UNKNOWN", "avg_change": 0, "rising_pct": 0}

        min_sample = min(
            max(200, int(os.getenv("MARKET_CONTEXT_MIN_SAMPLE", "800"))),
            max(200, int(sample_size)),
        )
        if len(changes) < min_sample:
            fallback = _fallback_context_from_breadth() or _fallback_context_from_indices()
            if fallback:
                fallback["sample_warning"] = f"small_sample:{len(changes)}<{min_sample}"
                _MARKET_CONTEXT_CACHE["data"] = fallback
                _MARKET_CONTEXT_CACHE["ts"] = now
                return fallback

        rising_count = sum(1 for c in changes if c > 0)
        rising_pct = (rising_count / len(changes)) * 100

        model = os.getenv("MARKET_CONTEXT_MODEL", "percentile").strip().lower()
        avg_change = sum(changes) / len(changes)
        weighted_avg = None
        p25 = p50 = p75 = None
        if model == "amount_weighted":
            total = 0.0
            weight_sum = 0.0
            for row in rows:
                pct = row.get("pct_change")
                amt = row.get("amount") or 0.0
                if pct is None:
                    continue
                weight = float(amt) if amt and amt > 0 else 1.0
                total += pct * weight
                weight_sum += weight
            weighted_avg = total / weight_sum if weight_sum > 0 else avg_change
            key_value = weighted_avg
        else:
            sorted_changes = sorted(changes)
            p25 = _percentile(sorted_changes, 0.25)
            p50 = _percentile(sorted_changes, 0.50)
            p75 = _percentile(sorted_changes, 0.75)
            key_value = p50 if p50 is not None else avg_change

        if rising_pct > 70 and key_value > 0.8:
            context, description = "BULL", "市场强势上涨，超70%股票上涨"
        elif rising_pct > 55 and key_value > 0.3:
            context, description = "BULL_WEAK", "市场温和上涨，多头占优"
        elif rising_pct < 30 and key_value < -0.8:
            context, description = "BEAR", "市场大幅下跌，超70%股票下跌"
        elif rising_pct < 45 and key_value < -0.3:
            context, description = "BEAR_WEAK", "市场震荡下行，空头占优"
        elif 40 <= rising_pct <= 60:
            context, description = "CONSOLIDATION", "市场横盘震荡，多空平衡"
        else:
            context, description = "MIXED", "市场分化，结构性行情"
        
        result = {
            "context": context,
            "description": description,
            "model": model,
            "avg_change": round(avg_change, 2),
            "weighted_avg": None if weighted_avg is None else round(weighted_avg, 2),
            "p25": None if p25 is None else round(p25, 2),
            "p50": None if p50 is None else round(p50, 2),
            "p75": None if p75 is None else round(p75, 2),
            "rising_pct": round(rising_pct, 1),
            "total_stocks": len(changes),
            "timestamp": now,
        }
        
        _MARKET_CONTEXT_CACHE["data"] = result
        _MARKET_CONTEXT_CACHE["ts"] = now
        return result
        
    except Exception as e:
        cached = _MARKET_CONTEXT_CACHE.get("data")
        if cached:
            out = dict(cached)
            out["error"] = str(e)
            out["context"] = out.get("context", "UNKNOWN")
            return out
        fallback = _fallback_context_from_breadth() or _fallback_context_from_indices()
        if fallback:
            fallback["error"] = str(e)
            return fallback
        return {"context": "UNKNOWN", "error": str(e)}


def get_time_pattern() -> Dict:
    """时间模式分析"""
    now = _now_bj()
    hour, minute = now.hour, now.minute
    weekday = now.weekday()
    
    patterns = {
        (9, 30, 10, 0): {"pattern": "OPENING", "desc": "开盘活跃期，波动较大"},
        (10, 0, 11, 0): {"pattern": "MORNING", "desc": "上午盘，趋势形成"},
        (11, 0, 11, 30): {"pattern": "PRE_NOON", "desc": "午盘前，观望为主"},
        (11, 30, 13, 0): {"pattern": "NOON", "desc": "午间休市"},
        (13, 0, 14, 0): {"pattern": "AFTER_NOON", "desc": "下午盘，方向选择"},
        (14, 0, 14, 30): {"pattern": "LATE", "desc": "尾盘，波动加剧"},
        (14, 30, 15, 0): {"pattern": "CLOSING", "desc": "收盘，方向确定"},
    }
    
    current_minutes = hour * 60 + minute
    period_key = "CLOSING"
    pattern_info = {"pattern": "CLOSING", "desc": "收盘确定"}
    for (sh, sm, eh, em), info in patterns.items():
        start_m = sh * 60 + sm
        end_m = eh * 60 + em
        if start_m <= current_minutes < end_m:
            period_key = f"{sh:02d}:{sm:02d}-{eh:02d}:{em:02d}"
            pattern_info = info
            break
    
    return {
        "period": period_key,
        "pattern": pattern_info["pattern"],
        "description": pattern_info["desc"],
        "is_monday": weekday == 0,
        "is_friday": weekday == 4,
        "is_month_end": now.day >= 28,
        "hour": hour,
        "minute": minute,
    }


def analyze_news_impact(news_list: List[Dict]) -> Dict:
    """新闻影响分析"""
    if not news_list:
        return {"impact": "NEUTRAL", "score": 0, "summary": "暂无新闻"}
    
    keywords_positive = ["利好", "上涨", "突破", "增长", "盈利", "增持", "回购", "业绩", "订单", "合作", "获批", "创新", "景气", "拐点"]
    keywords_negative = ["利空", "下跌", "减持", "亏损", "风险", "处罚", "诉讼", "业绩下滑", "解禁", "退市", "ST", "爆雷", "裁员"]
    
    positive_count = 0
    negative_count = 0
    
    for news in news_list:
        title = news.get("title", "") + news.get("content", "")
        for kw in keywords_positive:
            if kw in title:
                positive_count += 1
                break
        for kw in keywords_negative:
            if kw in title:
                negative_count += 1
                break
    
    net_score = positive_count - negative_count
    
    if net_score >= 2:
        impact, summary = "POSITIVE", f"利好主导(+{positive_count}/-{negative_count})"
    elif net_score <= -2:
        impact, summary = "NEGATIVE", f"利空主导(+{positive_count}/-{negative_count})"
    elif positive_count > negative_count:
        impact, summary = "POSITIVE_WEAK", "略偏利好"
    elif negative_count > positive_count:
        impact, summary = "NEGATIVE_WEAK", "略偏利空"
    else:
        impact, summary = "NEUTRAL", "中性"
    
    return {"impact": impact, "score": net_score, "summary": summary, "positive": positive_count, "negative": negative_count}


def get_sector_rotation() -> Dict:
    """板块轮动分析"""
    try:
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1, "pz": 30, "po": 1, "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fid": "f3", "fs": "m:90+t:2",
            "fields": "f2,f3,f12,f14",
        }
        response = request_with_throttle(url, timeout=10, params=params)
        response.raise_for_status()
        payload = response.json()
        
        diff = ((payload.get("data") or {}).get("diff")) or []
        sectors = []
        for row in diff:
            pct = to_float(row.get("f3"), scale=100)
            if pct is None:
                continue
            sectors.append({"name": row.get("f14"), "pct_change": pct})
        
        sectors.sort(key=lambda x: x["pct_change"], reverse=True)
        
        return {
            "leading_sectors": sectors[:5],
            "lagging_sectors": sectors[-5:],
            "rotation": "IN" if sectors and sectors[0]["pct_change"] > 3 else "OUT",
        }
    except Exception:
        return {"leading_sectors": [], "lagging_sectors": [], "rotation": "UNKNOWN"}


class TradingAnalyst:
    """交易分析师 - 模拟人类决策流程"""
    
    def __init__(self):
        self.reason_generator = TradingReason()
        self.conclusion = {}
        self.reasons = []
    
    def analyze(self, stock_code: str, factors: Dict, market_context: Dict, 
                time_pattern: Dict, news_impact: Dict = None, sector_data: Dict = None) -> Dict:
        """
        完整的人类思维分析流程：
        1. 先判断市场环境
        2. 再看时间节点
        3. 然后分析基本面
        4. 最后综合决策
        """
        self.reasons = []
        
        # Step 1: 市场环境判断（最重要）
        if market_context:
            self.reasons.extend(self.reason_generator.market_context_reason(market_context))
        
        # Step 2: 时间节点判断
        if time_pattern:
            self.reasons.extend(self.reason_generator.time_period_reason(time_pattern))
        
        # Step 3: 新闻影响
        if news_impact:
            self.reasons.extend(self.reason_generator.news_reason(news_impact))
        
        # Step 4: 板块轮动
        if sector_data:
            self.reasons.extend(self.reason_generator.sector_reason(sector_data))
        
        # Step 5: 个股因子分析
        if factors:
            self.reasons.extend(self.reason_generator.factor_reason(factors, {}))
        
        # 计算综合评分
        final_score = self._calculate_score(factors, market_context, time_pattern)
        final_signal = self._get_signal(final_score, market_context)
        
        self.conclusion = {
            "stock_code": stock_code,
            "final_score": final_score,
            "final_signal": final_signal,
            "reasons": self.reasons,
            "market_context": market_context.get("context") if market_context else "UNKNOWN",
            "time_period": time_pattern.get("period") if time_pattern else "UNKNOWN",
            "analysis_time": _now_bj().strftime("%Y-%m-%d %H:%M:%S"),
        }
        
        return self.conclusion
    
    def _calculate_score(self, factors: Dict, market_context: Dict, time_pattern: Dict) -> float:
        """计算综合评分"""
        base_score = factors.get("score", 50)
        adjustment = 0
        
        # 市场环境调整
        ctx = market_context.get("context", "UNKNOWN") if market_context else "UNKNOWN"
        if ctx == "BULL":
            adjustment += 10
        elif ctx == "BULL_WEAK":
            adjustment += 5
        elif ctx == "BEAR":
            adjustment -= 15
        elif ctx == "BEAR_WEAK":
            adjustment -= 10
        elif ctx == "CONSOLIDATION":
            adjustment += 0
        
        # 时间节点调整
        pattern = time_pattern.get("pattern", "") if time_pattern else ""
        if pattern == "OPENING":
            adjustment -= 5  # 开盘波动大
        elif pattern == "CLOSING":
            adjustment += 3  # 收盘确认
        elif pattern == "PRE_NOON":
            adjustment -= 3  # 午盘前谨慎
        
        # 周一/周五效应
        if time_pattern and (time_pattern.get("is_monday") or time_pattern.get("is_friday")):
            adjustment -= 5
        
        return max(0, min(100, base_score + adjustment))
    
    def _get_signal(self, score: float, market_context: Dict) -> str:
        """根据评分和市场环境给出信号"""
        ctx = market_context.get("context", "UNKNOWN") if market_context else "UNKNOWN"
        
        if score >= 75:
            return "STRONG_BUY"
        elif score >= 60:
            return "BUY"
        elif score >= 45:
            return "HOLD"
        elif score >= 30:
            return "SELL"
        else:
            return "STRONG_SELL"
    
    def get_summary(self) -> str:
        """生成人类可读的总结"""
        if not self.conclusion:
            return "暂无分析结论"
        
        lines = []
        lines.append(f"【{self.conclusion.get('stock_code')}】")
        lines.append(f"信号：{self.conclusion.get('final_signal')} | 评分：{self.conclusion.get('final_score'):.1f}")
        lines.append(f"市场：{self.conclusion.get('market_context')} | 时段：{self.conclusion.get('time_period')}")
        lines.append("")
        lines.append("理由：")
        for i, reason in enumerate(self.reasons[:5], 1):
            lines.append(f"  {i}. {reason}")
        
        return "\n".join(lines)


def quick_analyze(stock_code: str, quote_data: Dict = None) -> Dict:
    """快速分析 - 用于盘中扫描"""
    analyst = TradingAnalyst()
    
    market_context = get_market_context()
    time_pattern = get_time_pattern()
    
    factors = {}
    if quote_data:
        factors = {
            "score": 50,
            "factors": {
                "pct_change": quote_data.get("pct_change"),
                "turnover_rate": quote_data.get("turnover_rate"),
            }
        }
    
    return analyst.analyze(stock_code, factors, market_context, time_pattern)
