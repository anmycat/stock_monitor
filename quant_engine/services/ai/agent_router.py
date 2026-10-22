"""Agent策略问股 - 意图识别 + 技能路由（借鉴 DSA Agent）"""
import re
import time
from typing import Dict, Tuple
from loguru import logger


# Session 管理
_sessions: Dict[str, dict] = {}
_SESSION_TTL = 1800  # 30分钟
_MAX_TURNS = 20


def get_session(session_id: str) -> dict:
    """获取或创建对话session"""
    now = time.time()
    # 清理过期session
    expired = [sid for sid, s in _sessions.items() if now - s['last_active'] > _SESSION_TTL]
    for sid in expired:
        del _sessions[sid]

    if session_id not in _sessions:
        _sessions[session_id] = {
            'history': [],
            'last_active': now,
        }
    session = _sessions[session_id]
    session['last_active'] = now
    # 限制对话轮数
    if len(session['history']) > _MAX_TURNS * 2:
        session['history'] = session['history'][-_MAX_TURNS * 2:]
    return session


def detect_intent(message: str) -> Tuple[str, Dict]:
    """
    简单规则意图识别
    
    Returns:
        (intent, params) 元组
    """
    msg = message.strip().lower()

    # 分析个股
    stock_pattern = r'(?:分析|看看|查一下|查询|评估)(.{2,10})'
    match = re.search(stock_pattern, msg)
    if match:
        query = match.group(1).strip()
        return 'analyze_stock', {'query': query}

    # 代码直查
    code_pattern = r'^(\d{6})(?:\.\w{2})?$'
    match = re.match(code_pattern, msg.strip())
    if match:
        return 'analyze_stock', {'query': match.group(1)}

    # 热点查询
    if any(kw in msg for kw in ['热点', '热门板块', '涨幅榜', '哪些板块']):
        return 'query_hotspots', {}

    # 策略推荐
    if any(kw in msg for kw in ['策略', '推荐', '有哪些策略', '策略列表']):
        return 'list_strategies', {}

    # 市场概览
    if any(kw in msg for kw in ['大盘', '市场', '行情', '今日']):
        return 'market_overview', {}

    # 信号查询
    if any(kw in msg for kw in ['信号', '买入', '扫描', '选股']):
        return 'scan_signals', {}

    # 评估/后验
    if any(kw in msg for kw in ['评估', '回测', '后验', '胜率']):
        return 'evaluate', {}

    # 兜底: LLM自由对话
    return 'free_chat', {'message': message}


async def route_message(message: str, session_id: str = "default", datacenter=None,
                        signal_generator=None) -> Dict:
    """
    路由消息到对应的处理技能

    Args:
        signal_generator: 信号扫描能力注入（P4 参数注入，ai 族不再 import signals 族，
            prd/15 §3.2 P4 清单 #5）；None 时信号类技能降级
    Returns:
        格式化的响应字典
    """
    intent, params = detect_intent(message)
    session = get_session(session_id)

    try:
        if intent == 'analyze_stock':
            return await _handle_analyze_stock(params['query'], datacenter, signal_generator)
        elif intent == 'query_hotspots':
            return await _handle_hotspots(datacenter)
        elif intent == 'list_strategies':
            return await _handle_list_strategies(datacenter)
        elif intent == 'market_overview':
            return await _handle_market_overview(datacenter)
        elif intent == 'scan_signals':
            return await _handle_scan_signals(datacenter, signal_generator)
        elif intent == 'evaluate':
            return await _handle_evaluate(datacenter)
        elif intent == 'free_chat':
            return await _handle_free_chat(params['message'], session, datacenter)
        else:
            return {'type': 'chat', 'text': f'未识别意图: {message}'}
    except Exception as e:
        logger.error(f"Agent route error: {e}")
        return {'type': 'chat', 'text': f'处理出错: {str(e)}'}


async def _handle_analyze_stock(query: str, datacenter, signal_generator=None) -> Dict:
    """处理个股分析"""
    from ...utils.stock_resolver import resolver
    # 尝试解析股票代码
    results = resolver.resolve(query, 1)
    if not results and datacenter:
        # 尝试从 stock list 查找
        stocks = datacenter.get_stock_list()
        if not stocks.empty:
            resolver.update_cache(stocks.to_dict('records'))
            results = resolver.resolve(query, 1)

    if not results:
        return {'type': 'chat', 'text': f'未找到股票: {query}'}

    code = results[0]['code']
    name = results[0].get('name', query)

    # P4 注入：signal_generator 缺失时跳过信号段（原为函数内 import signals 族）
    signal = None
    if signal_generator is not None:
        signal = signal_generator.scan_stock(code, name)

    from .ai_summary import AISummary
    from ...utils.indicators import calculate_indicators
    df = datacenter.get_daily_data(code, 120)
    indicators = calculate_indicators(df) if df is not None and not df.empty else {}
    ai = AISummary()
    analysis = ai.generate_stock_analysis(code, name, df, indicators)

    return {
        'type': 'stock_dashboard',
        'data': {
            'code': code,
            'name': name,
            'analysis': analysis,
            'signal': {
                'action': signal.action.value,
                'score': signal.score,
                'reason': signal.reason,
            } if signal else None,
        }
    }


async def _handle_hotspots(datacenter) -> Dict:
    """处理热点查询"""
    from ..monitors.hotspot import HotspotService
    hs = HotspotService(datacenter)
    hotspots = hs.get_hotspots(5)
    lines = ['🔥 今日热点板块：']
    for i, h in enumerate(hotspots, 1):
        leaders = ', '.join(s.get('name', '') for s in h.get('leader_stocks', [])[:2])
        lines.append(f"{i}. {h['name']} (热度{h['score']:.0f}) | 涨幅{h['avg_change']:.1f}% | 龙头: {leaders}")
    return {'type': 'chat', 'text': '\n'.join(lines)}


async def _handle_list_strategies(datacenter) -> Dict:
    """处理策略列表"""
    from ...strategies.registry import StrategyRegistry
    reg = StrategyRegistry(datacenter)
    strategies = reg.get_available_strategies()
    lines = ['📋 可用策略列表：']
    for s in strategies:
        lines.append(f"- {s['name']} ({s['type']})")
    return {'type': 'chat', 'text': '\n'.join(lines)}


async def _handle_market_overview(datacenter) -> Dict:
    """处理市场概览"""
    from ..market.market_analyzer import MarketAnalyzer
    analyzer = MarketAnalyzer(datacenter)
    market = analyzer.analyze_market()
    type_desc = {'BULL': '牛市', 'BULL_WEAK': '弱牛', 'SIDEWAYS': '震荡', 'BEAR': '熊市'}
    text = (
        f"📊 市场状态：{type_desc.get(market.type.value, '未知')}\n"
        f"评分：{market.score:.0f} | 趋势：{market.trend:.2f}\n"
        f"涨跌比：{market.breadth:.0%}"
    )
    return {'type': 'chat', 'text': text}


async def _handle_scan_signals(datacenter, signal_generator=None) -> Dict:
    """处理信号扫描"""
    if signal_generator is None:
        return {'type': 'chat', 'text': '信号服务暂不可用'}
    signals = signal_generator.scan_all(max_signals=3)
    if not signals:
        return {'type': 'chat', 'text': '📭 今日暂无交易信号'}
    lines = ['📈 当前信号：']
    for s in signals:
        lines.append(f"- {s.name}({s.code}) {s.action.value} 评分{s.score:.0f}")
    return {'type': 'chat', 'text': '\n'.join(lines)}


async def _handle_evaluate(datacenter) -> Dict:
    """处理后验评估"""
    from ...backtest.post_evaluation import PostEvaluation
    pe = PostEvaluation(datacenter.cache, datacenter)
    batch = pe.evaluate_batch(limit=5)
    if 'error' in batch:
        return {'type': 'chat', 'text': f'评估出错: {batch["error"]}'}
    if 'message' in batch:
        return {'type': 'chat', 'text': batch['message']}
    text = (
        f"📊 后验评估（最近{batch.get('total_runs', 0)}次运行）：\n"
        f"平均胜率：{batch.get('avg_win_rate', 0):.0%}\n"
        f"平均收益：{batch.get('avg_pnl', 0):.2%}\n"
        f"总信号数：{batch.get('total_signals', 0)}"
    )
    return {'type': 'chat', 'text': text}


async def _handle_free_chat(message: str, session: dict, datacenter) -> Dict:
    """处理自由对话（LLM）"""
    from .ai_summary import AISummary
    ai = AISummary()
    if not ai.models:
        return {'type': 'chat', 'text': '当前未配置AI模型，无法进行自由对话。'}

    cfg = list(ai.models.values())[0]
    system = "你是量化交易助手，擅长A股分析。可调用以下技能：分析个股、查询热点、策略推荐、市场概览、信号扫描、后验评估。"

    # 构建上下文
    history_text = ""
    for msg in session['history'][-4:]:
        role = "用户" if msg.get('role') == 'user' else "助手"
        history_text += f"{role}: {msg['content']}\n"

    user_prompt = f"{history_text}\n用户: {message}" if history_text else message
    resp = ai._call_single_model(cfg, system, user_prompt, max_tokens=600)

    if resp.success:
        session['history'].append({'role': 'user', 'content': message})
        session['history'].append({'role': 'assistant', 'content': resp.text})
        return {'type': 'chat', 'text': resp.text}

    return {'type': 'chat', 'text': 'AI响应失败，请稍后重试。'}
