import time
from datetime import datetime, timedelta

from loguru import logger

from .backtest.post_evaluation import PostEvaluation
from .data.repositories import MarketDataRepository
from .services.ai.ai_summary import AISummary
from .services.market.market_alert import MarketAlert
from .services.market.market_analyzer import MarketAnalyzer
from .services.monitors.burst_monitor import BurstMonitor
from .services.notifications.manager import NotificationManager
from .services.signals.signal_generator import SignalGenerator, warm_api_signals_cache
from .utils.calendar import is_trading_day


def get_current_phase() -> str:
    now = datetime.now()
    
    if not is_trading_day(now):
        return "WEEKEND"
    
    h, m = now.hour, now.minute
    t = h * 60 + m
    
    if t < 9 * 60 + 15:
        return "PRE_MARKET"
    elif 9 * 60 + 15 <= t < 9 * 60 + 25:
        return "SELF_CHECK"
    elif 9 * 60 + 25 <= t < 9 * 60 + 30:
        return "CALL_AUCTION"
    elif 9 * 60 + 30 <= t < 11 * 60 + 30:
        return "MORNING_SESSION"
    elif 11 * 60 + 30 <= t < 12 * 60 + 10:
        return "NOON_BREAK"
    elif 12 * 60 + 10 <= t < 12 * 60 + 15:
        return "MORNING_REVIEW"  # 12:10-12:15 早盘总结（避开DS涨价）
    elif 12 * 60 + 15 <= t < 13 * 60:
        return "NOON_BREAK"
    elif 13 * 60 <= t < 15 * 60:
        return "AFTERNOON_SESSION"
    elif 15 * 60 <= t < 18 * 60 + 10:
        return "AFTER_HOURS"
    elif 18 * 60 + 10 <= t < 18 * 60 + 15:
        return "DAILY_REVIEW"  # 18:10-18:15 全日总结（避开DS涨价）
    else:
        return "AFTER_HOURS"


def get_sleep_seconds(phase: str) -> int:
    now = datetime.now()
    h, m = now.hour, now.minute
    t = h * 60 + m
    schedule = {
        "PRE_MARKET": 9 * 60 + 15,
        "SELF_CHECK": 9 * 60 + 25,
        "CALL_AUCTION": 9 * 60 + 30,
        "MORNING_SESSION": None,
        "MORNING_REVIEW": None,
        "NOON_BREAK": 13 * 60,
        "AFTERNOON_SESSION": None,
        "DAILY_REVIEW": None,
        "AFTER_HOURS": 9 * 60 + 15,
    }
    target = schedule.get(phase)
    if target is None:
        return 300
    if phase == "AFTER_HOURS":
        tomorrow = now.replace(hour=9, minute=15, second=0) + timedelta(days=1)
        return int((tomorrow - now).total_seconds())
    return (target - t) * 60


def push_summary_to_web(summary_type: str, title: str, summary_data: dict):
    """通过WebSocket推送AI总结到Web页面"""
    try:
        import asyncio
        import json

        from .api.websocket_enhanced import manager
        
        ws_data = {
            'type': 'ai_summary',
            'data': {
                'summary_type': summary_type,
                'title': title,
                'content': summary_data.get('full_text', ''),
                'timestamp': datetime.now().isoformat()
            }
        }
        asyncio.run(manager.broadcast(json.dumps(ws_data, default=str)))
        logger.info(f"✅ {title}已推送到Web")
    except Exception as e:  # noqa: BLE001 任务兜底
        logger.error(f"推送{title}到Web失败: {e}")


def get_sector_data(datacenter):
    """获取板块数据（统一入口，使用单例）"""
    try:
        from .services.monitors.sector_monitor import SectorMonitor
        monitor = SectorMonitor(datacenter)
        return monitor.get_sector_data()
    except Exception as e:  # noqa: BLE001 任务兜底
        logger.warning(f"获取板块数据失败: {e}")
        return []


def check_etf_holdings(datacenter, notifier):
    """盘后ETF持仓变化检查（每日一次，含周末；季报披露日不固定）

    抓取热门ETF季度持仓，与已存快照比对，发现新季度且变化过阈值时推送。
    Redis键 etf_holdings:check_date 保证每日只跑一次。
    """
    try:
        from .services.monitors.etf_holdings_monitor import ETFHoldingsMonitor
        today = datetime.now().strftime('%Y-%m-%d')
        if datacenter.cache.get('etf_holdings:check_date') == today:
            return
        monitor = ETFHoldingsMonitor(datacenter)
        push_list = monitor.check_all()
        datacenter.cache.set('etf_holdings:check_date', today, ttl=2 * 86400)
        for changes in push_list:
            etf_code = changes['etf_code'].split('.')[0]
            title = f"📦 ETF持仓变化: {changes['etf_name']}({etf_code})"
            msg = f"{title}\n\n{monitor.format_changes_message(changes)}"
            notifier.send_custom_message(title, msg)
            logger.info(
                f"ETF holdings alert pushed: {etf_code} "
                f"{changes['previous_quarter']}→{changes['current_quarter']} "
                f"x{changes['alert_count']}"
            )
        if not push_list:
            logger.info("ETF holdings check done, no alertable changes")
    except Exception as e:  # noqa: BLE001 任务兜底
        logger.error(f"ETF holdings check error: {e}")


def check_code_guard_static(datacenter, notifier):
    """盘前代码一致性静态校验（交易日 9:00-9:15，每日一次）

    对全部持久化监控目标（热门ETF/ETF持仓成分股/stock_list缓存）做
    代码-市场-名称四重校验，发现错码自动修复脏缓存并推送告警。
    详见 services/code_guard.py。
    """
    try:
        from .services.code_guard import CodeGuard
        now = datetime.now()
        if not is_trading_day(now):
            return
        t = now.hour * 60 + now.minute
        # 盘前窗口 9:00-9:15（PRE_MARKET），避免凌晨/盘中误跑
        if not (9 * 60 <= t < 9 * 60 + 15):
            return
        today = now.strftime('%Y-%m-%d')
        if datacenter.cache.get('code_guard:check_date_static') == today:
            return
        guard = CodeGuard(datacenter)
        guard.check_and_push(notifier, mode='static')
        datacenter.cache.set('code_guard:check_date_static', today, ttl=2 * 86400)
    except Exception as e:  # noqa: BLE001 任务兜底
        logger.error(f"Code guard static check error: {e}")


def check_code_guard_auction(datacenter, notifier):
    """集合竞价代码一致性校验（交易日 9:15-9:25，每日一次）

    利用竞价期间的虚拟匹配价，与 quote:{code} 缓存中的昨收做连续性
    校验，发现缓存串号（价格跳空超阈值）即清理并告警。
    """
    try:
        from .services.code_guard import CodeGuard
        now = datetime.now()
        if not is_trading_day(now):
            return
        t = now.hour * 60 + now.minute
        # 集合竞价窗口 9:15-9:25（SELF_CHECK）
        if not (9 * 60 + 15 <= t < 9 * 60 + 25):
            return
        today = now.strftime('%Y-%m-%d')
        if datacenter.cache.get('code_guard:check_date_auction') == today:
            return
        guard = CodeGuard(datacenter)
        guard.check_and_push(notifier, mode='auction')
        datacenter.cache.set('code_guard:check_date_auction', today, ttl=2 * 86400)
    except Exception as e:  # noqa: BLE001 任务兜底
        logger.error(f"Code guard auction check error: {e}")


def check_morning_news(datacenter, notifier):
    """每日9:15早间财经要闻推送（按板块、去重、多渠道；含周末）

    执行窗口 9:14-9:35（覆盖交易日前与非交易日），Redis键
    news:check_date 保证每日只推一次。抓取自上次推送以来的多源新闻，
    去重后按板块分组推送；无新增不推送。
    """
    try:
        from .services.monitors.news_monitor import MorningNewsMonitor
        now = datetime.now()
        t = now.hour * 60 + now.minute
        if not (9 * 60 + 14 <= t <= 9 * 60 + 35):
            return
        today = now.strftime('%Y-%m-%d')
        if datacenter.cache.get('news:check_date') == today:
            return
        monitor = MorningNewsMonitor(datacenter)
        digest = monitor.check_and_push(notifier)
        datacenter.cache.set('news:check_date', today, ttl=2 * 86400)
        if digest:
            logger.info(
                f"Morning news pushed: {digest['total']} items "
                f"across {digest['sector_count']} sectors "
                f"(dropped {digest['dedup_dropped']})"
            )
        else:
            logger.info("Morning news check done, no new items")
    except Exception as e:  # noqa: BLE001 任务兜底
        logger.error(f"Morning news check error: {e}")


def save_summary_to_cache(datacenter, summary_type: str, summary_data: dict,
                          signals_count: int | None = None,
                          sectors_count: int | None = None):
    """保存总结到Redis缓存，供 Web页面显示；同时归档 SQLite（docs/25 §5.1）"""
    cache_key = f"ai_summary:{summary_type}"
    datacenter.cache.set(cache_key, summary_data, ttl=86400)  # 缓存24小时
    try:
        from .data.store import get_history_store
        get_history_store().save_summary(
            summary_type, summary_data,
            signals_count=signals_count, sectors_count=sectors_count)
    except Exception as e:  # noqa: BLE001 归档降级不影响缓存主流程
        logger.error(f"Archive summary failed: {e}")


def _save_post_evaluation(post_eval, signals, market):
    """保存信号到后验评估系统"""
    try:
        if not signals:
            return
        run_id = f"run_{datetime.now().strftime('%Y%m%d')}"
        signals_data = [
            {
                'code': s.code, 'name': s.name, 'action': s.action.value,
                'price': s.price, 'score': s.score,
                'timestamp': s.timestamp.isoformat(),
            }
            for s in signals
        ]
        market_state = {'type': market.type.value, 'score': market.score}
        post_eval.save_run(run_id, signals_data, market_state)
    except Exception as e:  # noqa: BLE001 任务兜底
        logger.error(f"Save post evaluation error: {e}")


def main():
    logger.info("Worker started")
    datacenter = MarketDataRepository()
    generator = SignalGenerator(datacenter)
    analyzer = MarketAnalyzer(datacenter)
    alert_detector = MarketAlert(
        datacenter, sector_data_provider=lambda: get_sector_data(datacenter))
    ai_summary = AISummary()
    burst_monitor = BurstMonitor(datacenter)
    notifier = NotificationManager()
    post_eval = PostEvaluation(datacenter.cache, datacenter)
    last_phase = None
    active_pool = []

    while True:
        try:
            phase = get_current_phase()
            if phase != last_phase:
                logger.info(f"Phase: {phase}")
                last_phase = phase

            if phase == "SELF_CHECK":
                logger.info("System self-check...")
                active_pool = burst_monitor.refresh_active_pool()
                logger.info(f"Active pool: {len(active_pool)} stocks")
                # 集合竞价（9:15-9:25）顺便校验代码缓存一致性：
                # 用竞价匹配价与 quote 缓存做连续性校验，拦截缓存串号
                check_code_guard_auction(datacenter, notifier)
                # 保险：若 PRE_MARKET 错过 9:14 窗口，此处补跑
                # （check_date 幂等去重，不会重复推送）
                check_morning_news(datacenter, notifier)
                check_etf_holdings(datacenter, notifier)
                # 睡到竞价结束（9:25）进入 CALL_AUCTION；不用硬编码 600s——
                # worker 晚启动/重启时不会睡过头错过集合竞价播报
                time.sleep(max(get_sleep_seconds("SELF_CHECK"), 5))

            elif phase == "CALL_AUCTION":
                market = analyzer.analyze_market()
                sectors = get_sector_data(datacenter)
                summary = ai_summary.generate_summary(market, [], sectors)
                msg = f"【集合竞价播报】\n\n{summary['full_text']}"
                notifier.send_custom_message("集合竞价播报", msg)
                save_summary_to_cache(datacenter, "call_auction", summary)
                logger.info("Call auction summary generated and pushed")
                # 睡到 9:30 开盘；不用硬编码 300s——晚启动时同样不睡过头
                time.sleep(max(get_sleep_seconds("CALL_AUCTION"), 5))

            elif phase in ["MORNING_SESSION", "AFTERNOON_SESSION"]:
                # 起爆资金监控
                bursts = burst_monitor.detect_burst()
                for burst in bursts:
                    msg = f"🚀 强势异动: {burst['name']} ({burst['code']})\n\n{burst_monitor.format_burst_message(burst)}"
                    notifier.send_custom_message(f"强势异动: {burst['name']}", msg)
                    logger.info(f"Burst alert: {burst['name']} {burst['change_pct']}%")

                # 大盘异动检测
                market = analyzer.analyze_market()
                alerts = alert_detector.detect_alerts(market)
                for alert in alerts:
                    notifier.send_custom_message(alert['title'], alert['detail'])
                    logger.info(f"Alert: {alert['title']}")

                # 信号扫描
                signals = generator.scan_all(max_signals=3)
                if signals:
                    logger.info(f"Found {len(signals)} signals")
                    for s in signals:
                        if s.action.value == 'BUY':
                            notifier.send_signal(s)
                warm_api_signals_cache(datacenter.cache, signals)

                time.sleep(300)

            elif phase == "MORNING_REVIEW":
                # 12:15 推送【早盘总结】（避开DS 12:00-13:00涨价时段）
                market = analyzer.analyze_market()
                signals = generator.scan_all(max_signals=3)
                warm_api_signals_cache(datacenter.cache, signals)
                sectors = get_sector_data(datacenter)
                summary = ai_summary.generate_summary(
                    market, signals, sectors,
                    summary_type="morning_review"
                )
                msg = f"【早盘总结】\n\n{summary['full_text']}"
                notifier.send_custom_message("早盘总结", msg)
                save_summary_to_cache(datacenter, "morning_review", summary,
                                      signals_count=len(signals),
                                      sectors_count=len(sectors))
                push_summary_to_web("morning_review", "早盘总结", summary)
                logger.info("✅ 早盘总结已推送 (12:15)")
                time.sleep(1800)  # 30分钟后进入午休

            elif phase == "DAILY_REVIEW":
                # 18:15 推送【全日总结】（避开DS 18:00-19:00涨价时段）
                market = analyzer.analyze_market()
                signals = generator.scan_all(max_signals=3)
                warm_api_signals_cache(datacenter.cache, signals)
                alerts_history = alert_detector.get_history(10)
                alert_summary = f"今日异动{len(alerts_history)}次" if alerts_history else "今日无异动"
                sectors = get_sector_data(datacenter)
                summary = ai_summary.generate_summary(
                    market, signals, sectors,
                    summary_type="daily_review",
                    alerts=alerts_history
                )
                summary['full_text'] = f"{summary['full_text']}\n\n{alert_summary}"
                msg = f"【全日总结】\n\n{summary['full_text']}"
                notifier.send_custom_message("全日总结", msg)
                save_summary_to_cache(datacenter, "daily_review", summary,
                                      signals_count=len(signals),
                                      sectors_count=len(sectors))
                push_summary_to_web("daily_review", "全日总结", summary)

                # 保存运行结果供 T+N 后验评估
                _save_post_evaluation(post_eval, signals, market)

                logger.info("✅ 全日总结已推送 (18:15)")
                time.sleep(3600)  # 1小时后进入盘后

            else:
                # 盘前（交易日9:00-9:15）代码一致性静态校验，拦截错码/改名/退市
                check_code_guard_static(datacenter, notifier)
                # 盘后/非交易时段：每日一次ETF持仓变化检查（含周末，季报披露日不固定）
                check_etf_holdings(datacenter, notifier)
                # 每日9:15早间财经要闻（按板块，含周末；窗口9:14-9:35，日期去重）
                check_morning_news(datacenter, notifier)
                sleep_sec = get_sleep_seconds(phase)
                if sleep_sec > 60:
                    logger.info(f"Phase {phase}, sleeping {sleep_sec}s")
                time.sleep(min(sleep_sec, 60))

        except Exception as e:  # noqa: BLE001 任务兜底
            logger.error(f"Worker error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
