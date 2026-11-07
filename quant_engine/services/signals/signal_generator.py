from __future__ import annotations

from datetime import datetime

from loguru import logger

from ...data.repositories import MarketDataRepository
from ...models.market import MarketState
from ...models.signal import Signal, SignalAction
from ...strategies.registry import StrategyRegistry
from ..market.market_analyzer import MarketAnalyzer
from ..notifications.manager import NotificationManager
from ..risk.cooldown import CooldownManager
from ..risk.position import PositionManager
from ..risk.position_sizer import get_position_sizer
from ..risk.risk import RiskManager


class SignalGenerator:
    def __init__(self, datacenter: MarketDataRepository):
        self.datacenter = datacenter
        self.registry = StrategyRegistry(datacenter)
        self.dispatcher = self.registry.dispatcher  # 兼容旧代码直接引用
        self.market_analyzer = MarketAnalyzer(datacenter)
        self.position_manager = PositionManager()
        self.risk_manager = RiskManager()
        self.cooldown_manager = CooldownManager(datacenter.cache)
        self.notifier = NotificationManager()
        # Kelly深度融合：动态参数经 provider 注入（risk 叶族不得反向 import signals）
        self.position_sizer = get_position_sizer(
            strategy_performance_provider=self._get_strategy_performance)
        self.current_positions: dict[str, float] = {}
        self.equity_curve: list[float] = []
        self.trades: list[dict] = []
        # I-05：资金流 provider 可注入（回归脚本不再需要 /tmp wrapper 去 patch 类方法）
        self._capital_flow_provider = None

    def set_capital_flow_provider(self, provider) -> None:
        """注入资金流 provider（需实现 ``get_capital_flow(code, days)``）。

        缺省 None → 走 ``EastMoneySource``，生产行为不变。
        """
        self._capital_flow_provider = provider

    @staticmethod
    def _get_strategy_performance() -> dict:
        """策略绩效提供者（供 PositionSizer 动态 Kelly 参数，延迟导入避免环）"""
        from .signal_tracker import get_signal_tracker
        return get_signal_tracker().get_strategy_performance()

    def scan_all(self, max_signals: int = 3) -> list[Signal]:
        market = self.market_analyzer.analyze_market()
        logger.info(f"Market state: {market.type.value}, score: {market.score}")
        if market.type.value == 'BEAR':
            logger.info("Bear market, skip scanning")
            return []
        stocks = self.datacenter.get_stock_list()

        # 预排序: 用实时行情的涨幅排序，优先分析活跃股
        # 避免数据源顺序导致错过优质标的
        quotes = None
        try:
            quotes = self.datacenter.get_realtime_quote(stocks['code'].tolist())
            if not quotes.empty and 'change_pct' in quotes.columns:
                quotes['abs_change'] = quotes['change_pct'].abs()
                top_codes = quotes.nlargest(80, 'abs_change')['code'].tolist()  # 取前80只活跃股
                stock_dict = dict(zip(stocks['code'], stocks['name']))
                scan_list = [(c, stock_dict.get(c, '')) for c in top_codes if c in stock_dict]
            else:
                scan_list = list(stocks.itertuples(index=False, name=None))
        except Exception as e:  # noqa: BLE001 降级默认
            logger.warning(f"Pre-ranking failed, using default order: {e}")
            scan_list = list(stocks.itertuples(index=False, name=None))
        # D64/D65：本轮实时报价留作「信号现价」与「盘中日K 修正」的来源
        # （零额外请求——上面已取全市场）
        self._round_quotes = self._extract_quotes(quotes)

        signals = []
        for stock in scan_list:
            try:
                signal = self._analyze_stock(stock[0], stock[1], market)
                if signal is not None:
                    signals.append(signal)
                    if len(signals) >= max_signals * 2:  # 多取一些用于后续排序
                        break
            except Exception as e:  # noqa: BLE001 单项跳过
                logger.error(f"Analyze {stock[0]} error: {e}")

        # 热点板块加分
        try:
            from ..monitors.hotspot import HotspotService
            hotspot = HotspotService(self.datacenter)
            hot_codes = set(hotspot.get_hot_sector_codes())
            for s in signals:
                if s.code in hot_codes:
                    s.score *= 1.05  # 热点板块 +5%
        except Exception:  # noqa: BLE001,S110 外部调用失败静默跳过（热点加权非关键路径）
            pass

        signals.sort(key=lambda s: s.score, reverse=True)
        signals = signals[:max_signals]

        # 持久化到 SQLite（docs/25 §5.1）：修复 Redis 15 分钟 TTL 后的审计缺口
        self._persist_signals(signals, market)

        # 为 BUY 信号添加上下文（新闻/舆情）
        self._enrich_signals(signals)

        # 合并发送限流期暂存的信号摘要（D44 防钉钉 660026：
        # 钉钉 20 条/分钟硬限，扫描轮内多票连发会被暂存，此处轮末汇总发出）
        try:
            self.notifier.flush_signals()
        except Exception as e:  # noqa: BLE001 聚合失败不影响扫描结果
            logger.warning(f"Flush signals failed: {e}")
        return signals

    def scan_stock(self, code: str, name: str = "") -> Signal | None:
        market = self.market_analyzer.analyze_market()
        return self._analyze_stock(code, name, market)

    def _analyze_stock(self, code: str, name: str, market: MarketState) -> Signal | None:
        if not self.cooldown_manager.is_cooled_down(code):
            logger.debug(f"Signal cooldown active for {code}")
            return None
        df = self.datacenter.get_daily_data(code, 120)
        if df is None or df.empty:
            return None
        df['code'] = code
        df['name'] = name

        # D65：盘中日K 实时修正 + A/B 影子对比（修正会改变信号，故配影子取证）
        signal = self._dispatch_with_intraday(df, code, name, market)
        if signal is None:
            return None
        # I-06：把日K 血缘标签透出成只读证据字段（不参与任何评分/风控计算）
        self._fill_data_provenance(signal, df)
        risk_check = self.risk_manager.check_signal(signal, self.current_positions)
        if not risk_check['approved']:
            logger.info(f"Signal rejected: {risk_check['reason']}")
            return None
        
        # Kelly公式深度融合: 用PositionSizer替代简单仓位计算
        current_total = sum(self.current_positions.values())
        num_holdings = len(self.current_positions) + 1  # 包括本次
        kelly_result = self.position_sizer.calculate_position(
            signal_score=signal.score,
            market_state=market,
            volatility=self._estimate_volatility(df),
            current_positions=current_total,
            strategy_name=signal.strategy,
            num_holdings=num_holdings
        )
        
        # 应用Kelly优化后的仓位(取风控仓位和Kelly仓位的较小值)
        risk_position = risk_check['adjusted_position']
        kelly_position = kelly_result.recommended_position
        signal.position = min(risk_position, kelly_position)
        
        # 填充Kelly字段
        signal.kelly_position = kelly_result.kelly_position
        signal.kelly_confidence = kelly_result.kelly_confidence
        signal.kelly_source = kelly_result.kelly_source
        
        date_str = datetime.now().strftime("%Y%m%d")
        signal_hash = CooldownManager.generate_signal_hash(code, signal.action.value, date_str)
        if not self.cooldown_manager.is_idempotent(signal_hash):
            logger.debug(f"Idempotent check failed for {code}")
            return None
        self.cooldown_manager.set_idempotent(signal_hash)
        if signal.action == SignalAction.BUY:
            self.cooldown_manager.set_cooldown(code)
            self._fill_capital_flow(signal)
            self._fill_current_price(signal)   # D64：现价 + 日K参考价
            self.notifier.send_signal(signal)
        return signal

    def _quote_for_code(self, code: str) -> dict | None:
        """取该票的实时报价（D65：修正与现价都用它，零额外请求优先）"""
        q = (getattr(self, '_round_quotes', None) or {}).get(code)
        if q:
            return q
        try:
            df = self.datacenter.get_realtime_quote([code])
            if df is not None and not df.empty:
                row = df.to_dict('records')[0]
                if float(row.get('price') or 0) > 0:
                    return row
        except Exception as e:  # noqa: BLE001 取价失败不影响信号主流程
            logger.debug(f"[D65] quote for {code} unavailable: {e}")
        return None

    def _dispatch_with_intraday(self, df, code: str, name: str, market):
        """D65：按开关决定用「修正后」还是「修正前」的日K 出信号，并做 A/B 影子取证

        - INTRADAY_KLINE_CORRECTION=True：用实时报价修正后的日K（默认）
        - INTRADAY_KLINE_SHADOW=True：同时算两口径，把差异写入 Redis `intraday_kline:ab`
          （这是本次改动唯一的真实证据来源——历史完成日上修正是 no-op，半年窗口验不了）
        """
        from ...config import get_settings
        from ...utils.intraday_kline import apply_intraday_correction, describe

        settings = get_settings()
        want_corr = bool(settings.INTRADAY_KLINE_CORRECTION)
        want_shadow = bool(settings.INTRADAY_KLINE_SHADOW)
        if not (want_corr or want_shadow):
            return self.dispatcher.dispatch(df, market, self.trades, self.equity_curve)

        quote = self._quote_for_code(code)
        if not quote:
            return self.dispatcher.dispatch(df, market, self.trades, self.equity_curve)

        df_corr, info = apply_intraday_correction(df, quote)
        if not info.get('applied'):
            return self.dispatcher.dispatch(df, market, self.trades, self.equity_curve)

        if not want_shadow:
            logger.debug(f"[D65] {code} {describe(info)}")
            return self.dispatcher.dispatch(
                df_corr if want_corr else df, market, self.trades, self.equity_curve)

        # A/B：两口径各算一次（dispatch/engine 均无副作用，可安全双跑）
        sig_base = self.dispatcher.dispatch(df, market, self.trades, self.equity_curve)
        sig_corr = self.dispatcher.dispatch(df_corr, market, self.trades, self.equity_curve)
        self._record_intraday_ab(code, name, sig_base, sig_corr, info)
        signal = sig_corr if want_corr else sig_base
        base_desc = (f"{sig_base.action.value}/{sig_base.score:.1f}"
                     if sig_base else 'None')
        corr_desc = (f"{sig_corr.action.value}/{sig_corr.score:.1f}"
                     if sig_corr else 'None')
        logger.debug(f"[D65] {code} {describe(info)} | base={base_desc} "
                     f"corr={corr_desc}")
        return signal

    def _record_intraday_ab(self, code: str, name: str, sig_base, sig_corr,
                            info: dict) -> None:
        """把 A/B 差异写入 Redis（intraday_kline:ab，滚动保留最近 200 条）"""
        try:
            rec = {
                'ts': datetime.now().isoformat(timespec='seconds'),
                'code': code,
                'name': name,
                'mode': info.get('mode'),
                'price': info.get('price'),
                'base_close': info.get('base_close'),
                'gap_pct': info.get('gap_pct'),
                'base_action': sig_base.action.value if sig_base else None,
                'base_score': round(sig_base.score, 1) if sig_base else None,
                'corr_action': sig_corr.action.value if sig_corr else None,
                'corr_score': round(sig_corr.score, 1) if sig_corr else None,
                'changed': bool(
                    (sig_base.action.value if sig_base else None)
                    != (sig_corr.action.value if sig_corr else None)),
            }
            key = 'intraday_kline:ab'
            cache = self.datacenter.cache
            items = cache.get(key) or []
            if not isinstance(items, list):
                items = []
            items.append(rec)
            items = items[-200:]
            cache.set(key, items, ttl=604800)   # 7 天
            if rec['changed']:
                logger.info(
                    f"[D65][A/B变更] {code} {name} base="
                    f"{rec['base_action']}/{rec['base_score']} → corr="
                    f"{rec['corr_action']}/{rec['corr_score']} "
                    f"(跳空 {info.get('gap_pct')})")
        except Exception as e:  # noqa: BLE001 影子记录失败不影响信号主流程
            logger.debug(f"[D65] record A/B failed for {code}: {e}")

    @staticmethod
    def _extract_quotes(quotes) -> dict:
        """从本轮全市场实时报价提取 {code: quote_dict}（D64/D65）

        保留整行（price/open/high/low/volume/prev_close）——D65 的盘中日K 修正
        需要 open/high/low 构造当日 bar，仅有 price 不够。
        """
        if quotes is None or getattr(quotes, 'empty', True):
            return {}
        if 'code' not in quotes.columns or 'price' not in quotes.columns:
            return {}
        out = {}
        for row in quotes.to_dict('records'):
            code = str(row.get('code') or '')
            try:
                p = float(row.get('price') or 0)
            except (TypeError, ValueError):
                continue
            if code and p > 0:
                out[code] = row
        return out

    def _fill_data_provenance(self, signal: Signal, df,
                              session: str | None = None) -> None:
        """I-06/I-09：把 D66 的血缘标签透出为信号卡可读的只读字段。

        **严格只读**：这四个字段不进入评分、风控、Kelly、通知决策的任何路径，
        只用于 API/前端/日报展示「数据截至哪一天、是否复核过、整帧是否健康」。
        D64 的 current_price/price_ref 也在此一并补上透出（此前只落 Signal、未进 API）。
        """
        try:
            from ...utils.kline_provenance import evidence
            ev = evidence(df, session)
            signal.data_as_of = ev.get('data_as_of')
            signal.data_source = ev.get('data_source')
            signal.freshness = ev.get('freshness')
            signal.cross_check = ev.get('cross_check')
            signal.frame_status = ev.get('frame_status')
        except Exception as e:  # noqa: BLE001 标签缺失是历史路径常态，不影响信号
            logger.debug(f"[I-06] provenance unavailable for {signal.code}: {e}")

    def _fill_current_price(self, signal: Signal) -> None:
        """D64：填充「信号现价」与「日K参考价」

        背景（2026-11-06 实测）：日K 主源（腾讯）盘中返回**含当日未完成 bar**，
        `df['close'].iloc[-1]` 即盘中实时价；但腾讯 501（限流，实测 15+ 只/日）时降级到
        新浪，而**新浪的 240 分钟日K 盘中不含当日未完成 bar** → 最后一根是昨日 →
        信号价变成昨收（实测 600892 大晟文化：卡片显示 5.14，当日实际最高仅 4.88、
        收盘跌停 4.63）。这不是数据错，是**降级源口径与主源不一致**。

        本方法不改动信号计算（日K 口径保持不变 → 信号逻辑零改动），只把**现价**
        单独取出来供展示与审计：
          - current_price：本轮扫描已取全市场实时报价（零额外请求）
          - price_ref：日K 最后一根收盘价（信号计算所用基准）
        两者偏离 > 0.5% 即说明日K 未含当日 bar → 卡片同时给出并提示。
        """
        signal.price_ref = signal.price
        cur = 0.0
        quotes_map = getattr(self, '_round_quotes', None) or {}
        if signal.code in quotes_map:
            cur = float(quotes_map[signal.code].get('price') or 0)
        else:
            # 兜底：按票 Redis 缓存（TTL 60s，本轮扫描刚被全市场请求预热）
            try:
                quote = self.datacenter.get_cached_quote(signal.code)
                if quote:
                    cur = float(quote.get('price') or 0)
            except Exception as e:  # noqa: BLE001 取价失败不影响信号主流程
                logger.debug(f"Get current price for {signal.code} failed: {e}")
        if cur > 0:
            signal.current_price = cur

    def _fill_capital_flow(self, signal: Signal) -> None:
        """填充近5日主力资金净流入（推送前 enrichment）

        东财 fflow 日频接口，Redis 按日缓存（key 含日期）：
        同一股票当日重复扫描不重复请求；接口失败/无数据静默降级
        （字段留 None，通知模板隐藏该行）。
        成功缓存 2 天；失败负缓存仅 10 分钟——东财 push2 端点偶发
        IP 级限流（RemoteDisconnected），短 TTL 保证限流恢复后可自愈，
        同时避免连续扫描反复冲击。
        """
        try:
            provider = self._capital_flow_provider
            if provider is None:
                from ...data.sources.eastmoney import EastMoneySource
                provider = EastMoneySource
            today = datetime.now().strftime('%Y%m%d')
            cache_key = f'capflow:{signal.code}:{today}'
            cached = self.datacenter.cache.get(cache_key)
            if cached is not None:
                signal.capital_flow_5d = cached if cached != '' else None
                return
            flow = provider.get_capital_flow(signal.code, days=5)
            signal.capital_flow_5d = flow
            # 成功 2 天 / 失败 10 分钟负缓存（空串哨兵）
            self.datacenter.cache.set(
                cache_key, '' if flow is None else flow,
                ttl=600 if flow is None else 2 * 86400)
        except Exception as e:  # noqa: BLE001 外部调用，降级不影响主流程
            logger.warning(f"Capital flow fill failed for {signal.code}: {e}")
    
    def _persist_signals(self, signals: list[Signal], market: MarketState) -> None:
        """信号归档到 SQLite（docs/25 §5.1）

        只落 BUY/WATCH（可执行信号；SKIP 是噪声）。同日同代码同策略
        upsert——盘中多轮扫描只保留最新一次观测。只归档 scan_all 系统性
        轮次（worker/API 冷扫都走本路径），不含 /api/signals/{code} 按需查询。
        持久层故障静默降级，绝不影响推送主流程。
        """
        try:
            from ...data.store import get_history_store
            actionable = [s for s in signals if s.action != SignalAction.SKIP]
            if not actionable:
                return
            rows = [{
                'signal_date': s.timestamp.strftime('%Y-%m-%d'),
                'created_at': s.timestamp.isoformat(),
                'code': s.code,
                'name': s.name,
                'action': s.action.value,
                'level': s.level.value if s.level else None,
                'score': s.score,
                'strategy': s.strategy,
                'price': s.price,
                'position': s.position,
                'target_price': s.target_price,
                'stop_loss': s.stop_loss,
                'capital_flow_5d': s.capital_flow_5d,
                'kelly_position': s.kelly_position,
                'kelly_confidence': s.kelly_confidence,
                'kelly_source': s.kelly_source,
                'market_type': market.type.value,
                'reason': s.reason,
            } for s in actionable]
            get_history_store().save_signals(rows)
        except Exception as e:  # noqa: BLE001 持久层降级不影响主流程
            logger.warning(f"Persist signals failed: {e}")

    def _estimate_volatility(self, df) -> float:
        """估算年化波动率"""
        try:
            if len(df) < 20:
                return 0.25  # 默认25%
            returns = df['close'].pct_change().dropna()
            daily_vol = returns.std()
            # 年化: * sqrt(252)
            annual_vol = daily_vol * (252 ** 0.5)
            return max(0.1, min(1.0, annual_vol))
        except (KeyError, TypeError, ValueError):
            return 0.25

    def update_after_trade(self, code: str, pnl: float):
        self.position_manager.update_capital(pnl)
        if code in self.current_positions:
            del self.current_positions[code]
        self.trades.append({'code': code, 'pnl': pnl})
        capital_info = self.position_manager.get_capital_info()
        self.equity_curve.append(capital_info['current'])

    def _enrich_signals(self, signals: list[Signal]):
        """为 BUY 信号添加候选股上下文（新闻/舆情）"""
        buy_signals = [s for s in signals if s.action == SignalAction.BUY]
        if not buy_signals:
            return
        try:
            from .candidate_context import CandidateContext
            ctx = CandidateContext()
            candidates = [
                {'code': s.code, 'name': s.name, 'score': s.score}
                for s in buy_signals
            ]
            enriched = ctx.enrich_candidates(candidates, max_candidates=3)
            # 将上下文写回信号
            enriched_map = {c['code']: c.get('context', {}) for c in enriched}
            for s in buy_signals:
                context = enriched_map.get(s.code)
                if context:
                    s.context = context
        except Exception as e:  # noqa: BLE001 外部调用
            logger.warning(f"Signal context enrichment failed: {e}")


# ---- API 信号缓存（worker 预热 + api/routers/signals.py 同一键读取）----
API_SIGNALS_TTL = 900  # 15 分钟：信号非高频数据，避免缓存未命中触发分钟级全量扫描


def api_signals_cache_key(max_signals: int = 3) -> str:
    """API 信号缓存键（routers/signals.py 与本函数必须一致）"""
    return f'api_cache:signals:{max_signals}'


def signal_to_api_dict(s: Signal) -> dict:
    """Signal → API 响应 dict（字段与 api/schemas SignalResponse 对齐）"""
    return {
        'code': s.code,
        'name': s.name,
        'action': s.action.value,
        'level': s.level.value if s.level else None,
        'score': s.score,
        'position': s.position,
        'reason': s.reason,
        'strategy': s.strategy,
        'price': s.price,
        'target_price': s.target_price,
        'stop_loss': s.stop_loss,
        'kelly_position': s.kelly_position,
        'kelly_confidence': s.kelly_confidence,
        'kelly_source': s.kelly_source,
        'capital_flow_5d': s.capital_flow_5d,
        # D64 现价/参考价（此前只落 Signal 未透出，本次随 I-06 一并补齐）
        'current_price': s.current_price,
        'price_ref': s.price_ref,
        # I-06 只读血缘证据（不参与评分）
        'data_as_of': s.data_as_of,
        'data_source': s.data_source,
        'freshness': s.freshness,
        'cross_check': s.cross_check,
        'frame_status': s.frame_status,
    }


def warm_api_signals_cache(cache, signals: list[Signal],
                           max_signals: int = 3) -> None:
    """worker 扫描后预热 API 信号缓存（键/TTL 与 routers/signals.py 一致）"""
    try:
        cache.set(api_signals_cache_key(max_signals),
                  [signal_to_api_dict(s) for s in signals],
                  ttl=API_SIGNALS_TTL)
    except Exception as e:  # noqa: BLE001 预热失败不影响主流程
        logger.warning(f"Warm api signals cache failed: {e}")
