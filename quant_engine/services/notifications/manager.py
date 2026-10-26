"""通知管理器 - 统一管理所有通知渠道"""

import threading
import time

from loguru import logger

from ...config import get_settings
from ...utils.helpers import format_number
from .base import BaseNotifier
from .feishu import FeishuNotifier
from .telegram import TelegramNotifier
from .wechat import WeChatNotifier


class DingTalkNotifier(BaseNotifier):
    """钉钉通知器（兼容旧代码）"""

    def __init__(self, webhook_url: str, secret: str = ""):
        super().__init__("DingTalk")
        self.webhook_url = webhook_url
        self.secret = secret
        self.enabled = bool(webhook_url)

    def send(self, title: str, content: str) -> bool:
        if not self.enabled:
            return False
        try:
            import base64
            import hashlib
            import hmac
            import time
            import urllib.parse

            url = self.webhook_url
            if self.secret:
                timestamp = str(round(time.time() * 1000))
                string_to_sign = f"{timestamp}\n{self.secret}"
                hmac_code = hmac.new(
                    self.secret.encode('utf-8'),
                    string_to_sign.encode('utf-8'),
                    digestmod=hashlib.sha256
                ).digest()
                sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
                url = f"{self.webhook_url}&timestamp={timestamp}&sign={sign}"

            import httpx
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "title": title,
                    "text": f"## {title}\n\n{content}",
                },
            }
            with httpx.Client(timeout=10) as client:
                response = client.post(url, json=payload)
                if response.status_code == 200:
                    result = response.json()
                    if result.get('errcode') == 0:
                        self._log_success()
                        return True
                    else:
                        self._log_error(str(result))
                        return False
        except Exception as e:
            self._log_error(str(e))
        return False


class NotificationManager:
    """统一通知管理器

    渠道级流量控制（2026-10-27 D44，修复钉钉 660026 限流）：
    钉钉自定义机器人官方规则为"每机器人每分钟最多 20 条，超过限流 10 分钟"，
    官方建议大量发消息场景"整合为 markdown 摘要发送"。burst 异动逐票连发
    （10-26 日志 09:36 一轮 7+ 只）即触发该限流。本类实现：
      - 最小发送间隔（NOTIFY_MIN_INTERVAL_SECONDS，默认 3s）+ 每分钟上限
        （NOTIFY_MAX_PER_MINUTE，默认 18，留 2 条余量给市场告警）
      - 超限消息不丢弃：信号进 pending_signals 聚合缓冲，自定义消息进
        pending_texts，由 flush_signals() 在扫描轮/异动轮结束后合并为
        一条摘要发出（flush 时若窗口仍满则保留到下一轮，最多延迟一轮）
      - send_market_alert 为高频监控类立即发送（低频，不参与聚合）
    """

    def __init__(self):
        settings = get_settings()
        self.notifiers: list[BaseNotifier] = []

        # 钉钉
        if settings.DINGTALK_WEBHOOK:
            self.notifiers.append(
                DingTalkNotifier(settings.DINGTALK_WEBHOOK, settings.DINGTALK_SECRET)
            )

        # 企业微信
        if settings.WECHAT_WEBHOOK_URL:
            self.notifiers.append(WeChatNotifier(settings.WECHAT_WEBHOOK_URL))

        # 飞书
        if settings.FEISHU_WEBHOOK_URL:
            self.notifiers.append(FeishuNotifier(settings.FEISHU_WEBHOOK_URL))

        # Telegram
        if settings.TELEGRAM_BOT_TOKEN and settings.TELEGRAM_CHAT_ID:
            self.notifiers.append(
                TelegramNotifier(settings.TELEGRAM_BOT_TOKEN, settings.TELEGRAM_CHAT_ID)
            )

        # ── 流量控制状态（以最严格渠道钉钉 20 条/分钟为准统一节流）──
        self._min_interval = getattr(settings, 'NOTIFY_MIN_INTERVAL_SECONDS', 3.0)
        self._max_per_minute = getattr(settings, 'NOTIFY_MAX_PER_MINUTE', 18)
        self._window_start = time.monotonic()
        self._window_count = 0
        self._last_sent = 0.0
        self._lock = threading.Lock()
        # 聚合缓冲（限流期暂存，flush_signals 合并发出）
        self._pending_signals: list = []
        self._pending_texts: list[tuple[str, str]] = []

        logger.info(f"NotificationManager initialized with {len(self.notifiers)} notifiers")

    # ── 流量控制 ──
    def _can_send_now(self) -> bool:
        """是否可立即发送（调用方持锁）"""
        now = time.monotonic()
        if now - self._window_start >= 60:
            self._window_start = now
            self._window_count = 0
        return self._window_count < self._max_per_minute

    def _mark_sent(self) -> None:
        """记录一次发送（调用方持锁）"""
        now = time.monotonic()
        if now - self._window_start >= 60:
            self._window_start = now
            self._window_count = 0
        self._window_count += 1

    def _last_sent_at(self) -> float:
        return getattr(self, '_last_sent', 0.0)

    def send(self, title: str, content: str) -> bool:
        """发送到所有已配置渠道，任一成功返回 True"""
        success = False
        for notifier in self.notifiers:
            try:
                if notifier.send(title, content):
                    success = True
            except Exception as e:
                logger.error(f"Send to {notifier.name} failed: {e}")
        if success:
            with self._lock:
                self._last_sent = time.monotonic()
        return success

    def send_custom_message(self, title: str, content: str) -> bool:
        """发送自定义消息（burst 异动等）。

        节流策略：满足最小间隔且在每分钟上限内 → 立即发；
        否则暂存 pending_texts，由 flush_signals() 合并为摘要。
        """
        with self._lock:
            if self._can_send_now() and time.monotonic() - self._last_sent_at() >= self._min_interval:
                self._mark_sent()
                send_now = True
            else:
                send_now = False
                self._pending_texts.append((title, content))
                n = len(self._pending_texts)
        if send_now:
            return self.send(title, content)
        logger.debug(f"Custom message throttled, buffered ({n} pending): {title}")
        return False

    def flush_signals(self) -> int:
        """把限流期暂存的消息合并为摘要发出。

        Returns:
            本次 flush 发出的摘要条数（0 = 无暂存或窗口仍满保留到下轮）
        """
        with self._lock:
            signals = list(self._pending_signals)
            texts = list(self._pending_texts)
            self._pending_signals.clear()
            self._pending_texts.clear()

        sent = 0
        if signals:
            if not self._flush_allowed():
                # 窗口仍满：全部退回缓冲，下一轮再试（不丢消息）
                with self._lock:
                    self._pending_signals = signals + self._pending_signals
                logger.warning(f"Flush deferred, rate window full ({len(signals)} signals buffered)")
                return 0
            title, content = self._render_signals_digest(signals)
            if self.send(title, content):
                sent += 1
        if texts:
            if not self._flush_allowed():
                with self._lock:
                    self._pending_texts = texts + self._pending_texts
                logger.warning(f"Flush deferred, rate window full ({len(texts)} texts buffered)")
                return sent
            title, content = self._render_texts_digest(texts)
            if self.send(title, content):
                sent += 1
        return sent

    def _flush_allowed(self) -> bool:
        """flush 时窗口是否可发（独立判断，不受最小间隔限制——
        摘要本身已是聚合产物，且扫描轮间隔远大于最小间隔）"""
        with self._lock:
            return self._can_send_now()

    @staticmethod
    def _render_signals_digest(signals: list) -> tuple[str, str]:
        """多票信号合并为一条 markdown 摘要（钉钉官方建议的摘要式发送）"""
        action_emoji = {'BUY': '🟢', 'WATCH': '🟡', 'SKIP': '⚪'}
        lines = [f"## 📬 信号汇总（{len(signals)} 只，限流期合并发送）\n"]
        for s in signals:
            emoji = action_emoji.get(s.action.value, '❓')
            flow = ""
            if getattr(s, 'capital_flow_5d', None) is not None:
                sign = "+" if s.capital_flow_5d >= 0 else ""
                flow = f"｜资金{sign}{format_number(s.capital_flow_5d)}"
            reason = ', '.join(s.reason[:3]) if s.reason else ''
            lines.append(
                f"{emoji} **{s.name}** ({s.code}) {s.action.value} "
                f"评分{s.score:.1f} 仓位{s.position:.1%} {s.strategy}{flow}\n"
                f"　└ {reason}"
            )
        return f"📬 信号汇总 {len(signals)} 只", "\n".join(lines)

    @staticmethod
    def _render_texts_digest(texts: list[tuple[str, str]]) -> tuple[str, str]:
        """多条自定义消息合并为一条摘要（保留各条标题）"""
        lines = [f"## 📬 消息汇总（{len(texts)} 条，限流期合并发送）\n"]
        for title, content in texts:
            lines.append(f"### {title}\n{content}\n")
        return f"📬 消息汇总 {len(texts)} 条", "\n".join(lines)

    def send_signal(self, signal):
        """发送信号通知（节流：超限进聚合缓冲，轮末 flush_signals 合并发）"""
        action_emoji = {
            SignalAction.BUY: "🟢",
            SignalAction.WATCH: "🟡",
            SignalAction.SKIP: "⚪",
        }
        emoji = action_emoji.get(signal.action, "❓")
        title = f"{emoji} 量化信号: {signal.name}"
        # 资金流行（可选）：近5日主力净流入累计，正负号直读；无数据时隐藏整行
        flow_line = ""
        if signal.capital_flow_5d is not None:
            sign = "+" if signal.capital_flow_5d >= 0 else ""
            flow_line = f"- **资金**: 5日主力净流入 {sign}{format_number(signal.capital_flow_5d)}\n"
        content = f"""
- **股票**: {signal.name} ({signal.code})
- **操作**: {signal.action.value}
- **评分/级别**: {signal.score:.1f} / {signal.level.value if signal.level else 'N/A'}
- **仓位**: {signal.position:.1%}
- **价格**: {signal.price:.2f}
- **策略**: {signal.strategy}
{flow_line}- **原因**: {', '.join(signal.reason)}
        """
        with self._lock:
            if self._can_send_now() and time.monotonic() - self._last_sent_at() >= self._min_interval:
                self._mark_sent()
                send_now = True
            else:
                send_now = False
                self._pending_signals.append(signal)
                n = len(self._pending_signals)
        if send_now:
            return self.send(title, content)
        logger.debug(f"Signal throttled, buffered ({n} pending): {signal.code}")
        return False

    def send_market_alert(self, market):
        """发送市场状态变更通知（高频监控类：立即发送，不参与聚合）"""
        type_desc = {
            MarketType.BULL: "牛市",
            MarketType.BULL_WEAK: "弱牛",
            MarketType.SIDEWAYS: "震荡",
            MarketType.BEAR: "熊市",
        }
        title = "📊 市场状态变更"
        content = f"""
- **状态**: {type_desc.get(market.type, '未知')}
- **评分**: {market.score:.1f}
- **趋势**: {market.trend:.2f}
- **涨跌比**: {market.breadth:.1%}
        """
        with self._lock:
            # 市场告警低频（状态变更才触发），仍尊重每分钟硬上限防限流
            if self._can_send_now():
                self._mark_sent()
                send_now = True
            else:
                send_now = False
                self._pending_texts.append((title, content))
                logger.warning("Market alert throttled, buffered for next flush")
        if send_now:
            self.send(title, content)


# 导入这些类以便在manager中使用
from ...models.market import MarketType
from ...models.signal import SignalAction
