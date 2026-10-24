"""通知管理器 - 统一管理所有通知渠道"""

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
    """统一通知管理器"""

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

        logger.info(f"NotificationManager initialized with {len(self.notifiers)} notifiers")

    def send(self, title: str, content: str):
        """发送到所有已配置的渠道"""
        for notifier in self.notifiers:
            try:
                notifier.send(title, content)
            except Exception as e:
                logger.error(f"Send to {notifier.name} failed: {e}")

    def send_custom_message(self, title: str, content: str):
        """发送自定义消息（兼容旧代码）"""
        self.send(title, content)

    def send_signal(self, signal):
        """发送信号通知"""
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
        self.send(title, content)

    def send_market_alert(self, market):
        """发送市场状态变更通知"""
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
        self.send(title, content)


# 导入这些类以便在manager中使用
from ...models.market import MarketType
from ...models.signal import SignalAction
