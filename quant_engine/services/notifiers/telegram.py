"""Telegram通知器"""
import httpx
from .base import BaseNotifier


class TelegramNotifier(BaseNotifier):
    """Telegram Bot通知"""

    def __init__(self, bot_token: str, chat_id: str):
        super().__init__("Telegram")
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.enabled = bool(bot_token and chat_id)
        self.api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    def send(self, title: str, content: str) -> bool:
        if not self.enabled:
            return False

        try:
            text = f"*{title}*\n\n{content}"
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }

            with httpx.Client(timeout=10) as client:
                response = client.post(self.api_url, json=payload)
                if response.status_code == 200:
                    result = response.json()
                    if result.get('ok'):
                        self._log_success()
                        return True
                    else:
                        self._log_error(str(result))
                        return False
                else:
                    self._log_error(f"HTTP {response.status_code}")
                    return False
        except Exception as e:
            self._log_error(str(e))
            return False