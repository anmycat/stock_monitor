"""飞书通知器"""
import httpx
from loguru import logger
from .base import BaseNotifier


class FeishuNotifier(BaseNotifier):
    """飞书机器人通知"""

    def __init__(self, webhook_url: str):
        super().__init__("Feishu")
        self.webhook_url = webhook_url
        self.enabled = bool(webhook_url)

    def send(self, title: str, content: str) -> bool:
        if not self.enabled:
            return False

        try:
            payload = {
                "msg_type": "interactive",
                "card": {
                    "header": {
                        "title": {
                            "tag": "plain_text",
                            "content": title
                        }
                    },
                    "elements": [
                        {
                            "tag": "markdown",
                            "content": content
                        }
                    ]
                }
            }

            with httpx.Client(timeout=10) as client:
                response = client.post(self.webhook_url, json=payload)
                if response.status_code == 200:
                    result = response.json()
                    if result.get('code') == 0:
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