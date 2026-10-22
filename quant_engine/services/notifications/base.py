"""通知器基类"""
from abc import ABC, abstractmethod

from loguru import logger


class BaseNotifier(ABC):
    """通知器基类"""

    def __init__(self, name: str):
        self.name = name
        self.enabled = True

    @abstractmethod
    def send(self, title: str, content: str) -> bool:
        """发送消息"""
        pass

    def send_text(self, text: str) -> bool:
        """发送纯文本"""
        return self.send("通知", text)

    def _log_success(self):
        logger.info(f"[{self.name}] Message sent successfully")

    def _log_error(self, error: str):
        logger.error(f"[{self.name}] Send failed: {error}")