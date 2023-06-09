from __future__ import annotations

import sys
from pathlib import Path

# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.config import Config


class BaseRequestHandler:
    """Base Requests handler class. Contains basic attributes. Must be inherited by other classes."""

    __slots__ = "_MAX_RETRIES", "_DELAY", "_SESSION_TIMEOUT", "config", "logger"

    def __init__(
        self,
        *,
        max_retries: int = 10,
        retry_delay: int = 60,
        session_timeout: int = 60 * 2,
    ) -> None:
        self._MAX_RETRIES = max_retries
        self._DELAY = retry_delay
        self._SESSION_TIMEOUT = session_timeout

        self.config = Config(config_name="config.yaml")

    @property
    def max_retries(self) -> int:
        """
        Max retries to send request. Must be lower than 20 times
        """
        return self._MAX_RETRIES

    @max_retries.setter
    def max_retries(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError("value must be integer")

        if value > 20:
            raise ValueError("value must be lower than 20")

        if value < 0:
            raise ValueError("value must be positive")

        self._MAX_RETRIES = value

    @max_retries.deleter
    def max_retries(self) -> None:
        "Reset to default value"
        self._MAX_RETRIES = 10

    @property
    def retry_delay(self) -> int:
        """Delay between retries in secs. Must be lower than 30 minutes"""
        return self._DELAY

    @retry_delay.setter
    def retry_delay(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError("value must be integer")

        if value < 0:
            raise ValueError("value must be positive")

        if value > 60 * 30:
            raise ValueError("must be lower than 30 minutes")

        self._DELAY = value

    @retry_delay.deleter
    def retry_delay(self) -> None:
        "Reset to default value"
        self._DELAY = 60

    @property
    def session_timeout(self) -> int:
        """Session timeout. Must be lower than 120 minutes"""
        return self._SESSION_TIMEOUT

    @session_timeout.setter
    def session_timeout(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError("value must be integer")

        if value < 0:
            raise ValueError("value must be positive")

        if value > 60 * 60 * 2:
            raise ValueError("must be lower than 60 * 60 * 2")

        self._SESSION_TIMEOUT = value

    @session_timeout.deleter
    def session_timeout(self) -> None:
        "Reset to default value"
        self._SESSION_TIMEOUT = 60 * 2
