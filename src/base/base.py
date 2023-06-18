from __future__ import annotations

import sys
from os import getenv
from pathlib import Path

# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.config import Config
from src.environ import EnvironManager


class BaseRequestHandler:
    """Base Requests handler class. Contains basic attributes. Must be inherited by other classes."""

    __slots__ = (
        "_MAX_RETRIES",
        "_DELAY",
        "_SESSION_TIMEOUT",
        "_CLUSTER_ID",
        "_BASE_URL",
        "_OAUTH_TOKEN",
        "_IAM_TOKEN",
        "_CLUSTER_API_BASE_URL",
        "config",
    )

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

        env_man = EnvironManager()
        env_man.load_environ()

        _REQUIRED_VARS = (
            "YC_DATAPROC_CLUSTER_ID",
            "YC_DATAPROC_BASE_URL",
            "YC_OAUTH_TOKEN",
            "CLUSTER_API_BASE_URL",
            "PROJECT_PATH",
        )
        env_man.check_environ(var=_REQUIRED_VARS)

        (
            self._CLUSTER_ID,
            self._BASE_URL,
            self._OAUTH_TOKEN,
            self._CLUSTER_API_BASE_URL,
        ) = map(getenv, _REQUIRED_VARS[:4])

        self.config = Config(
            config_path=Path(getenv("PROJECT_PATH"), "config/config.yaml")  # type: ignore
        )

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
