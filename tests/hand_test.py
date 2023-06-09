import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Literal

from requests.exceptions import JSONDecodeError

sys.path.append(str(Path(__file__).parent.parent))
# package
sys.path.append(str(Path(__file__).parent.parent))
from src.base import BaseRequestHandler
from src.config import Config, EnableToGetConfig
from src.environ import DotEnvError, EnvironNotSet
from src.helper import S3ServiceError, SparkHelper
from src.keeper import ArgsKeeper, SparkConfigKeeper
from src.logger import SparkLogger
from src.spark import SparkRunner

logger = SparkLogger().get_logger(logger_name=__name__)


class A:
    __slots__ = "a", "b"

    def __init__(self, a, b) -> None:
        self.a = a
        self.b = b


class B(A):
    __slots__ = "c"

    def __init__(self, a, b, c) -> None:
        super().__init__(a, b)
        self.c = c

    def get_slots(self):
        for _ in self.__slots__:
            print(_)


def main():
    b = B(a="..", b="..", c="some")

    b.a = "new"
    b.new = 1

    b.get_slots()

    print(b.new)


if __name__ == "__main__":
    main()
