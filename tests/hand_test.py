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


from typing import Tuple


def main(one: str, two: Tuple[str, str]) -> Tuple[str, ...]:
    if one:
        if two:
            print("Hi!")
    if not one:
        raise ValueError("error!")
    if not two:
        raise KeyError("...")

    return ()


if __name__ == "__main__":
    ...
