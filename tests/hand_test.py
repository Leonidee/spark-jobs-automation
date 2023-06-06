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

from src.environ import EnvironManager
from src.keeper import ArgsKeeper


def main():
    environ = EnvironManager()
    environ.load_environ()

    _REQUIRED_VARS = (
        "HADOOP_CONF_DIR",
        "YARN_CONF_DIR",
        "JAVA_HOME",
        "SPARK_HOME",
        "PYTHONPATH",
    )

    environ.check_environ(var=_REQUIRED_VARS)


if __name__ == "__main__":
    main()
