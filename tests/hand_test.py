import os
import sys
from pathlib import Path
import time
from typing import Literal
from enum import Enum

from requests.exceptions import JSONDecodeError
from dataclasses import dataclass
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent))

from src.keeper import ArgsKeeper


def main():
    keeper = ArgsKeeper(
        date="2022-04-03",
        depth=10,
        src_path="s3a://...",
        tgt_path="s3a://...",
        processed_dttm="2023-05-22T12:03:25",
    )
    print(keeper)

    keeper = ArgsKeeper(
        date="2022-04-03",
        depth=10,
        src_path="s3a://...",
        tgt_path="s3a://...",
        processed_dttm="2023-05-22",
    )
    print(keeper)


if __name__ == "__main__":
    main()
