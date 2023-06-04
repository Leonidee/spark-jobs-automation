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
    class Test:
        def __init__(self) -> None:
            pass

        def print_some(self):
            print("some!")

    class DoTest(Test):
        def __init__(self) -> None:
            super().__init__()

    do_test = DoTest()

    do_test.print_some()


if __name__ == "__main__":
    main()
