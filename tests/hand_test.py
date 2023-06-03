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


def main():
    import os

    for dirpath, dirname, files in os.walk(Path(__file__).parent.parent):
        if (
            ".venv" not in dirpath
            and ".pytest_cache" not in dirpath
            and ".git" not in dirpath
        ):
            if "__pycache__" in dirpath:
                print(dirpath)
                os.remove(path=dirpath)

        # for file in files:
        #     if ".pyc" in file:
        #         print(file)


if __name__ == "__main__":
    main()
