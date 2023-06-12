import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.logger import SparkLogger


def main() -> ...:
    import os

    log = SparkLogger().get_logger(name=__name__)
    log.info("everythings is ok")

    a = Path(__file__).absolute()
    print(a)


if __name__ == "__main__":
    main()
