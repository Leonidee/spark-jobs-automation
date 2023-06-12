import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.logger import SparkLogger


def main() -> ...:
    import os

    conf = Config(config_name="config.yaml")
    print(conf.environ)

    conf.environ = "testing"
    print(conf.environ)

    conf.environ = "airflow"
    print(conf.environ)


if __name__ == "__main__":
    main()
