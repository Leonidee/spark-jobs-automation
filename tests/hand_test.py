import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config


def main() -> ...:
    import os

    conf = Config(config_name="config.yaml")
    a = conf.get_logging_level["python"]
    print(a)


if __name__ == "__main__":
    main()
