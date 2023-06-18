import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config


def main():
    config = Config(config_path=Path(os.getenv("PROJECT_PATH"), "config/config.yaml"))


if __name__ == "__main__":
    main()
