import sys
from pathlib import Path

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.logger import SparkLogger


def main() -> ...:
    from pprint import pprint

    config = Config("config.yaml")
    a = config.get_job_config["collect_add_to_friends_recommendations_dm_job"]

    pprint(a["depth"])


if __name__ == "__main__":
    main()
