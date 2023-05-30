import pytest
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.config import EnableToGetConfig
from src.config import Config


#
# class TestConfig:


if __name__ == "__main__":
    conf = Config()
    print(conf.get_spark_application_name)
