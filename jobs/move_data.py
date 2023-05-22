import sys
from logging import getLogger
from pathlib import Path
from datetime import datetime

from pyspark.sql.utils import AnalysisException, CapturedException

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.logger import SparkLogger
from src.runner import SparkRunner
from src.utils import (
    ArgsHolder,
)

config = Config()

logger = SparkLogger(level=config.python_log_level).get_logger(
    logger_name=str(Path(Path(__file__).name))
)


def main() -> None:
    holder = ArgsHolder(
        date="2022-04-26",
        depth=2,
        src_path="s3a://data-ice-lake-04/messager-data/analytics/geo-events",
        tgt_path="s3a://data-ice-lake-04/messager-data/analytics/tmp/friend_recommendation_datamart",
        processed_dttm=str(datetime.now()),
    )
    try:
        spark = SparkRunner()
        spark.init_session(app_name="data-mover-app", log4j_level="INFO")
        spark.testing()

    except (CapturedException, AnalysisException) as e:
        logger.exception(e)
        sys.exit(1)

    finally:
        spark.stop_session()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
