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
from src.utils import ArgsKeeper, SparkConfigKeeper

config = Config()

logger = SparkLogger(level=config.python_log_level).get_logger(logger_name=__name__)


def main() -> None:
    keeper = ArgsKeeper(
        date="2022-04-26",
        depth=10,
        src_path="s3a://data-ice-lake-05/messager-data/analytics/geo-events",
        tgt_path="s3a://data-ice-lake-05/messager-data/analytics/tmp/friend_recommendation_datamart",
        processed_dttm=str(datetime.now()),
    )
    conf = SparkConfigKeeper(
        executor_memory="3000m", executor_cores=1, max_executors_num=12
    )

    try:
        spark = SparkRunner()
        spark.init_session(app_name="testing-app", spark_conf=conf, log4j_level="INFO")
        spark.testing(keeper=keeper)

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
