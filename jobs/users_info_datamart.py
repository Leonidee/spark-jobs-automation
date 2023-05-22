import sys
from logging import getLogger
from pathlib import Path

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
    try:
        # if len(sys.argv) > 6:
        #     raise KeyError

        # keeper = ArgsKeeper(
        #     date=str(sys.argv[1]),
        #     depth=int(sys.argv[2]),
        #     src_path=str(sys.argv[3]),
        #     tgt_path=str(sys.argv[4]),
        #     processed_dttm=str(sys.argv[5]),
        # )
        conf = SparkConfigKeeper(
            executor_memory="3000m", executor_cores=1, max_executors_num=12
        )
        # todo remove this
        from datetime import datetime

        keeper = ArgsKeeper(
            date="2022-04-26",
            depth=30,
            src_path="s3a://data-ice-lake-05/messager-data/analytics/geo-events",
            tgt_path="s3a://data-ice-lake-05/messager-data/analytics/tmp/friend_recommendation_datamart",
            processed_dttm=str(datetime.now()),
        )

    except (IndexError, KeyError) as e:
        logger.exception(e)
        sys.exit(1)
    try:
        spark = SparkRunner()
        spark.init_session(
            app_name=config.get_spark_application_name,
            spark_conf=conf,
            log4j_level=config.log4j_level,
        )
        spark.collect_users_info_datamart(keeper=keeper)

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
