import sys
from pathlib import Path

from pyspark.sql.utils import AnalysisException, CapturedException  # type: ignore

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.logger import SparkLogger
from src.spark import SparkRunner
from src.keeper import ArgsKeeper, SparkConfigKeeper

config = Config("config.yaml")


logger = SparkLogger(level=config.python_log_level).get_logger(logger_name=__name__)


def main() -> None:
    try:
        DATE = str(sys.argv[1])
        DEPTH = int(sys.argv[2])
        SRC_PATH = str(sys.argv[3])
        TGT_PATH = str(sys.argv[4])
        PROCESSED_DTTM = str(sys.argv[5])

        if len(sys.argv) > 6:
            raise KeyError("Too many arguments for job submitting! Expected 5")

        keeper = ArgsKeeper(
            date=DATE,
            depth=DEPTH,
            src_path=SRC_PATH,
            tgt_path=TGT_PATH,
            processed_dttm=PROCESSED_DTTM,
        )
        conf = SparkConfigKeeper(
            executor_memory="3000m", executor_cores=1, max_executors_num=12
        )

    except (IndexError, KeyError) as e:
        logger.exception(e)
        sys.exit(1)
    try:
        spark = SparkRunner()
        spark.init_session(
            app_name=config.get_spark_application_name,
            spark_conf=conf,
            log4j_level=config.log4j_level,  # type: ignore
        )
        spark.collect_users_info_datamart(keeper=keeper)

    except (CapturedException, AnalysisException) as e:
        logger.exception(e)
        sys.exit(1)

    finally:
        spark.stop_session()  # type: ignore
        sys.exit(2)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
