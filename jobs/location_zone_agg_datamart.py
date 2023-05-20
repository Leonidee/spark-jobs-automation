import sys
from pathlib import Path
from logging import getLogger

from pyspark.sql.utils import (
    CapturedException,
    AnalysisException,
)

# package
sys.path.append(str(Path(__file__).parent.parent))
from scripts.logger import SparkLogger
from scripts.runner import SparkRunner
from scripts.utils import ArgsHolder
from scripts.config import Config

config = Config()


logger = (
    getLogger("aiflow.task")
    if config.IS_PROD
    else SparkLogger(level=config.log_level).get_logger(
        logger_name=str(Path(Path(__file__).name))
    )
)


def main() -> None:
    try:
        if len(sys.argv) > 6:
            raise KeyError

        holder = ArgsHolder(
            date=str(sys.argv[1]),
            depth=int(sys.argv[2]),
            src_path=str(sys.argv[3]),
            tgt_path=str(sys.argv[4]),
            processed_dttm=str(sys.argv[5]),
        )

    except (IndexError, KeyError) as e:
        logger.exception(e)
        sys.exit(1)
    try:
        spark = SparkRunner()
        spark.init_session(
            app_name=config.spark_application_name, log4j_level=config.log4j_level
        )
        spark.compute_location_zone_agg_datamart(holder=holder)

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
