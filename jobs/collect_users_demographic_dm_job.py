import sys
from os import getenv
from pathlib import Path

from pyspark.sql.utils import CapturedException  # type: ignore

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config, UnableToGetConfig
from src.environ import DotEnvError, EnvironManager, EnvironNotSet
from src.helper import S3ServiceError
from src.keeper import ArgsKeeper, SparkConfigKeeper
from src.logger import SparkLogger
from src.spark import DatamartCollector

EnvironManager().load_environ()

config = Config(config_path=Path(getenv("PROJECT_PATH"), "config/config.yaml"))  # type: ignore

logger = SparkLogger(level=config.get_logging_level["python"]).get_logger(name=__name__)


def main() -> ...:
    try:
        if len(sys.argv) > 7:
            raise IndexError("Too many arguments for job submitting! Expected 6")

        keeper = ArgsKeeper(
            date=str(sys.argv[1]),
            depth=int(sys.argv[2]),
            src_path=str(sys.argv[3]),
            tgt_path=str(sys.argv[4]),
            coords_path=str(sys.argv[5]),
            processed_dttm=str(sys.argv[6]),
        )
        if not keeper.coords_path:
            raise S3ServiceError(
                "We need 'coords_path' for this job! Please specify one in given 'ArgsKeeper' instance"
            )
        spark_conf = SparkConfigKeeper(
            executor_memory="3000m", executor_cores=1, max_executors_num=12
        )

    except (IndexError, S3ServiceError) as err:
        logger.error(err)
        sys.exit(1)

    try:
        collector = DatamartCollector()
    except (DotEnvError, EnvironNotSet, UnableToGetConfig) as err:
        logger.error(err)
        sys.exit(1)

    try:
        for bucket in (keeper.src_path, keeper.tgt_path, keeper.coords_path):
            collector.check_s3_object_existence(key=bucket.split(sep="/")[2], type="bucket")  # type: ignore
    except S3ServiceError as err:
        logger.error(err)
        sys.exit(1)

    try:
        collector.init_session(
            app_name=config.get_spark_app_name,
            spark_conf=spark_conf,
            log4j_level=config.get_logging_level["java"],  # type: ignore
        )
        collector.collect_users_demographic_dm(keeper=keeper)

    except CapturedException as err:
        logger.error(err)
        sys.exit(1)

    finally:
        collector.stop_session()  # type: ignore
        sys.exit(2)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logger.exception(err)
        sys.exit(1)
