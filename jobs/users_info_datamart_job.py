import sys
from pathlib import Path

from pyspark.sql.utils import CapturedException  # type: ignore

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config, EnableToGetConfig
from src.environ import DotEnvError, EnvironNotSet
from src.helper import S3ServiceError
from src.keeper import ArgsKeeper, SparkConfigKeeper
from src.logger import SparkLogger
from src.spark import SparkRunner

config = Config("config.yaml")

logger = SparkLogger().get_logger(logger_name=__name__)


def main() -> None:
    try:
        DATE = str(sys.argv[1])
        DEPTH = int(sys.argv[2])
        SRC_PATH = str(sys.argv[3])
        TGT_PATH = str(sys.argv[4])
        COORDS_PATH = str(sys.argv[5])
        PROCESSED_DTTM = str(sys.argv[6])

        if len(sys.argv) > 7:
            raise KeyError("Too many arguments for job submitting! Expected 6")

        keeper = ArgsKeeper(
            date=DATE,
            depth=DEPTH,
            src_path=SRC_PATH,
            tgt_path=TGT_PATH,
            coords_path=COORDS_PATH,
            processed_dttm=PROCESSED_DTTM,
        )
        spark_conf = SparkConfigKeeper(
            executor_memory="3000m", executor_cores=1, max_executors_num=12
        )

    except (IndexError, KeyError) as e:
        logger.error(e)
        sys.exit(1)

    try:
        spark = SparkRunner()
    except (DotEnvError, EnvironNotSet, EnableToGetConfig) as err:
        logger.error(err)
        sys.exit(1)

    try:
        for bucket in (keeper.src_path, keeper.tgt_path, keeper.coords_path):
            spark.check_s3_object_existence(key=bucket.split(sep="/")[2], type="bucket")
    except S3ServiceError as err:
        logger.error(err)
        sys.exit(1)

    try:
        spark.init_session(
            app_name=config.get_spark_application_name,
            spark_conf=spark_conf,
            log4j_level=config.log4j_level,  # type: ignore
        )
        spark.collect_users_info_datamart(keeper=keeper)

    except CapturedException as err:
        logger.error(err)
        sys.exit(1)

    finally:
        spark.stop_session()  # type: ignore
        sys.exit(2)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logger.exception(err)
        sys.exit(1)
