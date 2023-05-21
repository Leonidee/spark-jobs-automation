import sys
from logging import getLogger
from pathlib import Path

from pyspark.sql.utils import AnalysisException, CapturedException

# package
sys.path.append(str(Path(__file__).parent.parent))
from scripts.config import Config
from scripts.logger import SparkLogger
from scripts.runner import SparkRunner

config = Config()

logger = SparkLogger(level=config.python_log_level).get_logger(
    logger_name=str(Path(Path(__file__).name))
)


def main() -> None:
    try:
        spark = SparkRunner()
        spark.init_session(app_name="data-mover-app", log4j_level="INFO")
        spark.move_data(
            src_path="s3a://data-ice-lake-04/messager-data/snapshots/tags_verified/actual",
            tgt_path="s3a://data-ice-lake-05/messager-data/snapshots/tags_verified/actual",
        )

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
