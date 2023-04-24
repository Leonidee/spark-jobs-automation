import sys
from pathlib import Path
from pyspark.sql.utils import CapturedException

from jobs.jobs import SparkRunner
from src.logger import SparkLogger

logger = SparkLogger().get_logger(logger_name=str(Path(Path(__file__).name)))


def main() -> None:
    try:
        if len(sys.argv) > 7:
            raise KeyError

        DATE = str(sys.argv[1])
        DEPTH = int(sys.argv[2])
        THRESHOLD = int(sys.argv[3])
        VERIFIED_TAGS_PATH = str(sys.argv[4])
        SRC_PATHS = str(sys.argv[5])
        TGT_PATH = str(sys.argv[6])

    except (IndexError, KeyError) as e:
        logger.exception(e)
        sys.exit(1)

    try:
        spark = SparkRunner(app_name="APP")
        spark.do_tags_job(
            date=DATE,
            depth=DEPTH,
            threshold=THRESHOLD,
            tags_verified_path=VERIFIED_TAGS_PATH,
            src_paths=SRC_PATHS,
            tgt_path=TGT_PATH,
        )
    except CapturedException as e:
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
