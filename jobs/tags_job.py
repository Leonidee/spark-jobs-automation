import sys
from pathlib import Path
from pyspark.sql.utils import CapturedException


sys.path.append(str(Path(__file__).parent.parent))

from src.jobs import SparkRunner
from src.logger import SparkLogger
from src.utils import validate_job_submit_args, get_src_paths

logger = SparkLogger().get_logger(logger_name=str(Path(Path(__file__).name)))


def main() -> None:
    try:
        if len(sys.argv) > 7:
            raise KeyError

        DATE = str(sys.argv[1])
        DEPTH = str(sys.argv[2])
        THRESHOLD = str(sys.argv[3])
        VERIFIED_TAGS_PATH = str(sys.argv[4])
        SRC_PATH = str(sys.argv[5])
        TGT_PATH = str(sys.argv[6])

    except (IndexError, KeyError) as e:
        logger.exception(e)
        sys.exit(1)

    try:
        validate_job_submit_args(date=DATE, depth=DEPTH, threshold=THRESHOLD)
    except AssertionError as e:
        logger.exception(e)
        sys.exit(1)

    PATHS = get_src_paths(
        event_type="message", date=DATE, depth=int(DEPTH), src_path=SRC_PATH
    )

    try:
        spark = SparkRunner(app_name="APP")
        spark.do_tags_job(
            date=DATE,
            depth=int(DEPTH),
            threshold=int(THRESHOLD),
            tags_verified_path=VERIFIED_TAGS_PATH,
            src_paths=PATHS,
            tgt_path=TGT_PATH,
        )
        spark.stop_session()
    except CapturedException as e:
        logger.exception(e)
        spark.stop_session()
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
