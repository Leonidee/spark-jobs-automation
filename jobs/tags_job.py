import sys
from pathlib import Path

from pyspark.sql.utils import CapturedException

sys.path.append(str(Path(__file__).parent.parent))

from src.jobs import SparkRunner
from src.logger import SparkLogger
from src.utils import SparkArgsValidator, TagsJobArgsHolder, get_src_paths

logger = SparkLogger().get_logger(logger_name=str(Path(Path(__file__).name)))
validator = SparkArgsValidator()


def main() -> None:
    try:
        if len(sys.argv) > 7:
            raise KeyError

        holder = TagsJobArgsHolder(
            date=sys.argv[1],
            depth=int(sys.argv[2]),
            threshold=int(sys.argv[3]),
            tags_verified_path=sys.argv[4],
            src_path=sys.argv[5],
            tgt_path=sys.argv[6],
        )

    except (IndexError, KeyError) as e:
        logger.exception(e)
        sys.exit(1)

    try:
        validator.validate_tags_job_args(holder=holder)
    except AssertionError as e:
        logger.exception(e)
        sys.exit(1)

    PATHS = get_src_paths(event_type="message", holder=holder)

    try:
        spark = SparkRunner(app_name="APP")
        spark.do_tags_job(
            holder=holder,
            src_paths=PATHS,
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
