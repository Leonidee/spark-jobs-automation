import sys
from pathlib import Path

from pyspark.sql.utils import CapturedException

# package
sys.path.append(str(Path(__file__).parent.parent))

from src.jobs import SparkRunner
from src.logger import SparkLogger
from src.utils import TagsJobArgsHolder

logger = SparkLogger().get_logger(logger_name=str(Path(Path(__file__).name)))


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
        spark = SparkRunner()
        spark.do_tags_job(
            holder=holder,
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
