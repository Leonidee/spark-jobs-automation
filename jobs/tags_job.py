import sys

from utils import assert_args
from jobs import SparkKiller


def main() -> None:
    USAGE_MESSAGE = "\nSubmiting tags_job.py\n\nUsage:\n\t`spark-submit tags_job.py <date> <depth> <threshold> <verified_tags_path> <src_path> <tgt_path>`\n"
    try:
        if sys.argv[1] == "help":
            print(USAGE_MESSAGE)
            sys.exit(1)

        if len(sys.argv) > 7:
            raise KeyError

        DATE = sys.argv[1]
        DEPTH = sys.argv[2]
        THRESHOLD = sys.argv[3]
        VERIFIED_TAGS_PATH = sys.argv[4]
        SRC_PATH = sys.argv[5]
        TGT_PATH = sys.argv[6]

    except (IndexError, KeyError):
        print(USAGE_MESSAGE)
        sys.exit(1)

    try:
        assert_args(date=DATE, depth=DEPTH, threshold=THRESHOLD)
    except AssertionError as e:
        print(e)
        sys.exit(1)

    try:  # ?
        spark = SparkKiller(app_name="APP")
        spark.get_tags_dataset(
            date=DATE,
            depth=int(DEPTH),
            threshold=int(THRESHOLD),
            tags_verified_path=str(VERIFIED_TAGS_PATH),
            src_path=str(SRC_PATH),
            tgt_path=str(TGT_PATH),
        )

    except Exception:  # ?
        print("Unable to submit spark application! Something went wrong.")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
        sys.exit(1)
