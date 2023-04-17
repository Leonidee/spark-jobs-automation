import sys

from utils import assert_args
from jobs import SparkKiller


def main() -> None:
    try:
        DATE = sys.argv[1]
        DEPTH = sys.argv[2]
        THRESHOLD = sys.argv[3]
        VERIFIED_TAGS_PATH = sys.argv[4]
        SRC_PATH = sys.argv[5]
        TGT_PATH = sys.argv[6]

        assert_args(date=DATE, depth=DEPTH, threshold=THRESHOLD)

    except IndexError:
        print(
            "Wrong arguments! Usage of `main.py` is: spark-submit main.py <date> <depth> <threshold> <verified_tags_path> <src_path> <tgt_path>"
        )
        sys.exit(1)

    except AssertionError:
        sys.exit(1)

    except Exception:
        print("Unable to submit spark application! Something went wrong.")
        sys.exit(1)

    try:
        spark = SparkKiller(app_name="APP")
        spark.get_tags_dataset(
            date=DATE,
            depth=int(DEPTH),
            threshold=int(THRESHOLD),
            tags_verified_path=str(VERIFIED_TAGS_PATH),
            src_path=str(SRC_PATH),
            tgt_path=str(TGT_PATH),
        )

    except Exception:
        print("Unable to submit spark application! Something went wrong.")
        sys.exit(1)


if __name__ == "__main__":
    main()
