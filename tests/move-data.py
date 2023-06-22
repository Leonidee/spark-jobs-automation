import sys
from pathlib import Path

from tqdm import tqdm

sys.path.append(str(Path(__file__).parent.parent))
from src.helper import SparkHelper


def main() -> None:
    BUCKET = "data-ice-lake-05"
    helper = SparkHelper()

    s3 = helper._get_s3_instance()

    response = s3.list_objects_v2(
        Bucket=BUCKET,
        MaxKeys=5,
        Prefix="prod",
    )
    bar = tqdm(total=len(response["Contents"]))

    for i in response["Contents"]:
        try:
            source_key = i["Key"]
            print(source_key)

            key = source_key.split("/", maxsplit=3)
            dist_key = f"prod/dictionary/messenger-yp/cities-coordinates-dict/{key[3]}"
            print(dist_key)

            s3.copy_object(
                Bucket=BUCKET,
                CopySource={"Bucket": BUCKET, "Key": source_key},
                Key=dist_key,
            )
            bar.update(1)

        except Exception as err:
            print(err)
            pass


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(err)
        sys.exit(1)
