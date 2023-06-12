import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from src.keeper import ArgsKeeper
from src.submitter import SparkSubmitter


def main() -> ...:
    keeper = ArgsKeeper(
        date="2022-04-26",
        depth=10,
        src_path="s3a://data-ice-lake-05/messager-data/analytics/geo-events",
        tgt_path="s3a://data-ice-lake-05/messager-data/analytics/tmp",
        coords_path="s3a://data-ice-lake-05/messager-data/analytics/cities-coordinates",
        processed_dttm="2023-05-22T12:03:25",
    )

    spark = SparkSubmitter()
    spark.submit_job(job="collect_users_demographic_dm_job", keeper=keeper)


if __name__ == "__main__":
    main()
