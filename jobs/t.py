import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from src import ArgsKeeper

if __name__ == "__main__":
    keeper = ArgsKeeper(
        date="2022-04-26",
        depth=62,
        src_path="s3a://data-ice-lake-05/messager-data/analytics/geo-events",
        tgt_path="s3a://data-ice-lake-05/messager-data/analytics/tmp/location_zone_agg_datamart",
        processed_dttm="2023-05-22T12:03:25",
    )
    print(keeper)
