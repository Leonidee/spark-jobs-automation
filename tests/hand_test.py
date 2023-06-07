import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Literal

from requests.exceptions import JSONDecodeError

sys.path.append(str(Path(__file__).parent.parent))
# package
sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config, EnableToGetConfig
from src.environ import DotEnvError, EnvironNotSet
from src.helper import S3ServiceError
from src.keeper import ArgsKeeper, SparkConfigKeeper
from src.logger import SparkLogger
from src.spark import SparkRunner

logger = SparkLogger().get_logger(logger_name=__name__)


def main():
    keeper = ArgsKeeper(
        date="2022-04-26",
        depth=62,
        src_path="s3a://data-ice-lake-04/messager-data/analytics/geo-events",
        tgt_path="s3a://data-ice-lake-05/messager-data/analytics/tmp/location_zone_agg_datamart",
        coords_path="s3a://data-ice-lake-06/messager-data/analytics/cities-coordinates",
        processed_dttm="2023-05-22T12:03:25",
    )

    spark = SparkRunner()

    try:
        for bucket in (keeper.src_path, keeper.tgt_path, keeper.coords_path):
            if not spark.check_s3_object_existence(
                key=bucket.split(sep="/")[2], type="bucket"
            ):
                raise S3ServiceError(
                    f'Bucket {bucket.split(sep="/")[2]} does not exists'
                )
    except S3ServiceError as err:
        logger.exception(err)
        sys.exit(1)


if __name__ == "__main__":
    main()
