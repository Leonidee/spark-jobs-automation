import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Literal

import boto3
import findspark
from botocore.exceptions import ClientError

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.logger import SparkLogger
from src.utils import (
    # SparkArgsValidator,
    EnvironManager,
    ArgsKeeper,
    SparkConfigKeeper,
)

os.environ["HADOOP_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr/bin/java"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/opt/conda/bin/python3"

findspark.init()
findspark.find()

from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as f


from src.runner import SparkRunner
from src.utils import ArgsKeeper


class SparkRunner_(SparkRunner):
    def __init__(self) -> None:
        super().__init__()

    def init_session(
        self,
        app_name: str,
        spark_conf: SparkConfigKeeper,
        log4j_level: Literal[
            "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"
        ] = "WARN",
    ) -> None:
        return super().init_session(app_name, spark_conf, log4j_level)

    def stop_session(self) -> None:
        return super().stop_session()

    def _get_src_paths(
        self,
        event_type: Literal["message", "reaction", "subscription"],
        keeper: ArgsKeeper,
    ) -> List[str]:
        return super()._get_src_paths(event_type, keeper)

    def testing(self, keeper: ArgsKeeper) -> None:
        src_paths = self._get_src_paths(keeper=keeper, event_type="message")
        messages_sdf = (
            self.spark.read.option("mergeSchema", "true")
            .option("cacheMetadata", "true")
            .parquet(*src_paths)
        )

        messages_sdf = (
            messages_sdf.where(messages_sdf.message_from.isNotNull())
            .select(
                messages_sdf.message_from.alias("user_id"),
                messages_sdf.message_id,
                messages_sdf.message_ts,
                messages_sdf.datetime,
                messages_sdf.lat.alias("event_lat"),
                messages_sdf.lon.alias("event_lon"),
            )
            .withColumn(
                "msg_ts",
                f.when(f.col("message_ts").isNotNull(), f.col("message_ts")).otherwise(
                    f.col("datetime")
                ),
            )
            .drop_duplicates(subset=["user_id", "message_id", "msg_ts"])
            .drop("datetime", "message_ts")
        )
        messages_sdf = self._get_event_location(
            dataframe=messages_sdf, event_type="message"
        )

        messages_sdf = (
            messages_sdf.withColumnRenamed("city_id", "zone_id")
            .withColumn("week", f.trunc(f.col("msg_ts"), "week"))
            .withColumn("month", f.trunc(f.col("msg_ts"), "month"))
            .groupby("month", "week", "zone_id")
            .agg(f.count("message_id").alias("week_message"))
            .withColumn(
                "month_message",
                f.sum(f.col("week_message")).over(
                    Window().partitionBy(f.col("zone_id"), f.col("month"))
                ),
            )
        )

        messages_sdf.show(200, False)

    def how_it_looks(self, keeper: ArgsKeeper) -> None:
        src_paths = self._get_src_paths(keeper=keeper, event_type="message")
        messages_sdf = (
            self.spark.read.option("mergeSchema", "true")
            .option("cacheMetadata", "true")
            .parquet(*src_paths)
        )
        messages_sdf.show(100)


if __name__ == "__main__":
    conf = SparkConfigKeeper(
        executor_memory="3000m", executor_cores=1, max_executors_num=12
    )
    keeper = ArgsKeeper(
        date="2022-04-26",
        depth=30,
        src_path="s3a://data-ice-lake-05/messager-data/analytics/geo-events",
        tgt_path="s3a://data-ice-lake-05/messager-data/analytics/tmp/location_zone_agg_datamart",
        processed_dttm=str(datetime.now()),
    )

    try:
        spark = SparkRunner_()
        spark.init_session(app_name="testing-app", spark_conf=conf, log4j_level="INFO")
        spark.how_it_looks(keeper=keeper)

    except Exception as e:
        print(e)
        sys.exit(1)

    finally:
        spark.stop_session()  # type: ignore
