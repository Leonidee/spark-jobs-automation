import sys
from datetime import datetime
from pathlib import Path
from typing import Literal

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.keeper import SparkConfigKeeper
from src.logger import SparkLogger
from src.spark.runner import SparkRunner


class DataMover(SparkRunner):
    __slots__ = "logger"

    def __init__(self) -> None:
        super().__init__()

        self.logger = SparkLogger(
            level=self.config.get_logging_level["python"]
        ).get_logger(name=__name__)

    def init_session(
        self,
        app_name: str,
        spark_conf: SparkConfigKeeper,
        log4j_level: Literal[
            "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"
        ] = "WARN",
    ) -> ...:
        return super().init_session(app_name, spark_conf, log4j_level)

    def stop_session(self) -> ...:
        return super().stop_session()

    def _move_data(self, source_path: str, tgt_path: str) -> ...:
        "Moves data between DWH layers. This method not for public calling"

        _job_start = datetime.now()

        import pyspark.sql.functions as F

        df = (
            self.spark.read.option("mergeSchema", "true")
            .option("cacheMetadata", "true")
            .parquet(source_path)
        )
        df = df.repartition(56)

        df = (
            df.withColumn("admins", df.event.admins)
            .withColumn("channel_id", df.event.channel_id)
            .withColumn(
                "datetime",
                F.to_timestamp(col=df.event.datetime, format="yyyy-MM-dd HH:mm:ss"),
            )
            .withColumn("media_type", df.event.media.media_type)
            .withColumn("media_src", df.event.media.src)
            .withColumn("message", df.event.message)
            .withColumn("message_channel_to", df.event.message_channel_to)
            .withColumn("message_from", df.event.message_from)
            .withColumn("message_group", df.event.message_group)
            .withColumn("message_id", df.event.message_id)
            .withColumn("message_to", df.event.message_to)
            .withColumn(
                "message_ts",
                F.to_timestamp(
                    F.split(str=df.event.message_ts, pattern=r"\.").getItem(0),
                    format="yyyy-MM-dd HH:mm:ss",
                ),
            )
            .withColumn("reaction_from", df.event.reaction_from)
            .withColumn("reaction_type", df.event.reaction_type)
            .withColumn("subscription_channel", df.event.subscription_channel)
            .withColumn("subscription_user", df.event.subscription_user)
            .withColumn("tags", df.event.tags)
            .withColumn("user", df.event.user)
            .withColumn("event_type", df.event_type)
            .withColumn("date", F.date_format("datetime", "yyyy-MM-dd"))
            .withColumn("lat", df.lat)
            .withColumn("lon", df.lon)
            .select(
                "admins",
                "channel_id",
                "datetime",
                "media_type",
                "media_src",
                "message",
                "message_channel_to",
                "message_from",
                "message_group",
                "message_id",
                "message_to",
                "message_ts",
                "reaction_from",
                "reaction_type",
                "subscription_channel",
                "subscription_user",
                "tags",
                "user",
                "event_type",
                "lat",
                "lon",
                "date",
            )
        )

        df.repartition(1).write.parquet(
            path=tgt_path,
            mode="overwrite",
            partitionBy=["event_type", "date"],
            compression="gzip",
        )
        _job_end = datetime.now()

        self.logger.info(f"Job execution time: {_job_end - _job_start}")
