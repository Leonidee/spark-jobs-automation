import sys
from pathlib import Path
from typing import Literal
from src.keeper import SparkConfigKeeper

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.spark.runner import SparkRunner
from src.logger import SparkLogger


class DataMover(SparkRunner):
    def __init__(self) -> None:
        super().__init__()

        self.logger = SparkLogger().get_logger(logger_name=__name__)

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

    def _move_data(self, source_path: str, tgt_path: str):
        "Moves data between DWH layers. This method not for public calling"

        job_start = datetime.now()

        df = (
            self.spark.read.option("mergeSchema", "true")
            .option("cacheMetadata", "true")
            .parquet("s3a://data-ice-lake-04/messager-data/analytics/geo-events")
        )
        df = df.repartition(56)

        df = (
            df.withColumn("admins", df.event.admins)
            .withColumn("channel_id", df.event.channel_id)
            .withColumn(
                "datetime",
                to_timestamp(col=df.event.datetime, format="yyyy-MM-dd HH:mm:ss"),
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
                to_timestamp(
                    split(str=df.event.message_ts, pattern="\.").getItem(0),
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
            .withColumn("date", date_format("datetime", "yyyy-MM-dd"))
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

        tgt_path = "s3a://data-ice-lake-05/messager-data/analytics/geo-events"
        df.repartition(1).write.parquet(
            path=tgt_path,
            mode="overwrite",
            partitionBy=["event_type", "date"],
            compression="gzip",
        )
        job_end = datetime.now()
        self.logger.info(f"Job execution time: {job_end - job_start}")
