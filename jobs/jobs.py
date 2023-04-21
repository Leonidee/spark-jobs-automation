import os
import findspark
from datetime import datetime, timedelta
from typing import List
from pathlib import Path

from typing import Literal

os.environ["HADOOP_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr/bin/java"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/opt/conda/bin/python3"

findspark.init()
findspark.find()

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from utils import SparkLogger


class SparkKiller:
    def __init__(self, app_name: str):
        self.logger = SparkLogger.get_logger(logger_name=str(Path(Path(__file__).name)))
        self.spark = SparkSession.builder.master("yarn").appName(app_name).getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.logger.info("Initializing Spark Session.")

    def _get_partitions_paths(
        self,
        event_type: Literal["message", "reaction", "subscription"],
        date: str,
        depth: int,
        src_path: str,
    ) -> List:
        if event_type in ("message", "reaction", "subscription"):
            date = datetime.strptime(date, "%Y-%m-%d").date()
            paths = [
                f"{src_path}/event_type={event_type}/date="
                + str(date - timedelta(days=i))
                for i in range(depth)
            ]
            self.logger.info("Done.")

        else:
            raise AttributeError(
                "`event_type` must be only message, reaction or subscription."
            )

        return paths

    def do_tags_job(
        self,
        date: str,
        depth: int,
        threshold: int,
        src_path: str,
        tags_verified_path: str,
        tgt_path: str,
    ) -> None:
        self.logger.info(
            f"Getting `tags` dataset from {date} date with {depth} days depth and {threshold} unique users threshold."
        )
        try:
            self.logger.info("Preparing s3 paths.")
            paths = self._get_partitions_paths(
                event_type="message", date=date, depth=depth, src_path=src_path
            )
            self.logger.info("Done.")
        except AttributeError as e:
            self.logger.exception(e)

        try:
            self.logger.info("Reading parquet on s3.")
            messages = self.spark.read.parquet(*paths, compression="gzip")
            self.logger.info(
                f"Successfully get `tags` dataset with {messages.count()} rows."
            )
        except Exception as e:
            self.logger.exception("Unable access to s3 service!")
            raise e

        try:
            self.logger.info("Getting `tags_verified` dataset.")
            tags_verified = self.spark.read.parquet(tags_verified_path)

            tags_verified = (
                tags_verified.withColumn("tag", f.trim(tags_verified.tag))
                .select("tag")
                .distinct()
            )
        except Exception as e:
            self.logger.exception("Unable access to s3 service!")
            raise e

        try:
            self.logger.info("Grouping `tags` dataset.")
            all_tags = (
                messages.where(messages.message_channel_to.isNotNull())
                .withColumn("tag", f.explode(messages.tags))
                .select(f.col("message_from"), f.col("tag"))
                .groupBy(f.col("tag"))
                .agg(f.count_distinct(messages.message_from).alias("suggested_count"))
                .where(f.col("suggested_count") >= threshold)
            )

        except Exception as e:
            self.logger.exception(
                "Unable to do some transformations with dataset! Something went wrong."
            )
            raise e

        try:
            self.logger.info("Excluding verified tags.")
            tags = all_tags.join(other=tags_verified, on="tag", how="left_anti")
            self.logger.info("Successfully excluded.")
        except Exception as e:
            self.logger.exception("Unable to exclude tags!")
            raise e

        self.logger.info(
            f"Successfully got `tags` dataset from {date} date with {depth} days depth with {tags.count()} row in total!"
        )
        try:
            self.logger.info(
                f"Writing parquet -> {tgt_path}/date={date}/candidates-d{depth}"
            )
            tags.write.parquet(
                path=f"{tgt_path}/date={date}/candidates-d{depth}",
                mode="overwrite",
                compression="gzip",
            )
            self.logger.info("Dataset was successfully wrote.")
        except Exception as e:
            self.logger.exception("Unable to save dataset!")
            raise e

        self.logger.info("Stopping Spark Session.")
        self.spark.stop()


class DataMover:
    def __init__(self):
        self.logger = SparkLogger.get_logger(logger_name=str(Path(Path(__file__).name)))
        self.spark = SparkSession.builder.master("yarn").appName("APP").getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.logger.info("Initializing Spark Session.")

    def prepare_dataset(self):
        SRC_PATH = "s3a://data-ice-lake-04/messager-data/events"
        TGT_PATH = "s3a://data-ice-lake-04/messager-data/analytics/cleaned-events"

        self.logger.info("Reading raw data from s3.")

        df = self.spark.read.json(SRC_PATH)

        try:
            self.logger.info("Cleaning dataset.")
            df = (
                df.withColumn("admins", df.event.admins)
                .withColumn("channel_id", df.event.channel_id)
                .withColumn(
                    "datetime",
                    f.to_timestamp(col=df.event.datetime, format="yyyy-MM-dd HH:mm:ss"),
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
                    f.to_timestamp(
                        f.split(str=df.event.message_ts, pattern="\.").getItem(0),
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
                .withColumn("date", df.date)
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
                    "date",
                )
            )
            self.logger.info("Dataset was cleaned successfully.")

            self.logger.info(f"Data types of dataset is:\n")
            for _ in df.dtypes:
                print(_)

        except Exception as e:
            self.logger.exception("Unable to clean dataset! Something went wrong.")
            raise e

        try:
            self.logger.info("Writing cleaned dataset to s3.")
            df.write.parquet(
                path=TGT_PATH,
                mode="overwrite",
                partitionBy=["event_type", "date"],
                compression="gzip",
            )
            self.logger.info("Successfully wrote data to s3.")
        except Exception as e:
            self.logger.exception("Unable to write dataset to s3!")
            raise e
