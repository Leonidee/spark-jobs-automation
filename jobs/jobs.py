import os
import findspark
from typing import List
from pathlib import Path


os.environ["HADOOP_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr/bin/java"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/opt/conda/bin/python3"


findspark.init()
findspark.find()

from pyspark.sql import SparkSession
import pyspark.sql.functions as f


from jobs.utils import SparkLogger


class SparkKiller:
    def __init__(self, app_name: str):
        self.logger = SparkLogger.get_logger(logger_name=str(Path(Path(__file__).name)))
        self.spark = SparkSession.builder.master("yarn").appName(app_name).getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.logger.info("Initializing Spark Session.")

    def do_tags_job(
        self,
        date: str,
        depth: int,
        threshold: int,
        src_paths: List[str] | str,
        tags_verified_path: str,
        tgt_path: str,
    ) -> None:
        self.logger.info(f"Starting tags job.")
        try:
            self.logger.info("Getting `messages` dataset from s3.")
            messages = self.spark.read.parquet(*src_paths, compression="gzip")
            self.logger.info(f"Done. Rows in dataset {messages.count()}.")

            self.logger.info("Getting `tags_verified` dataset from s3.")
            tags_verified = self.spark.read.parquet(tags_verified_path)
            self.logger.info("Done.")

            self.logger.info("Preparing `tags_verified`.")
            tags_verified = (
                tags_verified.withColumn("tag", f.trim(tags_verified.tag))
                .select("tag")
                .distinct()
            )  # ? which exceptions can it raise?
            self.logger.info("Done.")

            self.logger.info("Grouping `messages` dataset.")
            all_tags = (
                messages.where(messages.message_channel_to.isNotNull())
                .withColumn("tag", f.explode(messages.tags))
                .select(f.col("message_from"), f.col("tag"))
                .groupBy(f.col("tag"))
                .agg(f.count_distinct(messages.message_from).alias("suggested_count"))
                .where(f.col("suggested_count") >= threshold)
            )
            self.logger.info(f"Done. Rows in dataset: {all_tags.count()}.")

            self.logger.info("Excluding verified tags.")
            tags = all_tags.join(other=tags_verified, on="tag", how="left_anti")
            self.logger.info(f"Done. Rows in dataset: {tags.count()}")

            self.logger.info(
                f"Writing parquet -> {tgt_path}/date={date}/candidates-d{depth}"
            )
            try:
                tags.write.parquet(
                    path=f"{tgt_path}/date={date}/candidates-d{depth}",
                    mode="errorifexists",
                    compression="gzip",
                )
                self.logger.info("Done.")
            except Exception:
                self.logger.warning(
                    "Notice that file already exists on s3 and will be overwritten!"
                )
                tags.write.parquet(
                    path=f"{tgt_path}/date={date}/candidates-d{depth}",
                    mode="overwrite",
                    compression="gzip",
                )
                self.logger.info("Done.")

        except Exception as e:
            self.logger.exception(e)

        finally:
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
