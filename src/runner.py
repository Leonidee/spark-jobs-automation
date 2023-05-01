import os
import sys
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import boto3
import findspark
from botocore.exceptions import ClientError

from src.logger import SparkLogger
from src.utils import SparkArgsValidator, TagsJobArgsHolder, load_environment

os.environ["HADOOP_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr/bin/java"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/opt/conda/bin/python3"

findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count_distinct,
    explode,
    split,
    to_timestamp,
    trim,
)

load_environment()


class SparkRunner:
    def __init__(self) -> None:
        """Main data processor class"""
        self.logger = SparkLogger().get_logger(
            logger_name=str(Path(Path(__file__).name))
        )
        self.validator = SparkArgsValidator()

    def _get_s3_instance(self):
        "Get boto3 S3 connection instance"

        self.logger.info("Getting s3 connection instace")

        s3 = boto3.session.Session().client(
            service_name="s3",
            endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

        return s3

    def _get_src_paths(
        self,
        event_type: Literal["message", "reaction", "subscription"],
        holder: Any,
    ) -> deque:
        """Get S3 paths contains dataset partitions

        Collects paths corresponding to the passed in `holder` object arguments and checks if each path exists on S3. Collects only existing paths.
        If no paths for given arguments raise `SystemExit` with 1 code

        Args:
            event_type (Literal[message, reaction, subscription]): Event type for partition
            holder (Any): `pydantic.BaseModel` like object with Spark Job arguments

        Returns:
            collections.deque[str]: Queue of S3 path
        """
        s3 = self._get_s3_instance()

        self.logger.info("Collecting src paths")

        date = datetime.strptime(holder.date, "%Y-%m-%d").date()

        paths = [
            f"{holder.src_path}/event_type={event_type}/date="
            + str(date - timedelta(days=i))
            for i in range(int(holder.depth))
        ]

        self.logger.info("Checking if each path exists on s3")

        existing_paths = deque()
        for path in paths:
            try:
                response = s3.list_objects(
                    Bucket=path.split(sep="/")[2],
                    MaxKeys=5,
                    Prefix="/".join(path.split(sep="/")[3:]),
                )
                if "Contents" in response.keys():
                    existing_paths.append(path)
                else:
                    self.logger.info(f"No data for `{path}` on s3. Skipping")
            except ClientError as e:
                self.logger.exception(e)
                sys.exit(1)

        # if returns empty queue - exit
        if existing_paths == deque():
            self.logger.error("There is no data for given arguments!")
            sys.exit(1)

        self.logger.info(f"Done with {len(existing_paths)} paths in total")

        return existing_paths

    def _init_session(self, app_name: str) -> None:
        """Configure and initialize Spark Session

        Args:
            app_name (str): Name of Spark Application
        """
        self.logger.info("Initializing Spark Session")

        self.spark = SparkSession.builder.master("yarn").appName(app_name).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def stop_session(self) -> None:
        """Stop active Spark Session"""
        self.logger.info("Stopping Spark Session")
        self.spark.stop()
        self.logger.info("Session stopped")

    def run_tags_job(
        self,
        holder: TagsJobArgsHolder,
    ) -> None:
        """Run `tags-job`

        Args:
            holder (TagsJobArgsHolder): `TagsJobArgsHolder` with Spark Job arguments
        """
        try:
            self.validator.validate_tags_job_args(holder=holder)
        except AssertionError as e:
            self.logger.exception(e)
            sys.exit(1)

        src_paths = self._get_src_paths(event_type="message", holder=holder)

        self._init_session(app_name="TAGS-APP")

        self.logger.info(f"Starting `tags` job")

        self.logger.info("Getting `messages` dataset from s3")
        messages = self.spark.read.parquet(*src_paths, compression="gzip")
        self.logger.info(f"Done. Rows in dataset {messages.count()}.")

        self.logger.info("Getting `tags_verified` dataset from s3")
        tags_verified = self.spark.read.parquet(holder.tags_verified_path)
        self.logger.info("Done")

        self.logger.info("Preparing `tags_verified`")
        tags_verified = (
            tags_verified.withColumn("tag", trim(tags_verified.tag))
            .select("tag")
            .distinct()
        )
        self.logger.info("Done")

        self.logger.info("Grouping `messages` dataset")
        all_tags = (
            messages.where(messages.message_channel_to.isNotNull())
            .withColumn("tag", explode(messages.tags))
            .select(col("message_from"), col("tag"))
            .groupBy(col("tag"))
            .agg(count_distinct(messages.message_from).alias("suggested_count"))
            .where(col("suggested_count") >= holder.threshold)
        )
        self.logger.info(f"Done. Rows in dataset: {all_tags.count()}")

        self.logger.info("Excluding verified tags.")
        tags = all_tags.join(other=tags_verified, on="tag", how="left_anti")
        self.logger.info(f"Done. Rows in dataset: {tags.count()}")

        self.logger.info(
            f"Writing parquet -> {holder.tgt_path}/date={holder.date}/candidates-d{holder.depth}"
        )
        try:
            tags.repartition(1).write.parquet(
                path=f"{holder.tgt_path}/date={holder.date}/candidates-d{holder.depth}",
                mode="errorifexists",
                compression="gzip",
            )
            self.logger.info("Done")
        except Exception:
            self.logger.warning(
                "Notice that file already exists on s3 and will be overwritten!"
            )
            tags.repartition(1).write.parquet(
                path=f"{holder.tgt_path}/date={holder.date}/candidates-d{holder.depth}",
                mode="overwrite",
                compression="gzip",
            )
            self.logger.info("Done")

    def prepare_dataset(self, src_path: str, tgt_path: str):
        self.logger.info("Starting dataset preperation")

        self.logger.info("Reading raw data from s3")
        df = self.spark.read.json(src_path)

        self.logger.info("Cleaning dataset")
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
        self.logger.info(f"Done. Rows in dataset: {df.count()}")

        self.logger.info("Writing cleaned dataset to s3.")
        df.write.parquet(
            path=tgt_path,
            mode="overwrite",
            partitionBy=["event_type", "date"],
            compression="gzip",
        )
        self.logger.info("Done")
