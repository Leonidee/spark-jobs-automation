import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Literal

import boto3
import findspark
from botocore.exceptions import ClientError
from pydantic import BaseModel

os.environ["HADOOP_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr/bin/java"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/opt/conda/bin/python3"

findspark.init()
findspark.find()

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    array,
    asc,
    col,
    count,
    count_distinct,
    desc,
    explode,
    first,
    rank,
    regexp_replace,
    row_number,
    split,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.utils import (  # todo add this exeptions to first tags job
    AnalysisException,
    CapturedException,
)

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.logger import SparkLogger
from src.utils import load_environment

load_environment()

config = Config()

logger = SparkLogger(level=config.log_level).get_logger(
    logger_name=str(Path(Path(__file__).name))
)


class ArgsHolder(BaseModel):
    date: str
    depth: int
    src_path: str


class SparkRunner:
    def __init__(self) -> None:
        """Main data processor class"""
        self.logger = SparkLogger(level=config.log_level).get_logger(
            logger_name=str(Path(Path(__file__).name))
        )

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
    ) -> List[str]:
        """Get S3 paths contains dataset partitions

        Collects paths corresponding to the passed in `holder` object arguments and checks if each path exists on S3. Collects only existing paths.
        If no paths for given arguments raise `SystemExit` with 1 code

        Args:
            event_type (Literal[message, reaction, subscription]): Event type for partition
            holder (Any): `pydantic.BaseModel` inherited  object with arguments for submiting Spark Job

        Returns:
            collections.List[str]: List of S3 paths
        """
        s3 = self._get_s3_instance()

        self.logger.info(f"Collecting src paths for {event_type} event type")

        date = datetime.strptime(holder.date, "%Y-%m-%d").date()

        paths = [
            f"{holder.src_path}/event_type={event_type}/date="
            + str(date - timedelta(days=i))
            for i in range(int(holder.depth))
        ]

        self.logger.info("Checking if each path exists on s3")

        existing_paths = []
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
        if existing_paths == []:
            self.logger.error("There is no data for given arguments!")
            sys.exit(1)

        self.logger.info(f"Done with {len(existing_paths)} paths in total")

        return existing_paths

    def _init_session(
        self,
        app_name: str,
        log4j_level: Literal[
            "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"
        ] = "WARN",
    ) -> None:
        """Configure and initialize Spark Session

        Args:
            app_name (str): Name of Spark Application
            log4j_level (str): Spark Context logging level. Defaults to `WARN`
        """
        self.logger.info("Initializing Spark Session")

        self.spark = SparkSession.builder.master("yarn").appName(app_name).getOrCreate()
        self.spark.sparkContext.setLogLevel(log4j_level)

        self.logger.info(f"Log4j level set to {log4j_level}")

    def stop_session(self) -> None:
        """Stop active Spark Session"""
        self.logger.info("Stopping Spark Session")
        self.spark.stop()
        self.logger.info("Session stopped")

    def calculate_user_interests(self, holder: ArgsHolder) -> None:
        self.logger.info("Executing `calculate_user_interests` job")

        self._init_session(app_name="new-app")

        # this window used for all dataframes
        w = (
            Window()
            .partitionBy(["user_id"])
            .orderBy(col("user_id").asc(), col("cnt").desc(), col("tag").desc())
        )

        # top_tags dataframe operations
        self.logger.debug("Reading data for `top_tags` frame")
        top_tags_src_paths = self._get_src_paths(event_type="message", holder=holder)
        top_tags_df = self.spark.read.parquet(*top_tags_src_paths, compression="gzip")
        # self.logger.debug(f"Done with {top_tags_df.count()} rows")

        self.logger.debug("Preparing")
        top_tags_df = (
            top_tags_df.where(top_tags_df.message_channel_to.isNotNull())
            .withColumn("tag", explode(top_tags_df.tags))
            .select(col("message_from").alias("user_id"), col("tag"), col("message_id"))
        )

        self.logger.debug("Collecting `top_tags` frame")
        top_tags_df = (
            top_tags_df.groupBy(col("user_id"), col("tag"))
            .agg(count(col("message_id")).alias("cnt"))
            .withColumn("tag_rank", row_number().over(w))
            .where(col("tag_rank") <= 3)
            .groupBy(col("user_id"))
            .pivot("tag_rank", [1, 2, 3])
            .agg(first("tag"))
            .withColumnRenamed("1", "tag_top_1")
            .withColumnRenamed("2", "tag_top_2")
            .withColumnRenamed("3", "tag_top_3")
        )
        # self.logger.debug(f"Done with {top_tags_df.count()} rows")

        # reactions dataframe operations
        self.logger.debug("Reading data for `reactions` frame")
        reaction_src_paths = self._get_src_paths(event_type="reaction", holder=holder)
        reactions_df = self.spark.read.parquet(*reaction_src_paths, compression="gzip")
        # self.logger.debug(f"Done with {reactions_df.count()} rows")

        self.logger.debug("Reading  data for `all_messages` frame")
        all_messages_df = self.spark.read.parquet(
            "s3a://data-ice-lake-04/messager-data/analytics/cleaned-events/event_type=message",
            compression="gzip",
        )
        # self.logger.debug(f"Done with {all_messages_df.count()} rows")

        self.logger.debug("Preparing `all_messages` frame")
        all_messages_df = (
            all_messages_df.where(all_messages_df.message_channel_to.isNotNull())
            .withColumn("tag", explode(all_messages_df.tags))
            .select(col("message_id"), col("tag"))
        )

        self.logger.debug("Joining `reactions` and `all_messages` frames")
        df = (
            reactions_df.where(reactions_df.message_id.isNotNull())
            .join(other=all_messages_df, on="message_id", how="inner")
            .select(
                col("reaction_from").alias("user_id"),
                col("reaction_type"),
                col("message_id"),
                col("tag"),
            )
        )
        # self.logger.debug(f"Done with {df.count()} rows")

        self.logger.debug("Collecting `likes` frame")

        likes_df = (
            df.where(df.reaction_type == "like")
            .groupBy(df.user_id, df.tag)
            .agg(count("*").alias("cnt"))
            .withColumn(
                "tag_rank",
                row_number().over(w),
            )
            .where(col("tag_rank") <= 3)
            .groupBy(col("user_id"))
            .pivot("tag_rank", [1, 2, 3])
            .agg(first("tag"))
            .withColumnRenamed("1", "like_tag_top_1")
            .withColumnRenamed("2", "like_tag_top_2")
            .withColumnRenamed("3", "like_tag_top_3")
        )

        # self.logger.debug(f"Done with {likes_df.count()} rows")

        self.logger.debug("Collecting `dislikes` frame")

        dislikes_df = (
            df.where(df.reaction_type == "dislike")
            .groupBy(df.user_id, df.tag)
            .agg(count("*").alias("cnt"))
            .withColumn(
                "tag_rank",
                row_number().over(w),
            )
            .where(col("tag_rank") <= 3)
            .groupBy(col("user_id"))
            .pivot("tag_rank", [1, 2, 3])
            .agg(first("tag"))
            .withColumnRenamed("1", "dislike_tag_top_1")
            .withColumnRenamed("2", "dislike_tag_top_2")
            .withColumnRenamed("3", "dislike_tag_top_3")
        )

        # self.logger.debug(f"Done with {dislikes_df.count()} rows")

        # resulting frame operations
        self.logger.debug("Joining all frames")

        result_df = top_tags_df.join(likes_df, how="outer", on="user_id").join(
            dislikes_df, how="outer", on="user_id"
        )
        result_df.show(10)
        # self.logger.debug(f"Done with {result_df.count()} rows")

        # result_df.explain(extended=True)

        # self.logger.info("Writing results on S3")
        # result_df.repartition(1).write.parquet(
        #     f"s3a://data-ice-lake-04/messager-data/analytics/tmp/user_interests_d{holder.depth}",
        #     mode="overwrite",
        # )
        # self.logger.info(f"Done")


def main() -> None:
    holder = ArgsHolder(
        date="2022-05-25",
        depth=7,
        src_path="s3a://data-ice-lake-04/messager-data/analytics/cleaned-events",
    )
    try:
        spark = SparkRunner()
        spark.calculate_user_interests(holder=holder)
    except (CapturedException, AnalysisException) as e:
        logger.exception(e)
        sys.exit(1)

    # finally:
    # spark.stop_session()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
