import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, List

from pydantic import BaseModel
import boto3
import findspark
from botocore.exceptions import ClientError


os.environ["HADOOP_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr/bin/java"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/opt/conda/bin/python3"

findspark.init()
findspark.find()

from pyspark.sql import SparkSession, Window
from pyspark.sql.utils import (
    CapturedException,
    AnalysisException,
)  # todo add this exeptions to first tags job
from pyspark.sql.functions import (
    col,
    count_distinct,
    explode,
    split,
    to_timestamp,
    trim,
    count,
    desc,
    asc,
    rank,
    row_number,
    when,
    regexp_replace,
    first,
)

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.logger import SparkLogger
from src.utils import load_environment
from src.config import Config

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

    def run_new_job(self, holder: ArgsHolder) -> None:
        src_paths = self._get_src_paths(event_type="message", holder=holder)

        self._init_session(app_name="new-app")

        df = self.spark.read.parquet(*src_paths, compression="gzip")

        df = (
            df.where(df.message_channel_to.isNotNull())
            .withColumn("tag", explode(df.tags))
            .select(col("message_from").alias("user_id"), col("tag"), col("message_id"))
        )

        df = df.groupBy(col("user_id"), col("tag")).agg(
            count(col("message_id")).alias("tag_cnt")
        )

        w = (
            Window()
            .partitionBy(["user_id"])
            .orderBy(col("user_id").asc(), col("tag_cnt").desc(), col("tag").desc())
        )

        df = df.withColumn("tag_rank", row_number().over(w)).select("*")

        df = df.where(col("tag_rank") <= 3)

        df = df.withColumn(
            "tag_rank_name",
            when(df.tag_rank == 1, regexp_replace(df.tag_rank, "1", "tag_top_1"))
            .when(df.tag_rank == 2, regexp_replace(df.tag_rank, "2", "tag_top_2"))
            .when(df.tag_rank == 3, regexp_replace(df.tag_rank, "3", "tag_top_3")),
        )

        df = df.groupBy(col("user_id")).pivot("tag_rank_name").agg(first("tag"))

        df.repartition(1).write.parquet(
            "s3a://data-ice-lake-04/messager-data/analytics/tmp/tag_tops_05_04_1",
            mode="overwrite",
        )

    def run_likes_job(self, holder: ArgsHolder) -> None:
        src_paths_reaction = self._get_src_paths(event_type="reaction", holder=holder)

        src_paths_message = self._get_src_paths(event_type="message", holder=holder)

        self._init_session(app_name="new-app")

        reactions_df = self.spark.read.parquet(*src_paths_reaction, compression="gzip")
        messages_df = self.spark.read.parquet(*src_paths_message, compression="gzip")

        reactions_df = reactions_df.where(reactions_df.message_id.isNotNull()).select(
            col("reaction_from").alias("user_id"),
            col("reaction_type"),
            col("message_id"),
        )
        self.logger.debug("Reactions dataframe is below")

        reactions_df.show(100)

        self.logger.debug("Messages dataframe is below")

        messages_df = (
            messages_df.where(messages_df.message_channel_to.isNotNull())
            .withColumn("tag", explode(messages_df.tags))
            .select(col("message_id"), col("tag"))
        )

        messages_df.show(100)

        self.logger.debug("Joined dataframe is below")

        df = messages_df.join(other=reactions_df, on=["message_id"], how="inner")
        df.show(100)

        self.logger.debug("Likes dataframe is below")

        likes_df = (
            df.where(df.reaction_type == "like")
            .groupBy(df.user_id, df.tag)
            .agg(count("message_id").alias("likes_cnt"))
        )
        likes_df.show(100)

        w = (
            Window()
            .partitionBy(["user_id"])
            .orderBy(col("user_id").asc(), col("likes_cnt").desc(), col("tag").desc())
        )

        likes_df = (
            likes_df.withColumn("tag_rank", row_number().over(w))
            .select("*")
            .where(col("tag_rank") <= 3)
        )

        likes_df = likes_df.withColumn(
            "tag_rank_name",
            when(
                likes_df.tag_rank == 1,
                regexp_replace(likes_df.tag_rank, "1", "like_tag_top_1"),
            )
            .when(
                likes_df.tag_rank == 2,
                regexp_replace(likes_df.tag_rank, "2", "like_tag_top_2"),
            )
            .when(
                likes_df.tag_rank == 3,
                regexp_replace(likes_df.tag_rank, "3", "like_tag_top_3"),
            ),
        )

        likes_df = (
            likes_df.groupBy(col("user_id")).pivot("tag_rank_name").agg(first("tag"))
        )

        logger.debug("`likes` dataframe results below")
        likes_df.show(100)

        dislikes_df = (
            df.where(df.reaction_type == "dislike")
            .groupBy(df.user_id, df.tag)
            .agg(count("message_id").alias("dislikes_cnt"))
        )
        w = (
            Window()
            .partitionBy(["user_id"])
            .orderBy(
                col("user_id").asc(), col("dislikes_cnt").desc(), col("tag").desc()
            )
        )

        dislikes_df = (
            dislikes_df.withColumn("tag_rank", row_number().over(w))
            .select("*")
            .where(col("tag_rank") <= 3)
        )
        dislikes_df = dislikes_df.withColumn(
            "tag_rank_name",
            when(
                dislikes_df.tag_rank == 1,
                regexp_replace(dislikes_df.tag_rank, "1", "dislike_tag_top_1"),
            )
            .when(
                dislikes_df.tag_rank == 2,
                regexp_replace(dislikes_df.tag_rank, "2", "dislike_tag_top_2"),
            )
            .when(
                dislikes_df.tag_rank == 3,
                regexp_replace(dislikes_df.tag_rank, "3", "dislike_tag_top_3"),
            ),
        )

        logger.debug("`dislikes` dataframe results below")
        dislikes_df = (
            dislikes_df.groupBy(col("user_id")).pivot("tag_rank_name").agg(first("tag"))
        )
        dislikes_df.show(100)

        result_df = likes_df.join(dislikes_df, how="inner", on="user_id")

        logger.debug("Resulting dataframe below")
        result_df.show(100)


def main() -> None:
    holder = ArgsHolder(
        date="2022-05-04",
        depth=10,
        src_path="s3a://data-ice-lake-04/messager-data/analytics/cleaned-events",
    )
    try:
        spark = SparkRunner()
        # spark.run_new_job(holder=holder)
        spark.run_likes_job(holder=holder)
    except (CapturedException, AnalysisException) as e:
        logger.exception(e)
        sys.exit(1)

    finally:
        spark.stop_session()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
