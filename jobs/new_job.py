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

from pyspark.sql import SparkSession
from pyspark.sql.utils import CapturedException
from pyspark.sql.functions import (
    col,
    count_distinct,
    explode,
    split,
    to_timestamp,
    trim,
    count,
    desc,
)


sys.path.append(str(Path(__file__).parent.parent))
from src.logger import SparkLogger
from src.utils import load_environment

load_environment()

logger = SparkLogger(level="DEBUG").get_logger(
    logger_name=str(Path(Path(__file__).name))
)


class ArgsHolder(BaseModel):
    date: str
    depth: int
    src_path: str


class SparkRunner:
    def __init__(self) -> None:
        """Main data processor class"""
        self.logger = SparkLogger(level="DEBUG").get_logger(
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

        # df = (
        #     df.where(df.message_channel_to.isNotNull())
        #     .withColumn("tag", explode(df.tags))
        #     .select(col("message_from"), col("tag"), col("message_id"))
        # )

        # df.groupBy(col("message_from"), col("tag")).agg(
        #     count(col("message_id")).alias("suggested_count")
        # ).orderBy(df.message_from, desc(col("suggested_count"))).show(100)

        df.repartition(1).write.parquet(
            "s3a://data-ice-lake-04/messager-data/analytics/tmp/wide-dataframe",
            mode="overwrite",
        )


def main() -> None:
    holder = ArgsHolder(
        date="2022-06-04",
        depth=50,
        src_path="s3a://data-ice-lake-04/messager-data/analytics/cleaned-events",
    )
    try:
        spark = SparkRunner()
        spark.run_new_job(holder=holder)
    except CapturedException as e:
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
