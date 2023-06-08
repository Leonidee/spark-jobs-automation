from __future__ import annotations

import sys
from pathlib import Path

import findspark

from typing import TYPE_CHECKING

# package
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.helper import SparkHelper
from src.logger import SparkLogger

if TYPE_CHECKING:
    from typing import Literal

    from src.keeper import SparkConfigKeeper


class SparkRuntimeError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class SparkRunner(SparkHelper):
    """Data processor and datamarts collector class.

    ## Examples
    Initialize `SparkRunner` class object:
    >>> spark = SparkRunner()

    Start session:
    >>> spark.init_session(app_name="test-app", spark_conf=conf, log4j_level="INFO")

    And now execute chosen job:
    >>> spark.collect_friend_recommendation_datamart(keeper=keeper)

    Don't forget to stop session:
    >>> spark.stop_session()
    """

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
        """Configure and initialize Spark Session

        ## Parameters
        `app_name` : Name of Spark application
        `spark_conf` : `SparkConfigKeeper` object with Spark configuration properties
        `log4j_level` : Spark Context Java logging level, by default "WARN"
        """
        self.logger.info("Initializing Spark Session")

        findspark.init(spark_home=self.SPARK_HOME, python_path=self.PYTHONPATH)
        findspark.find()

        from pyspark.sql import SparkSession  # type: ignore

        self.spark = (
            SparkSession.builder.master("yarn")
            .config("spark.hadoop.fs.s3a.access.key", self.AWS_ACCESS_KEY_ID)
            .config("spark.hadoop.fs.s3a.secret.key", self.AWS_SECRET_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.endpoint", self.AWS_ENDPOINT_URL)
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.executor.memory", spark_conf.executor_memory)
            .config("spark.executor.cores", str(spark_conf.executor_cores))
            .config(
                "spark.dynamicAllocation.maxExecutors",
                str(spark_conf.max_executors_num),
            )
            .appName(app_name)
            .getOrCreate()
        )

        self.logger.info(f"Spark job properties:\n{spark_conf}")

        self.spark.sparkContext.setLogLevel(log4j_level)

        self.logger.info(f"Log4j level: '{log4j_level}'")

    def stop_session(self) -> None:
        """Stop active Spark Session"""

        self.logger.info("Stopping Spark Session")

        self.spark.stop()

        self.logger.info("Session stopped")
