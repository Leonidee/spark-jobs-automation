import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, List
from pandas import read_csv

from pydantic import BaseModel
import findspark


os.environ["HADOOP_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr/bin/java"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/opt/conda/bin/python3"

findspark.init()
findspark.find()

# spark
from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.utils import (
    CapturedException,
    AnalysisException,
)
import pyspark.sql.functions as f
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType,
    FloatType,
    TimestampType,
)
from pyspark import StorageLevel

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

    def _get_src_paths(
        self,
        holder: Any,
    ) -> List[str]:
        self.logger.debug(f"Collecting src paths")

        date = datetime.strptime(holder.date, "%Y-%m-%d").date()

        paths = [
            f"{holder.src_path}/date=" + str(date - timedelta(days=i))
            for i in range(int(holder.depth))
        ]
        self.logger.debug(f"Done with {len(paths)} paths")

        return paths

    def init_session(
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

    def _get_coordinates_dataframe(self) -> DataFrame:
        self.logger.debug("Getting cities coordinates dataframe")

        self.logger.debug("Reading data from s3")
        df = read_csv(
            "https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv",
            delimiter=";",
        )

        self.logger.debug("Preparing dataframe")

        df.lat = df.lat.str.replace(",", ".").astype("float64")
        df.lng = df.lng.str.replace(",", ".").astype("float64")

        df = df.rename(
            columns={
                "id": "city_id",
                "city": "city_name",
                "lat": "city_lat",
                "lng": "city_lon",
            }
        )

        schema = StructType(
            [
                StructField("city_id", IntegerType(), nullable=False),
                StructField("city_name", StringType(), nullable=False),
                StructField("city_lat", FloatType(), nullable=False),
                StructField("city_lon", FloatType(), nullable=False),
            ]
        )
        sdf = self.spark.createDataFrame(df, schema=schema)

        self.logger.debug("Done")

        return sdf

    def compute_messages_city(self, holder: ArgsHolder) -> DataFrame:
        self.logger.debug("Computing city to each message")

        self.logger.debug("Getting input data")

        src_paths = self._get_src_paths(holder=holder)
        events_sdf = self.spark.read.parquet(*src_paths).where(
            "event_type == 'message'"
        )

        cities_coords_sdf = self._get_coordinates_dataframe()

        self.logger.debug("Preparing dataframe")

        messages_sdf = (
            events_sdf.where(events_sdf.event.message_from.isNotNull())
            .select(
                events_sdf.event.message_from.alias("user_id"),
                events_sdf.event.message_id.alias("message_id"),
                events_sdf.event.message_ts.alias("message_ts"),
                events_sdf.event.datetime.alias("datetime"),
                events_sdf.lat.alias("msg_lat"),
                events_sdf.lon.alias("msg_lon"),
            )
            .withColumn(
                "msg_ts",
                f.when(f.col("message_ts").isNotNull(), f.col("message_ts")).otherwise(
                    f.col("datetime")
                ),
            )
        )
        self.logger.debug("Getting main messages dataframe")
        self.logger.debug("Processing...")

        messages_sdf = (
            messages_sdf.crossJoin(cities_coords_sdf)
            .withColumn(
                "dlat", f.radians(f.col("msg_lat")) - f.radians(f.col("city_lat"))
            )
            .withColumn(
                "dlon", f.radians(f.col("msg_lon")) - f.radians(f.col("city_lon"))
            )
            .withColumn(
                "distance_a",
                f.sin(f.col("dlat") / 2) ** 2
                + f.cos(f.radians(f.col("city_lat")))
                * f.cos(f.radians(f.col("msg_lat")))
                * f.sin(f.col("dlon") / 2) ** 2,
            )
            .withColumn("distance_b", f.asin(f.sqrt(f.col("distance_a"))))
            .withColumn("distance", 2 * 6371 * f.col("distance_b"))
            .withColumn(
                "city_dist_rnk",
                f.row_number().over(
                    Window().partitionBy("message_id").orderBy(f.asc("distance"))
                ),
            )
            .where(f.col("city_dist_rnk") == 1)
            .select(
                "user_id",
                "message_id",
                f.col("msg_ts").cast(TimestampType()),
                "city_name",
            )
        )

        self.logger.debug("Persisting results")

        messages_sdf.repartition(1).write.parquet(
            "s3a://data-ice-lake-04/messager-data/tmp/messages_sdf_1", mode="overwrite"
        )

    def compute_act_and_home_city(self):
        self.logger.debug("Collecting act_city")

        messages_sdf = self.spark.read.parquet(
            "s3a://data-ice-lake-04/messager-data/tmp/messages_sdf_1"
        )

        # * travel_array and travel_count
        # travels = messages_sdf.withColumn(
        #     "prev_city",
        #     f.lag("city_name").over(
        #         Window().partitionBy("user_id").orderBy(f.asc("msg_ts"))
        #     ),
        # ).withColumn(
        #     "visit_flg",
        #     f.when(
        #         (f.col("city_name") != f.col("prev_city"))
        #         | (f.col("prev_city").isNull()),
        #         f.lit(1),
        #     ).otherwise(f.lit(0)),
        # ).where(
        #     f.col("visit_flg") == 1
        # ).groupby(
        #     "user_id"
        # ).agg(
        #     f.collect_list("city_name").alias("travel_array")
        # ).select(
        #     "user_id", "travel_array", f.size("travel_array").alias("travel_count")
        # )
        # todo сгруппировать по дням?
        messages_sdf.withColumn(
            "dense_rank",
            f.dense_rank().over(
                Window().partitionBy("user_id").orderBy(f.asc("msg_ts"))
            ),
        ).show(100, False)


# * act-city
#    messages_sdf.withColumn(
#             "act_city",
#             f.first("city_name", True).over(
#                 Window().partitionBy("user_id").orderBy(f.desc("msg_ts"))
#             ),


def main() -> None:
    holder = ArgsHolder(
        date="2022-03-12",
        depth=7,
        src_path="s3a://data-ice-lake-04/messager-data/analytics/geo-events",
    )
    try:
        spark = SparkRunner()
        spark.init_session(app_name="testing-app")
        spark.compute_act_and_home_city()

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
