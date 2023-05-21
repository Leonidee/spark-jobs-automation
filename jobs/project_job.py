import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Literal, Union

import findspark
from pandas import read_csv
from pydantic import BaseModel

os.environ["HADOOP_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr/bin/java"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/opt/conda/bin/python3"

findspark.init()
findspark.find()

import pyspark.sql.functions as f

# spark
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.utils import AnalysisException, CapturedException

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
import pandas as pd

pd.DataFrame()


class ArgsHolder(BaseModel):
    date: str
    depth: int
    src_path: str
    tgt_path: str
    processed_dttm: str


def t(d: str, b: BaseModel) -> dict:
    """Summary now too long and not too short

    Parameters
    ----------
    d
        Some text here
    b
        Another desctiprion

    Returns
    -------
        dict with bla-bla-bla

    Raises
    ------
    AnalysisException
        Some ezeprion
    """
    ...


def new_test(logger_name: str, model: BaseModel, level: str = "INFO") -> pd.DataFrame:
    """_summary_

    ## Parameters
    logger_name : _description_
    model : _description_
    level : _description_, by default "INFO"

    ## Returns
    `pd.DataFrame` : _description_

    ## Raises
    `AnalysisException` : _description_
    """

    df = pd.DataFrame(data={"a": [1, 2, 5, 6]})
    print(level)

    if not model:
        raise AnalysisException(logger_name)
    return df


class SparkRunner:
    def __init__(self) -> None:
        """Main data processor class"""
        self.logger = SparkLogger(level=config.log_level).get_logger(
            logger_name=str(Path(Path(__file__).name))
        )

    def _get_src_paths(
        self,
        holder: ArgsHolder,
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

        ## Args:
            app_name (str): Name of Spark Application
            log4j_level (str): Spark Context logging level. Defaults to `WARN`
        """
        self.logger.info("Initializing Spark Session")

        self.spark = SparkSession.builder.master("yarn").appName(app_name).getOrCreate()
        self.spark.sparkContext.setLogLevel(log4j_level)

        self.logger.info(f"Log4j level set to: {log4j_level}")

    def stop_session(self) -> None:
        """Stop active Spark Session"""
        self.logger.info("Stopping Spark Session")
        self.spark.stop()
        self.logger.info("Session stopped")

    def _get_cities_coordinates_dataframe(self) -> DataFrame:
        """
        Get dataframe with coordinates of each city

        ## Returns:
            `pyspark.sql.DataFrame`
        """
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
        self.logger.debug("Creating pyspark.sql.DataFrame")
        sdf = self.spark.createDataFrame(df, schema=schema)

        sdf.show()

    def _compute_distance(
        self, dataframe: DataFrame, coord_cols_prefix: List[str]
    ) -> DataFrame:
        "Compute distance between two point row-wise on `pyspark.sql.DataFrame`"

        self.logger.debug("Computing distances")

        self.logger.debug("Collecting columns with coordinates")
        cols = [(i + "_lat", i + "_lon") for i in coord_cols_prefix]
        lat_1, lon_1 = cols[0]
        lat_2, lon_2 = cols[1]

        self.logger.debug("Checking if each columns exists in dataframe")
        if not all(col in dataframe.columns for col in (lat_1, lon_1, lat_2, lon_2)):
            raise AnalysisException(
                "DataFrame should contains coordinates columns with names listed in `coord_cols_prefix` argument"
            )
        self.logger.debug("OK")

        self.logger.debug("Processing...")
        sdf = (
            dataframe.withColumn(
                "dlat", f.radians(f.col(lat_2)) - f.radians(f.col(lat_1))
            )
            .withColumn("dlon", f.radians(f.col(lon_2)) - f.radians(f.col(lon_1)))
            .withColumn(
                "distance_a",
                f.sin(f.col("dlat") / 2) ** 2
                + f.cos(f.radians(f.col(lat_1)))
                * f.cos(f.radians(f.col(lat_2)))
                * f.sin(f.col("dlon") / 2) ** 2,
            )
            .withColumn("distance_b", f.asin(f.sqrt(f.col("distance_a"))))
            .withColumn("distance_c", 2 * 6371 * f.col("distance_b"))
            .withColumn("distance", f.round(f.col("distance_c"), 0))
            .drop("dlat", "dlon", "distance_a", "distance_b", "distance_c")
        )

        return sdf

    def _get_event_location(
        self,
        dataframe: DataFrame,
        event_type: Literal["message", "reaction", "subscription", "registration"],
    ) -> DataFrame:
        """
        Gets city to each event of given dataframe

        ## Args:
            dataframe (`pyspark.sql.DataFrame`)
            event_type (`Literal[str]`)

        ## Returns:
            `pyspark.sql.DataFrame`
        """
        self.logger.debug(f"Getting location of `{event_type}` event type")

        cities_coords_sdf = self._get_cities_coordinates_dataframe()

        partition_by = (
            ["user_id", "subscription_channel"]
            if event_type == "subscription"
            else "message_id"
        )
        self.logger.debug(
            "Joining given dataframe with cities coordinates. Processing..."
        )
        sdf = dataframe.crossJoin(cities_coords_sdf)

        sdf = self._compute_distance(
            dataframe=sdf,
            coord_cols_prefix=["event", "city"],
        )
        self.logger.debug("Collecting resulting dataframe")
        sdf = (
            sdf.withColumn(
                "city_dist_rnk",
                f.row_number().over(
                    Window().partitionBy(partition_by).orderBy(f.asc("distance"))
                ),
            )
            .where(f.col("city_dist_rnk") == 1)
            .drop(
                "city_lat",
                "city_lon",
                "distance",
                "city_dist_rnk",
            )
        )

        return sdf

    def _get_users_actual_data_dataframe(self, holder: ArgsHolder) -> DataFrame:
        """
        Returns dataframe with user information based on sent messages

        ## Args:
            holder (`ArgsHolder`)

        ## Returns:
            `pyspark.sql.DataFrame`: DataFrame with columns:`user_id`, `message_id`, `msg_ts`, `city_name`, `act_city`, `act_city_id`, `local_time`
        """
        self.logger.debug(f"Getting input data from: {holder.src_path}")

        # todo uncomment this
        # src_paths = self._get_src_paths(holder=holder)
        # events_sdf = self.spark.read.parquet(*src_paths)

        # todo delete thiw
        events_sdf = self.spark.read.parquet(
            "s3a://data-ice-lake-04/messager-data/tmp/step-two-01"
        )

        self.logger.debug("Collecting dataframe")
        sdf = (
            events_sdf.where(events_sdf.event_type == "message")
            .where(events_sdf.event.message_from.isNotNull())
            .select(
                events_sdf.event.message_from.alias("user_id"),
                events_sdf.event.message_id.alias("message_id"),
                events_sdf.event.message_ts.alias("message_ts"),
                events_sdf.event.datetime.alias("datetime"),
                events_sdf.lat.alias("event_lat"),
                events_sdf.lon.alias("event_lon"),
            )
            .withColumn(
                "msg_ts",
                f.when(f.col("message_ts").isNotNull(), f.col("message_ts")).otherwise(
                    f.col("datetime")
                ),
            )
            .drop("message_ts", "datetime")
        )

        sdf = self._get_event_location(dataframe=sdf, event_type="message")

        self.logger.debug("Collecting resulting dataframe")

        sdf = (
            sdf.withColumn(
                "act_city",
                f.first(col="city_name", ignorenulls=True).over(
                    Window().partitionBy("user_id").orderBy(f.desc("msg_ts"))
                ),
            )
            .withColumn(
                "act_city_id",
                f.first(col="city_id", ignorenulls=True).over(
                    Window().partitionBy("user_id").orderBy(f.desc("msg_ts"))
                ),
            )
            .withColumn(
                "last_msg_ts",
                f.first(col="msg_ts", ignorenulls=True).over(
                    Window().partitionBy("user_id").orderBy(f.desc("msg_ts"))
                ),
            )
            .withColumn(
                "local_time",
                f.from_utc_timestamp(
                    timestamp=f.col("last_msg_ts"), tz="Australia/Sydney"
                ),
            )
            .select(
                "user_id",
                "message_id",
                "msg_ts",
                "city_name",
                "act_city",
                "act_city_id",
                "local_time",
            )
        )

        return sdf

    def compute_users_info_datamart(self, holder: ArgsHolder) -> None:
        """
        Compute and save user info datamart

        ## Args:
            holder (`ArgsHolder`)
        """
        self.logger.info("Starting collecting user info datamart")

        sdf = self._get_users_actual_data_dataframe(holder=holder)

        self.logger.debug("Collecting dataframe with travels data. Processing...")
        travels_sdf = (
            sdf.withColumn(
                "prev_city",
                f.lag("city_name").over(
                    Window().partitionBy("user_id").orderBy(f.asc("msg_ts"))
                ),
            )
            .withColumn(
                "visit_flg",
                f.when(
                    (f.col("city_name") != f.col("prev_city"))
                    | (f.col("prev_city").isNull()),
                    f.lit(1),
                ).otherwise(f.lit(0)),
            )
            .where(f.col("visit_flg") == 1)
            .groupby("user_id")
            .agg(
                f.collect_list("city_name").alias("travel_array"),
                f.collect_list("msg_ts").alias("travel_ts_array"),
            )
            .select(
                "user_id",
                "travel_array",
                f.size("travel_array").alias("travel_count"),
                "travel_ts_array",
            )
        )

        home_city_sdf = (
            travels_sdf.withColumn(
                "zipped_array", f.arrays_zip("travel_array", "travel_ts_array")
            )
            .withColumn("upzipped_array", f.explode("zipped_array"))
            .withColumn("travel_city", f.col("upzipped_array").getItem("travel_array"))
            .withColumn("travel_ts", f.col("upzipped_array").getItem("travel_ts_array"))
            .withColumn(
                "prev_travel_ts",
                f.lag("travel_ts").over(
                    Window().partitionBy("user_id").orderBy(f.asc("travel_ts"))
                ),
            )
            .withColumn(
                "prev_travel_city",
                f.lag("travel_city").over(
                    Window().partitionBy("user_id").orderBy(f.asc("travel_ts"))
                ),
            )
            .withColumn("diff", f.datediff("travel_ts", "prev_travel_ts"))
            .where(f.col("diff") > 27)
            .withColumn(
                "rnk",
                f.row_number().over(
                    Window().partitionBy("user_id").orderBy(f.desc("travel_ts"))
                ),
            )
            .where(f.col("rnk") == 1)
            .select("user_id", f.col("prev_travel_city").alias("home_city"))
        )

        self.logger.debug("Collecting resulting dataframe")

        sdf = (
            sdf.drop_duplicates(subset=["user_id"])
            .join(travels_sdf, how="left", on="user_id")
            .join(home_city_sdf, how="left", on="user_id")
            .select(
                "user_id",
                "act_city",
                "home_city",
                "local_time",
                "travel_count",
                "travel_array",
            )
        )

        self.logger.info("Datamart collected")
        self.logger.info("Writing results")

        processed_dt = datetime.strptime(
            holder.processed_dttm.replace("T", " "), "%Y-%m-%d %H:%M:%S.%f"
        ).date()

        try:
            sdf.repartition(1).write.parquet(
                path=f"{holder.tgt_path}/date={processed_dt}",
                mode="errorifexists",
            )
            self.logger.info(f"Done! Results -> {holder.tgt_path}/date={processed_dt}")
        except Exception:
            self.logger.warning(
                "Notice that target path is already exists and will be overwritten!"
            )
            self.logger.info("Overwriting...")
            sdf.repartition(1).write.parquet(
                path=f"{holder.tgt_path}/date={processed_dt}",
                mode="overwrite",
            )
            self.logger.info(f"Done! Results -> {holder.tgt_path}/date={processed_dt}")

    def compute_location_zone_agg_datamart(self, holder: ArgsHolder) -> None:
        """Compute and save location zone aggregeted datamart

        Args:
            holder (`ArgsHolder`)
        """
        self.logger.info("Staring collecting location zone aggregated datamart")

        self.logger.debug(f"Getting input data from: {holder.src_path}")

        src_paths = self._get_src_paths(holder=holder)
        events_sdf = self.spark.read.parquet(*src_paths)

        self.logger.debug("Collecing messages dataframe. Processing...")

        # сборка messages
        messages_sdf = (
            events_sdf.where(events_sdf.event_type == "message")
            .where(events_sdf.event.message_from.isNotNull())
            .select(
                events_sdf.event.message_from.alias("user_id"),
                events_sdf.event.message_id.alias("message_id"),
                events_sdf.event.message_ts.alias("message_ts"),
                events_sdf.event.datetime.alias("datetime"),
                events_sdf.lat.alias("event_lat"),
                events_sdf.lon.alias("event_lon"),
            )
            .withColumn(
                "msg_ts",
                f.when(f.col("message_ts").isNotNull(), f.col("message_ts")).otherwise(
                    f.col("datetime")
                ),
            )
            .drop_duplicates(subset=["user_id", "message_id", "msg_ts"])
            .drop("datetime", "message_ts")
        )
        messages_sdf = self._get_event_location(
            dataframe=messages_sdf, event_type="message"
        )

        messages_sdf = (
            messages_sdf.withColumnRenamed("city_id", "zone_id")
            .withColumn("week", f.weekofyear(f.col("msg_ts").cast("timestamp")))
            .withColumn("month", f.month(f.col("msg_ts").cast("timestamp")))
            .groupby("month", "week", "zone_id")
            .agg(f.count("message_id").alias("week_message"))
            .withColumn(
                "month_message",
                f.sum(f.col("week_message")).over(
                    Window().partitionBy(f.col("zone_id"), f.col("month"))
                ),
            )
        )

        self.logger.debug("Collecing reacitons dataframe. Processing...")

        # сборка reactions
        reaction_sdf = (
            events_sdf.where(events_sdf.event_type == "reaction")
            .select(
                events_sdf.event.datetime.alias("datetime"),
                events_sdf.event.message_id.alias("message_id"),
                events_sdf.event.reaction_from.alias("user_id"),
                events_sdf.lat.alias("event_lat"),
                events_sdf.lon.alias("event_lon"),
            )
            .drop_duplicates(subset=["user_id", "message_id", "datetime"])
            .where(f.col("event_lat").isNotNull())
        )
        reaction_sdf = self._get_event_location(
            dataframe=reaction_sdf, event_type="reaction"
        )
        reaction_sdf = (
            reaction_sdf.withColumnRenamed("city_id", "zone_id")
            .where(f.col("event_lat").isNotNull())
            .withColumn("week", f.weekofyear(f.col("datetime").cast("timestamp")))
            .withColumn("month", f.month(f.col("datetime").cast("timestamp")))
            .groupby("month", "week", "zone_id")
            .agg(f.count("message_id").alias("week_reaction"))
            .withColumn(
                "month_reaction",
                f.sum(f.col("week_reaction")).over(
                    Window().partitionBy(f.col("zone_id"), f.col("month"))
                ),
            )
        )

        self.logger.debug("Collecing registrations dataframe. Processing...")

        # сборка registrations
        registrations_sdf = (
            events_sdf.where(events_sdf.event_type == "message")
            .where(events_sdf.event.message_from.isNotNull())
            .select(
                events_sdf.event.message_from.alias("user_id"),
                events_sdf.event.message_id.alias("message_id"),
                events_sdf.event.message_ts.alias("message_ts"),
                events_sdf.event.datetime.alias("datetime"),
                events_sdf.lat.alias("event_lat"),
                events_sdf.lon.alias("event_lon"),
            )
            .withColumn(
                "msg_ts",
                f.when(f.col("message_ts").isNotNull(), f.col("message_ts")).otherwise(
                    f.col("datetime")
                ),
            )
            .drop_duplicates(subset=["user_id", "message_id", "msg_ts"])
            .drop("datetime", "message_ts")
            .withColumn(
                "registration_ts",
                f.first(col="msg_ts", ignorenulls=True).over(
                    Window().partitionBy("user_id").orderBy(f.asc("msg_ts"))
                ),
            )
            .withColumn(
                "is_reg",
                f.when(f.col("registration_ts") == f.col("msg_ts"), f.lit(1)).otherwise(
                    f.lit(0)
                ),
            )
            .where(f.col("is_reg") == f.lit(1))
            .drop("is_reg", "registration_ts")
        )
        registrations_sdf = self._get_event_location(
            dataframe=registrations_sdf, event_type="registration"
        )

        registrations_sdf = (
            registrations_sdf.withColumnRenamed("city_id", "zone_id")
            .where(f.col("event_lat").isNotNull())
            .withColumn("week", f.weekofyear(f.col("msg_ts").cast("timestamp")))
            .withColumn("month", f.month(f.col("msg_ts").cast("timestamp")))
            .groupby("month", "week", "zone_id")
            .agg(f.count("user_id").alias("week_user"))
            .withColumn(
                "month_user",
                f.sum(f.col("week_user")).over(
                    Window().partitionBy(f.col("zone_id"), f.col("month"))
                ),
            )
        )

        self.logger.debug("Collecing subscriptions dataframe. Processing...")

        # сборка subscriptions
        subscriptions_sdf = (
            events_sdf.where(events_sdf.event_type == "subscription")
            .select(
                events_sdf.event.datetime.alias("datetime"),
                events_sdf.event.subscription_channel.alias("subscription_channel"),
                events_sdf.event.user.alias("user_id"),
                events_sdf.lat.alias("event_lat"),
                events_sdf.lon.alias("event_lon"),
            )
            .drop_duplicates(subset=["user_id", "subscription_channel", "datetime"])
            .where(f.col("event_lat").isNotNull())
        )

        subscriptions_sdf = self._get_event_location(
            dataframe=subscriptions_sdf, event_type="subscription"
        )
        subscriptions_sdf = (
            subscriptions_sdf.withColumnRenamed("city_id", "zone_id")
            .where(f.col("event_lat").isNotNull())
            .withColumn("week", f.weekofyear(f.col("datetime").cast("timestamp")))
            .withColumn("month", f.month(f.col("datetime").cast("timestamp")))
            .groupby("month", "week", "zone_id")
            .agg(f.count("user_id").alias("week_subscription"))
            .withColumn(
                "month_subscription",
                f.sum(f.col("week_subscription")).over(
                    Window().partitionBy(f.col("zone_id"), f.col("month"))
                ),
            )
        )

        self.logger.debug("Joining resulsts")

        cols = ["zone_id", "week", "month"]
        sdf = (
            messages_sdf.join(other=reaction_sdf, on=cols)
            .join(other=registrations_sdf, on=cols)
            .join(other=subscriptions_sdf, on=cols)
            .orderBy(cols)
            .select(
                "zone_id",
                "week",
                "month",
                "week_message",
                "week_reaction",
                "week_subscription",
                "week_user",
                "month_message",
                "month_reaction",
                "month_subscription",
                "month_user",
            )
        )
        self.logger.info("Datamart collected")

        self.logger.info("Writing results")

        processed_dt = datetime.strptime(
            holder.processed_dttm.replace("T", " "), "%Y-%m-%d %H:%M:%S.%f"
        ).date()

        try:
            sdf.repartition(1).write.parquet(
                path=f"{holder.tgt_path}/date={processed_dt}",
                mode="errorifexists",
            )
            self.logger.info(f"Done! Results -> {holder.tgt_path}/date={processed_dt}")
        except Exception:
            self.logger.warning(
                "Notice that target path is already exists and will be overwritten!"
            )
            self.logger.info("Overwriting...")
            sdf.repartition(1).write.parquet(
                path=f"{holder.tgt_path}/date={processed_dt}",
                mode="overwrite",
            )
            self.logger.info(f"Done! Results -> {holder.tgt_path}/date={processed_dt}")

    def compute_friend_recommendation_datamart(self, holder: ArgsHolder) -> None:
        """Compute and save friend recommendations datamart

        Args:
            holder (`ArgsHolder`)
            dag_processed_dttm (`datetime`): `datetime` class object with processed timestamp
        """
        self.logger.info("Starting collecting friend recommendations datamart")

        src_paths = self._get_src_paths(holder=holder)
        events_sdf = self.spark.read.parquet(*src_paths)

        self.logger.debug("Collecting dataframe with real contacts")

        # реальные контакты
        real_contacts_sdf = (
            events_sdf.where(events_sdf.event_type == "message")
            .where(events_sdf.event.message_to.isNotNull())
            .select(
                events_sdf.event.message_from.alias("message_from"),
                events_sdf.event.message_to.alias("message_to"),
            )
            .withColumn(
                "user_id",
                f.explode(f.array(f.col("message_from"), f.col("message_to"))),
            )
            .withColumn(
                "contact_id",
                f.when(
                    f.col("user_id") == f.col("message_from"), f.col("message_to")
                ).otherwise(f.col("message_from")),
            )
            .select("user_id", "contact_id")
            .distinct()
            .repartitionByRange(92, "user_id", "contact_id")
        )

        self.logger.debug("Collecting all users with subscriptions dataframe")
        #  все пользователи подписавшиеся на один из каналов (любой)
        subs_sdf = (
            events_sdf.where(events_sdf.event_type == "subscription")
            .where(events_sdf.event.subscription_channel.isNotNull())
            .where(events_sdf.event.user.isNotNull())
            .select(
                events_sdf.event.subscription_channel.alias("subscription_channel"),
                events_sdf.event.user.alias("user_id"),
            )
            .drop_duplicates(subset=["user_id", "subscription_channel"])
        )

        self.logger.debug("Collecting users with same subsctiptions only dataframe")
        # пользователи подписанные на один и тот же канал
        subs_sdf = (
            subs_sdf.withColumnRenamed("user_id", "left_user")
            .join(
                subs_sdf.withColumnRenamed("user_id", "right_user"),
                on="subscription_channel",
                how="cross",
            )
            .where(f.col("left_user") != f.col("right_user"))
            .repartitionByRange(92, "left_user", "right_user")
        )

        self.logger.debug("Excluding real contacts")
        #  убрать пользователей которые переписывались
        users_for_rec = subs_sdf.join(
            real_contacts_sdf,
            on=[
                subs_sdf.left_user == real_contacts_sdf.user_id,
                subs_sdf.right_user == real_contacts_sdf.contact_id,
            ],
            how="left_anti",
        )

        self.logger.debug("Collecting last message coordinates dataframe")
        # все пользователи которые писали сообщения -> координаты последнего отправленого сообщения
        messages_sdf = (
            events_sdf.where(events_sdf.event_type == "message")
            .where(events_sdf.event.message_from.isNotNull())
            .select(
                events_sdf.event.message_from.alias("user_id"),
                events_sdf.event.message_ts.alias("message_ts"),
                events_sdf.event.datetime.alias("datetime"),
                events_sdf.lat.alias("event_lat"),
                events_sdf.lon.alias("event_lon"),
            )
            .withColumn(
                "msg_ts",
                f.when(f.col("message_ts").isNotNull(), f.col("message_ts")).otherwise(
                    f.col("datetime")
                ),
            )
            .withColumn(
                "last_msg_ts",
                f.first(col="msg_ts", ignorenulls=True).over(
                    Window().partitionBy("user_id").orderBy(f.asc("msg_ts"))
                ),
            )
            .where(f.col("msg_ts") == f.col("last_msg_ts"))
            .select("user_id", "event_lat", "event_lon")
            .distinct()
            .repartitionByRange(92, "user_id")
        )

        self.logger.debug("Collecting coordinates for potential recomendations users")
        #  коорнинаты пользователей
        users_for_rec = (
            users_for_rec.join(
                messages_sdf.select(
                    f.col("user_id"),
                    f.col("event_lat").alias("left_user_lat"),
                    f.col("event_lon").alias("left_user_lon"),
                ),
                how="left",
                on=[users_for_rec.left_user == messages_sdf.user_id],
            )
            .drop("user_id")
            .join(
                messages_sdf.select(
                    f.col("user_id"),
                    f.col("event_lat").alias("right_user_lat"),
                    f.col("event_lon").alias("right_user_lon"),
                ),
                how="left",
                on=[users_for_rec.right_user == messages_sdf.user_id],
            )
            .drop("user_id")
            .where(f.col("left_user_lat").isNotNull())
            .where(f.col("right_user_lat").isNotNull())
        )

        sdf = self._compute_distance(
            dataframe=users_for_rec, coord_cols_prefix=["left_user", "right_user"]
        )

        users_info_sdf = self._get_users_actual_data_dataframe(holder=holder)

        self.logger.debug("Collecting resulting dataframe")

        # сборка итога
        sdf = (
            sdf.where(sdf.distance <= 1)
            .select("left_user", "right_user")
            .distinct()
            .join(
                users_info_sdf.select(
                    "user_id", "act_city_id", "local_time"
                ).distinct(),
                on=[sdf.left_user == users_info_sdf.user_id],
                how="left",
            )
            .withColumn(
                "processed_dttm", f.lit(holder.processed_dttm.replace("T", " "))
            )
            .select(
                "left_user",
                "right_user",
                "processed_dttm",
                f.col("act_city_id").alias("zone_id"),
                "local_time",
            ),
        )

        self.logger.info("Datamart collected")

        self.logger.info("Writing results")

        processed_dt = datetime.strptime(
            holder.processed_dttm.replace("T", " "), "%Y-%m-%d %H:%M:%S.%f"
        ).date()

        try:
            sdf.repartition(1).write.parquet(
                path=f"{holder.tgt_path}/date={processed_dt}",
                mode="errorifexists",
            )
            self.logger.info(f"Done! Results -> {holder.tgt_path}/date={processed_dt}")
        except Exception:
            self.logger.warning(
                "Notice that target path is already exists and will be overwritten!"
            )
            self.logger.info("Overwriting...")
            sdf.repartition(1).write.parquet(
                path=f"{holder.tgt_path}/date={processed_dt}",
                mode="overwrite",
            )
            self.logger.info(f"Done! Results -> {holder.tgt_path}/date={processed_dt}")


def main() -> None:
    holder = ArgsHolder(
        date="2022-03-12",
        depth=10,
        src_path="s3a://data-ice-lake-04/messager-data/analytics/geo-events",
        tgt_path="s3a://data-ice-lake-04/messager-data/analytics/tmp/friend_recommendation_datamart",
        processed_dttm=str(datetime.now()),
    )
    try:
        spark = SparkRunner()
        spark.init_session(app_name="testing-app", log4j_level="INFO")
        spark.compute_users_info_datamart(holder=holder)

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
