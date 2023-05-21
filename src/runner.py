import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Literal
from pandas import read_csv

import boto3
import findspark
from botocore.exceptions import ClientError

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.logger import SparkLogger
from src.utils import (
    SparkArgsValidator,
    TagsJobArgsHolder,
    load_environment,
    ArgsHolder,
)

os.environ["HADOOP_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr/bin/java"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/opt/conda/bin/python3"

findspark.init()
findspark.find()

from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as f
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.utils import AnalysisException

load_environment()

config = Config()


class SparkRunner:
    def __init__(self) -> None:
        """Main data processor class"""
        self.logger = SparkLogger(level=config.python_log_level).get_logger(
            logger_name=str(Path(Path(__file__).name))
        )
        self.validator = SparkArgsValidator()

    def _get_s3_instance(self):
        "Get boto3 S3 connection instance"

        self.logger.debug("Getting s3 connection instace")

        return boto3.session.Session().client(
            service_name="s3",
            endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

    def _get_src_paths(
        self,
        holder: ArgsHolder,
    ) -> List[str]:
        """Get S3 paths contains dataset partitions.

        Collects paths corresponding to the passed in `holder` object arguments and checks if each path exists on S3. Collects only existing paths.

        ## Parameters
        `event_type` : Event type for partition
        `holder` : Dataclass-like object with Spark Job arguments

        ## Returns
        `List[str]` : List with existing partition paths on s3

        ## Reises
        If no paths for given arguments kill process

        ## Examples
        >>> holder = ArgsHolder(date="2022-03-12", depth=10, ..., src_path="s3a://data-ice-lake-04/messager-data/analytics/geo-events")
        >>> src_paths = self._get_src_paths(holder=holder)
        >>> print(len(src_paths))
        4 # only 4 paths exists on s3
        >>> for _ in src_paths: print(_)
        "s3a://data-ice-lake-04/messager-data/analytics/geo-events/date=2022-03-12"
        "s3a://data-ice-lake-04/messager-data/analytics/geo-events/date=2022-03-11"
        ...
        """
        s3 = self._get_s3_instance()

        self.logger.debug("Collecting src paths")

        date = datetime.strptime(holder.date, "%Y-%m-%d").date()

        paths = [
            f"{holder.src_path}/date=" + str(date - timedelta(days=i))
            for i in range(int(holder.depth))
        ]

        self.logger.debug("Checking if each path exists on s3")

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
                    self.logger.debug(f"No data for `{path}` on s3. Skipping")
            except ClientError as e:
                self.logger.exception(e)
                sys.exit(1)

        # if returns empty list - exit
        if existing_paths == []:
            self.logger.error("There is no data for given arguments!")
            sys.exit(1)

        self.logger.debug(f"Done with {len(existing_paths)} paths in total")

        return existing_paths

    def init_session(
        self,
        app_name: str,
        log4j_level: Literal[
            "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"
        ] = "WARN",
    ) -> None:
        """Configure and initialize Spark Session

        ## Parameters
        `app_name` : Name of Spark application
        `log4j_level` : Spark Context Java logging level, by default "WARN"
        """

        self.logger.info("Initializing Spark Session")

        self.spark = (
            SparkSession.builder.master("yarn")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
            .config(
                "spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")
            )
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("AWS_ENDPOINT_URL"))
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .appName(app_name)
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel(log4j_level)

        self.logger.info(f"Log4j level set to: {log4j_level}")

    def stop_session(self) -> None:
        """Stop active Spark Session"""
        self.logger.info("Stopping Spark Session")
        self.spark.stop()
        self.logger.info("Session stopped")

    def _compute_distance(
        self, dataframe: DataFrame, coord_cols_prefix: List[str]
    ) -> DataFrame:
        """Compute distance between two point for each row of DataFrame

        ## Parameters
        `dataframe` : `pyspark.sql.DataFrame`
        `coord_cols_prefix` : Prefix of columns names with coordinates.

        For example `['city', 'event']`. This means that DataFrame contains columns city_lat, city_lon, event_lat and event_lon with the corresponding coordinates

        ## Returns
        `pyspark.sql.DataFrame` : DataFrame with additional column `distance` which contains distance between two columns

        ## Raises
        `AnalysisException` : Raises if DataFrame does not contains columns with coordinates or if `coord_cols_prefix` contains more than two values

        ## Examples
        >>> sdf.show()
        +-------+----------+-------------------+------------------+-------+-----------+--------+--------+
        |user_id|message_id|          event_lat|         event_lon|city_id|  city_name|city_lat|city_lon|
        +-------+----------+-------------------+------------------+-------+-----------+--------+--------+
        |  11084|    649853|-36.862504936703104| 144.5634957576193|      1|     Sydney| -33.865|151.2094|
        |  11084|    649853|-36.862504936703104| 144.5634957576193|      2|  Melbourne|-37.8136|144.9631|
        |  11084|    649853|-36.862504936703104| 144.5634957576193|      3|   Brisbane|-27.4678|153.0281|
                                                ...
        |  11084|    649853|-36.862504936703104| 144.5634957576193|     17|     Darwin|-12.4381|130.8411|
        |  11084|    649853|-36.862504936703104| 144.5634957576193|     18|   Ballarat|  -37.55|  143.85|
        |  11084|    649853|-36.862504936703104| 144.5634957576193|     19|    Bendigo|  -36.75|144.2667|
        +-------+----------+-------------------+------------------+-------+-----------+--------+--------+
        >>> new_sdf = self._compute_distance(
        ...            dataframe=sdf,
        ...            coord_cols_prefix=["event", "city"],
        ...        )
        >>> new_sdf.show()
        +-------+----------+-------------------+------------------+-------+-----------+--------+--------+--------+
        |user_id|message_id|          event_lat|         event_lon|city_id|  city_name|city_lat|city_lon|distance|
        +-------+----------+-------------------+------------------+-------+-----------+--------+--------+--------+
        |  11084|    649853|-36.862504936703104| 144.5634957576193|      1|     Sydney| -33.865|151.2094|   688.0|
        |  11084|    649853|-36.862504936703104| 144.5634957576193|      2|  Melbourne|-37.8136|144.9631|   112.0|
        |  11084|    649853|-36.862504936703104| 144.5634957576193|      3|   Brisbane|-27.4678|153.0281|  1313.0|
                                                ...
        |  11084|    649853|-36.862504936703104| 144.5634957576193|     16|  Toowoomba|-27.5667|  151.95|  1245.0|
        |  11084|    649853|-36.862504936703104| 144.5634957576193|     17|     Darwin|-12.4381|130.8411|  3041.0|
        |  11084|    649853|-36.862504936703104| 144.5634957576193|     18|   Ballarat|  -37.55|  143.85|    99.0|
        +-------+----------+-------------------+------------------+-------+-----------+--------+--------+--------+
        """

        if len(coord_cols_prefix) > 2:
            raise AnalysisException(
                "Only two values are allowed for `coord_cols_prefix` argument"
            )

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
        """Takes a DataFrame containing events and their coordinates, calculates the distance to each city, and keeps only the closest cities.

        ## Parameters
        `dataframe` : `pyspark.sql.DataFrame` with user events and its coordinates

        `event_type` : Type of event. That needs to proper calculations

        ## Returns
        `DataFrame` : `pyspark.sql.DataFrame` with additional columns city_id and city_name

        ## Examples
        >>> sdf.show()
        +-------+----------+-------------------+------------------+--------------------+
        |user_id|message_id|          event_lat|         event_lon|              msg_ts|
        +-------+----------+-------------------+------------------+--------------------+
        |  11084|    649853|-36.862504936703104| 144.5634957576193|2021-04-26 06:51:...|
        |  69134|    941827| -34.60603589904486|149.33684624664335|2021-04-26 07:56:...|
        | 103904|    179329|  -37.5427633285771| 144.5178360308331|2021-04-26 21:09:...|
                                            ...
        |  45581|     31760| -20.36824312975724|149.86966318101773|2021-04-25 14:59:...|
        |  20609|    749450|-34.301014598797465|149.51979221740035|2021-04-25 07:33:...|
        +-------+----------+-------------------+------------------+--------------------+

        >>> result_sdf = self._get_event_location(dataframe=sdf, event_type="message")
        >>> result_sdf.show()
        +-------+----------+-------------------+------------------+--------------------+-------+-----------+
        |user_id|message_id|          event_lat|         event_lon|              msg_ts|city_id|  city_name|
        +-------+----------+-------------------+------------------+--------------------+-------+-----------+
        |  86176|      1149| -20.40504695348027|149.33952603935091|2021-04-25 22:12:...|     21|     Mackay|
        |   4867|      1540|-26.654484725492868|152.69213748942875|2021-04-25 09:29:...|      3|   Brisbane|
        | 145178|      2077|-27.166267995866157| 152.7462979951173|2021-04-26 15:31:...|      3|   Brisbane|
                                            ...
        | 147549|      9567| -40.58618348086873|147.93833305792634|2021-04-26 18:14:...|     20| Launceston|
        |  91578|     11869| -41.04773532335144|147.26558385326746|2021-04-26 21:19:...|     20| Launceston|
        +-------+----------+-------------------+------------------+--------------------+-------+-----------+
        """
        self.logger.debug(f"Getting location of `{event_type}` event type")

        self.logger.debug(f"Collecting cities coordinates dataframe from s3")
        cities_coords_sdf = self.spark.read.parquet(
            "s3a://data-ice-lake-05/messager-data/analytics/cities-coordinates"
        )

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
        """Returns dataframe with user information based on sent messages

        ## Parameters
        `holder` : Arguments holder object

        ## Returns
        `DataFrame` : `pyspark.sql.DataFrame`

        ## Examples
        >>> sdf = self._get_users_actual_data_dataframe(holder=holder)
        >>> sdf.printSchema()
        root
        |-- user_id: long (nullable = true)
        |-- message_id: long (nullable = true)
        |-- msg_ts: string (nullable = true)
        |-- city_name: string (nullable = true)
        |-- act_city: string (nullable = true)
        |-- act_city_id: integer (nullable = true)
        |-- local_time: timestamp (nullable = true)
        >>> sdf.show()
        +-------+----------+---------------+-----------+-----------+-----------+---------------+
        |user_id|message_id|         msg_ts|  city_name|   act_city|act_city_id|     local_time|
        +-------+----------+---------------+-----------+-----------+-----------+---------------+
        |     45|     22537|2021-04-26 ... |   Maitland|   Maitland|         23|2021-04-27 ... |
        |     54|   1118144|2022-04-25 ... |     Darwin|     Darwin|         17|2022-04-25 ... |
        |    111|    473206|2021-04-25 ... | Gold Coast| Gold Coast|          6|2021-04-25 ... |
        |    122|    304847|2021-04-26 ... |     Cairns|     Cairns|         15|2021-04-26 ... |
                                                    ...
        |    273|    167389|2021-04-25 ... |      Perth|      Perth|          4|2021-04-27 ... |
        |    273|    113588|2021-04-25 ... |      Perth|      Perth|          4|2021-04-27 ... |
        |    406|   1129907|2022-04-26 ... |   Maitland|   Maitland|         23|2022-04-26 ... |
        |    418|   1115254|2022-04-25 ... |      Perth|      Perth|          4|2022-04-26 ... |
        +-------+----------+---------------+-----------+-----------+-----------+---------------+
        """
        self.logger.debug(f"Getting input data from: {holder.src_path}")

        src_paths = self._get_src_paths(holder=holder)
        events_sdf = self.spark.read.parquet(*src_paths)

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
        """Collect users info datamart and save results on s3

        ## Parameters
        `holder` : Argument holder object

        ## Examples
        >>> spark = SparkRunner()
        >>> spark.init_session(app_name="testing-app", log4j_level="INFO")
        >>> spark.compute_users_info_datamart(holder=holder) # saved results on s3
        >>> sdf = spark.read.parquet(f"{holder.tgt_path}") # reading saved results
        >>> sdf.printSchema()
        root
        |-- user_id: long (nullable = true)
        |-- act_city: string (nullable = true)
        |-- home_city: string (nullable = true)
        |-- local_time: timestamp (nullable = true)
        |-- travel_count: integer (nullable = true)
        |-- travel_array: array (nullable = true)
        |    |-- element: string (containsNull = false)

        >>> sdf.show()
        +-------+-----------+---------+---------------+------------+--------------------+
        |user_id|   act_city|home_city|     local_time|travel_count|        travel_array|
        +-------+-----------+---------+---------------+------------+--------------------+
        |     45|   Maitland|     null|2021-04-27 ... |           1|          [Maitland]|
        |     54|     Darwin|     null|2022-04-25 ... |           1|            [Darwin]|
        |    111| Gold Coast|     null|2021-04-25 ... |           1|        [Gold Coast]|
        |    122|     Cairns|     null|2021-04-26 ... |           1|            [Cairns]|
                                            ...
        |    487|     Cairns|     null|2021-04-26 ... |           1|            [Cairns]|
        |    610| Wollongong|     null|2021-04-26 ... |           1|        [Wollongong]|
        |    611|    Bunbury|     null|2021-04-27 ... |           1|           [Bunbury]|
        |    617|  Newcastle|     null|2021-04-26 ... |           1|         [Newcastle]|
        +-------+-----------+---------+---------------+------------+--------------------+
        """
        self.logger.info("Starting collecting user info datamart")

        job_start = datetime.now()

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

        self.logger.debug("Collecting users home city. Processing...")
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
        sdf.printSchema()

        sdf.show(40)

        self.logger.info("Datamart collected")
        self.logger.info("Writing results")

        processed_dt = datetime.strptime(
            holder.processed_dttm.replace("T", " "), "%Y-%m-%d %H:%M:%S.%f"
        ).date()
        OUTPUT_PATH = f"{holder.tgt_path}/date={processed_dt}"
        try:
            sdf.repartition(1).write.parquet(
                path=OUTPUT_PATH,
                mode="errorifexists",
            )
            self.logger.info(f"Done! Results -> {OUTPUT_PATH}")
        except Exception:
            self.logger.warning(
                "Notice that target path is already exists and will be overwritten!"
            )
            self.logger.info("Overwriting...")
            sdf.repartition(1).write.parquet(
                path=OUTPUT_PATH,
                mode="overwrite",
            )
            self.logger.info(f"Done! Results -> {OUTPUT_PATH}")

        job_end = datetime.now()
        self.logger.info(f"Job execution time: {job_end - job_start}")

    def run_tags_job(
        self,
        holder: TagsJobArgsHolder,
    ) -> None:
        try:
            self.validator.validate_tags_job_args(holder=holder)
        except AssertionError as e:
            self.logger.exception(e)
            sys.exit(1)

        src_paths = self._get_src_paths(event_type="message", holder=holder)

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
        self.logger.info(f"Done. Rows in dataset: {df.count()}")

        self.logger.info("Writing cleaned dataset to s3.")
        df.write.parquet(
            path=tgt_path,
            mode="overwrite",
            partitionBy=["event_type", "date"],
            compression="gzip",
        )
        self.logger.info("Done")
