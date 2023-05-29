from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import findspark
from botocore.exceptions import ClientError

# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.config import Config
from src.logger import SparkLogger
from src.environ import EnvironError, EnvironManager

os.environ["HADOOP_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/usr/bin/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr/bin/java"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/opt/conda/bin/python3"

findspark.init()
findspark.find()

import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from pyspark.storagelevel import StorageLevel

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import List, Literal
    from pyspark.sql import DataFrame
    from src.datamodel import ArgsKeeper, SparkConfigKeeper


class SparkRunner:
    """Data processor and datamarts collector class.

    ## Usage
    Initialize all needed dataclasses:
    >>> keeper = ArgsKeeper(
    ...     date="2022-04-26",
    ...     depth=2,
    ...     src_path="s3a://data-ice-lake-05/messager-data/analytics/geo-events",
    ...     tgt_path="s3a://data-ice-lake-05/messager-data/analytics/prod/friend_recommendation_datamart",
    ...     processed_dttm=str(datetime.now()),
    ...     )
    >>> conf = SparkConfigKeeper(
    ...     executor_memory="3000m", executor_cores=1, max_executors_num=12
    ...     )

    Initialize `SparkRunner` class object:
    >>> spark = SparkRunner()

    Start session:
    >>> spark.init_session(app_name="data-collector-app", spark_conf=conf, log4j_level="INFO")

    And now execute chosen job:
    >>> spark.collect_friend_recommendation_datamart(keeper=keeper)

    Don't forget to always stop session:
    >>> spark.stop_session()
    """

    def __init__(self) -> None:
        config = Config()

        self.logger = SparkLogger(level=config.python_log_level).get_logger(
            logger_name=__name__
        )
        try:
            env = EnvironManager()
            env.load_environ()
        except EnvironError as e:
            self.logger.critical(e)

    def _get_s3_instance(self):
        "Get ready-to-use boto3 S3 connection instance"

        self.logger.debug("Getting s3 connection instace")

        s3 = boto3.session.Session().client(  # type: ignore
            service_name="s3",
            endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        self.logger.debug(f"Boto3 instance: {s3}")
        return s3

    def _get_src_paths(
        self,
        event_type: Literal["message", "reaction", "subscription"],
        keeper: ArgsKeeper,
    ) -> List[str]:
        """Get S3 paths contains dataset partitions.

        Collects paths corresponding to the passed in `keeper` object arguments and checks if each path exists on S3. Collects only existing paths.

        If no paths for given arguments kill process.

        ## Parameters
        `event_type` : Event type of partitions
        `keeper` : Dataclass-like object with Spark Job arguments

        ## Returns
        `List[str]` : List with existing partition paths on s3

        ## Examples
        >>> keeper = ArgsKeeper(date="2022-03-12", depth=10, ..., src_path="s3a://data-ice-lake-05/messager-data/analytics/geo-events")
        >>> src_paths = self._get_src_paths(event_type="message", keeper=keeper)
        >>> print(len(src_paths))
        4 # only 4 paths exists on s3
        >>> print(type(src_paths))
        <class 'list'>
        >>> for _ in src_paths: print(_) # how it looks
        "s3a://data-ice-lake-05/messager-data/analytics/geo-events/event_type=message/date=2022-03-12"
        ...
        "s3a://data-ice-lake-05/messager-data/analytics/geo-events/event_type=message/date=2022-03-11"
        """
        s3 = self._get_s3_instance()

        self.logger.debug(f"Collecting src paths for {event_type} event type")

        date = datetime.strptime(keeper.date, "%Y-%m-%d").date()

        paths = [
            f"{keeper.src_path}/event_type={event_type}/date="
            + str(date - timedelta(days=i))
            for i in range(int(keeper.depth))
        ]

        self.logger.debug("Checking if each path exists on s3")

        existing_paths = []
        for path in paths:
            self.logger.debug(f"Checking {path}")
            try:
                response = s3.list_objects(
                    Bucket=path.split(sep="/")[2],
                    MaxKeys=5,
                    Prefix="/".join(path.split(sep="/")[3:]),
                )
                if "Contents" in response.keys():
                    existing_paths.append(path)
                    self.logger.debug("OK")
                else:
                    self.logger.debug(f"No data for `{path}` on s3. Skipping")
            except ClientError as e:
                self.logger.exception(e)
                sys.exit(1)

        # if returns empty list - exit
        if existing_paths == []:
            self.logger.error("There is no data for given arguments!")
            sys.exit(1)

        self.logger.debug(f"Done. {len(existing_paths)} paths collected")

        return existing_paths

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
            .config("spark.executor.memory", spark_conf.executor_memory)
            .config("spark.executor.cores", str(spark_conf.executor_cores))
            .config(
                "spark.dynamicAllocation.maxExecutors",
                str(spark_conf.max_executors_num),
            )
            .appName(app_name)
            .getOrCreate()
        )
        s = "".join(
            f"\t{i[0]}: {i[1]}\n" for i in spark_conf
        )  # for print job config in logs
        self.logger.info("Spark job properties:\n" + s)

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
            raise IndexError(
                "Only two values are allowed for `coord_cols_prefix` argument"
            )

        self.logger.debug("Computing distances")

        self.logger.debug("Collecting columns with coordinates")
        cols = [(i + "_lat", i + "_lon") for i in coord_cols_prefix]
        lat_1, lon_1 = cols[0]
        lat_2, lon_2 = cols[1]

        self.logger.debug("Checking if each columns exists in dataframe")
        if not all(col in dataframe.columns for col in (lat_1, lon_1, lat_2, lon_2)):
            raise KeyError(
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
        self.logger.debug(f"Collecting location of `{event_type}` event type")

        self.logger.debug(f"Getting cities coordinates dataframe from s3")
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
        sdf = dataframe.crossJoin(
            cities_coords_sdf.select("city_id", "city_name", "city_lat", "city_lon")
        )
        sdf = self._compute_distance(
            dataframe=sdf,
            coord_cols_prefix=["event", "city"],
        )
        self.logger.debug("Collecting resulting dataframe")
        sdf = (
            sdf.withColumn(
                "city_dist_rnk",
                f.row_number().over(
                    Window().partitionBy(partition_by).orderBy(f.asc("distance"))  # type: ignore
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

    def _get_users_actual_data_dataframe(self, keeper: ArgsKeeper) -> DataFrame:
        """Returns dataframe with user information based on sent messages

        ## Parameters
        `keeper` : Arguments keeper object

        ## Returns
        `DataFrame` : `pyspark.sql.DataFrame`

        ## Examples
        >>> sdf = self._get_users_actual_data_dataframe(keeper=keeper)
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
        self.logger.debug("Collecting users actual data dataframe")

        self.logger.debug(f"Getting input data from: {keeper.src_path}")

        src_paths = self._get_src_paths(event_type="message", keeper=keeper)
        events_sdf = (
            self.spark.read.option("mergeSchema", "true")
            .option("cacheMetadata", "true")
            .parquet(*src_paths)
        )

        self.logger.debug("Collecting dataframe. Processing...")

        sdf = (
            events_sdf.where(events_sdf.message_from.isNotNull())
            .select(
                events_sdf.message_from.alias("user_id"),
                events_sdf.message_id,
                events_sdf.message_ts,
                events_sdf.datetime,
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

        self.logger.debug("Getting cities coordinates dataframe from s3")
        cities_coords_sdf = self.spark.read.parquet(
            "s3a://data-ice-lake-05/messager-data/analytics/cities-coordinates"
        )

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
            .join(
                cities_coords_sdf.select("city_id", "timezone"),
                on=f.col("act_city_id") == cities_coords_sdf.city_id,
                how="left",
            )
            .withColumn(
                "local_time",
                f.from_utc_timestamp(
                    timestamp=f.col("last_msg_ts"), tz=f.col("timezone")
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

    def collect_users_info_datamart(self, keeper: ArgsKeeper) -> None:
        """Collect users info datamart and save results on s3

        ## Parameters
        `keeper` : Arguments keeper object

        ## Examples
        Lets initialize class object and start session:
        >>> spark = SparkRunner()
        >>> spark.init_session(app_name="testing-app", spark_conf=conf, log4j_level="INFO")

        Now we can execute class method that collects datamart:
        >>> spark.collect_users_info_datamart(keeper=keeper)

        And after that read saved results to see how it looks:
        >>> sdf = spark.read.parquet(keeper.tgt_path)
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
        +-------+-----------+--------------------+---------------+------------+--------------------+
        |user_id|   act_city|     home_city      |  local_time   |travel_count|        travel_array|
        +-------+-----------+--------------------+---------------+------------+--------------------+
        |     45|   Maitland| Couldn't determine |2021-04-27 ... |           1|          [Maitland]|
        |     54|     Darwin| Couldn't determine |2022-04-25 ... |           1|            [Darwin]|
        |    111| Gold Coast| Couldn't determine |2021-04-25 ... |           1|        [Gold Coast]|
        |    122|     Cairns| Couldn't determine |2021-04-26 ... |           1|            [Cairns]|
                                            ...
        |    487|     Cairns|      Maitland      |2021-04-26 ... |           1|            [Cairns]|
        |    610| Wollongong| Couldn't determine |2021-04-26 ... |           1|        [Wollongong]|
        |    611|    Bunbury| Couldn't determine |2021-04-27 ... |           1|           [Bunbury]|
        |    617|  Newcastle| Couldn't determine |2021-04-26 ... |           1|         [Newcastle]|
        +-------+-----------+--------------------+---------------+------------+--------------------+
        """
        self.logger.info("Starting collecting user info datamart")

        job_start = datetime.now()

        sdf = self._get_users_actual_data_dataframe(keeper=keeper)

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

        self.logger.debug("Collecting datamart")

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
        sdf = sdf.fillna(value="Couldn't determine", subset="home_city")

        self.logger.info("Datamart collected!")

        self.logger.info("Writing results")

        processed_dt = datetime.strptime(
            keeper.processed_dttm.replace("T", " "), "%Y-%m-%d %H:%M:%S"
        ).date()
        OUTPUT_PATH = f"{keeper.tgt_path}/date={processed_dt}"
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

    def collect_location_zone_agg_datamart(self, keeper: ArgsKeeper) -> None:
        """Collect location zone aggregation datamart and save results on s3

        ## Parameters
        `keeper` : Arguments keeper object

        ## Examples
        Lets initialize class object and start session:
        >>> spark = SparkRunner()
        >>> spark.init_session(app_name="testing-app", spark_conf=conf, log4j_level="INFO")

        Now we can execute class method that collects datamart:
        >>> spark.collect_location_zone_agg_datamart(keeper=keeper) # saved results on s3

        And after that read saved results to see how it looks:
        >>> sdf = spark.read.parquet(keeper.tgt_path)
        >>> sdf.printSchema()
        root
        |-- zone_id: integer (nullable = true)
        |-- week: date (nullable = true)
        |-- month: date (nullable = true)
        |-- week_message: long (nullable = false)
        |-- week_reaction: long (nullable = false)
        |-- week_subscription: long (nullable = false)
        |-- week_user: long (nullable = false)
        |-- month_message: long (nullable = true)
        |-- month_reaction: long (nullable = true)
        |-- month_subscription: long (nullable = true)
        |-- month_user: long (nullable = true)
        >>> sdf.show()
        +-------+----------+----------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+
        |zone_id|week      |month     |week_message|week_reaction|week_subscription|week_user|month_message|month_reaction|month_subscription|month_user|
        +-------+----------+----------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+
        |1      |2022-02-21|2022-02-01|78          |99           |3713             |77       |105          |123           |4739              |103       |
        |1      |2022-02-28|2022-02-01|27          |24           |1026             |26       |105          |123           |4739              |103       |
        |1      |2022-02-28|2022-03-01|134         |147          |6453             |130      |659          |1098          |45580             |630       |
        |1      |2022-03-07|2022-03-01|145         |224          |8833             |143      |659          |1098          |45580             |630       |
                                                                             ...
        |3      |2022-02-28|2022-03-01|238         |142          |13786            |233      |1190         |1068          |99175             |1121      |
        |3      |2022-03-07|2022-03-01|264         |192          |19320            |253      |1190         |1068          |99175             |1121      |
        |3      |2022-03-14|2022-03-01|270         |253          |22658            |254      |1190         |1068          |99175             |1121      |
        |3      |2022-03-21|2022-03-01|258         |300          |26533            |233      |1190         |1068          |99175             |1121      |
        |3      |2022-03-28|2022-03-01|160         |181          |16878            |148      |1190         |1068          |99175             |1121      |
        +-------+----------+----------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+
        """
        self.logger.info("Staring collecting location zone aggregated datamart")
        job_start = datetime.now()

        self.logger.debug("Collecing messages dataframe. Processing...")

        src_paths = self._get_src_paths(keeper=keeper, event_type="message")
        messages_sdf = (
            self.spark.read.option("mergeSchema", "true")
            .option("cacheMetadata", "true")
            .parquet(*src_paths)
        )

        messages_sdf = (
            messages_sdf.where(messages_sdf.message_from.isNotNull())
            .select(
                messages_sdf.message_from.alias("user_id"),
                messages_sdf.message_id,
                messages_sdf.message_ts,
                messages_sdf.datetime,
                messages_sdf.lat.alias("event_lat"),
                messages_sdf.lon.alias("event_lon"),
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
            .withColumn("week", f.trunc(f.col("msg_ts"), "week"))
            .withColumn("month", f.trunc(f.col("msg_ts"), "month"))
            .groupby("month", "week", "zone_id")
            .agg(f.count("message_id").alias("week_message"))
            .withColumn(
                "month_message",
                f.sum(f.col("week_message")).over(
                    Window().partitionBy(f.col("zone_id"), f.col("month"))
                ),
            )
            .persist(storageLevel=StorageLevel.MEMORY_ONLY)
        )

        self.logger.debug("Collecing reacitons dataframe. Processing...")

        src_paths = self._get_src_paths(keeper=keeper, event_type="reaction")
        reaction_sdf = self.spark.read.parquet(*src_paths)
        reaction_sdf = (
            self.spark.read.option("mergeSchema", "true")
            .option("cacheMetadata", "true")
            .parquet(*src_paths)
        )

        reaction_sdf = (
            reaction_sdf.select(
                reaction_sdf.datetime,
                reaction_sdf.message_id,
                reaction_sdf.reaction_from.alias("user_id"),
                reaction_sdf.lat.alias("event_lat"),
                reaction_sdf.lon.alias("event_lon"),
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
            .withColumn("week", f.trunc(f.col("datetime"), "week"))
            .withColumn("month", f.trunc(f.col("datetime"), "month"))
            .groupby("month", "week", "zone_id")
            .agg(f.count("message_id").alias("week_reaction"))
            .withColumn(
                "month_reaction",
                f.sum(f.col("week_reaction")).over(
                    Window().partitionBy(f.col("zone_id"), f.col("month"))
                ),
            )
            .persist(storageLevel=StorageLevel.MEMORY_ONLY)
        )

        self.logger.debug("Collecing registrations dataframe. Processing...")

        src_paths = self._get_src_paths(keeper=keeper, event_type="message")
        registrations_sdf = (
            self.spark.read.option("mergeSchema", "true")
            .option("cacheMetadata", "true")
            .parquet(*src_paths)
        )

        registrations_sdf = (
            registrations_sdf.where(registrations_sdf.message_from.isNotNull())
            .select(
                registrations_sdf.message_from.alias("user_id"),
                registrations_sdf.message_id,
                registrations_sdf.message_ts,
                registrations_sdf.datetime,
                registrations_sdf.lat.alias("event_lat"),
                registrations_sdf.lon.alias("event_lon"),
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
            .withColumn("week", f.trunc(f.col("msg_ts"), "week"))
            .withColumn("month", f.trunc(f.col("msg_ts"), "month"))
            .groupby("month", "week", "zone_id")
            .agg(f.count("user_id").alias("week_user"))
            .withColumn(
                "month_user",
                f.sum(f.col("week_user")).over(
                    Window().partitionBy(f.col("zone_id"), f.col("month"))
                ),
            )
            .persist(storageLevel=StorageLevel.MEMORY_ONLY)
        )

        self.logger.debug("Collecing subscriptions dataframe. Processing...")

        src_paths = self._get_src_paths(keeper=keeper, event_type="subscription")
        subscriptions_sdf = (
            self.spark.read.option("mergeSchema", "true")
            .option("cacheMetadata", "true")
            .parquet(*src_paths)
        )

        subscriptions_sdf = (
            subscriptions_sdf.select(
                subscriptions_sdf.datetime,
                subscriptions_sdf.subscription_channel,
                subscriptions_sdf.user.alias("user_id"),
                subscriptions_sdf.lat.alias("event_lat"),
                subscriptions_sdf.lon.alias("event_lon"),
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
            .withColumn("week", f.trunc(f.col("datetime"), "week"))
            .withColumn("month", f.trunc(f.col("datetime"), "month"))
            .groupby("month", "week", "zone_id")
            .agg(f.count("user_id").alias("week_subscription"))
            .withColumn(
                "month_subscription",
                f.sum(f.col("week_subscription")).over(
                    Window().partitionBy(f.col("zone_id"), f.col("month"))
                ),
            )
            .persist(storageLevel=StorageLevel.MEMORY_ONLY)
        )

        self.logger.debug("Joining dataframes")

        cols = ["zone_id", "week", "month"]
        sdf = (
            messages_sdf.join(other=reaction_sdf, on=cols)
            .join(other=registrations_sdf, on=cols)
            .join(other=subscriptions_sdf, on=cols)
            .orderBy(cols)  # type: ignore
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
        messages_sdf.unpersist()
        reaction_sdf.unpersist()
        registrations_sdf.unpersist()
        subscriptions_sdf.unpersist()
        del (messages_sdf, reaction_sdf, registrations_sdf, subscriptions_sdf)

        self.logger.info("Datamart collected!")

        self.logger.info("Writing results")

        processed_dt = datetime.strptime(
            keeper.processed_dttm.replace("T", " "), "%Y-%m-%d %H:%M:%S"
        ).date()
        OUTPUT_PATH = f"{keeper.tgt_path}/date={processed_dt}"
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

    def collect_friend_recommendation_datamart(self, keeper: ArgsKeeper) -> None:
        """Collect friend recommendation datamart and save results on s3

        ## Parameters
        `keeper` : Arguments keeper object

        ## Examples
        Lets initialize class object and start session:
        >>> spark = SparkRunner()
        >>> spark.init_session(app_name="testing-app", log4j_level="INFO")

        Now we can execute class method that collects datamart:
        >>> spark.collect_friend_recommendation_datamart(keeper=keeper)

        And after that read saved results to see how it looks:
        >>> sdf = spark.read.parquet(keeper.tgt_path)
        >>> sdf.printSchema()
        root
        |-- left_user: string (nullable = true)
        |-- right_user: string (nullable = true)
        |-- processed_dttm: string (nullable = true)
        |-- zone_id: integer (nullable = true)
        |-- local_time: timestamp (nullable = true)
        """
        self.logger.info("Starting collecting friend recommendations datamart")
        job_start = datetime.now()

        messages_src_paths = self._get_src_paths(keeper=keeper, event_type="message")
        real_contacts_sdf = (
            self.spark.read.option("mergeSchema", "true")
            .option("cacheMetadata", "true")
            .parquet(*messages_src_paths)
        )

        self.logger.debug("Collecting dataframe with real contacts")

        # реальные контакты
        real_contacts_sdf = (
            real_contacts_sdf.where(real_contacts_sdf.message_to.isNotNull())
            .select(
                real_contacts_sdf.message_from,
                real_contacts_sdf.message_to,
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
            # .repartition(92, "user_id", "contact_id")
            # .persist(storageLevel=StorageLevel.MEMORY_ONLY)
        )

        self.logger.debug("Collecting all users with subscriptions dataframe")
        #  все пользователи подписавшиеся на один из каналов (любой)
        subscription_src_paths = self._get_src_paths(
            keeper=keeper, event_type="subscription"
        )
        subs_sdf = (
            self.spark.read.option("mergeSchema", "true")
            .option("cacheMetadata", "true")
            .parquet(*subscription_src_paths)
        )

        subs_sdf = (
            subs_sdf.where(subs_sdf.subscription_channel.isNotNull())
            .where(subs_sdf.user.isNotNull())
            .select(
                subs_sdf.subscription_channel,
                subs_sdf.user.alias("user_id"),
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
            # .repartition(92, "left_user", "right_user")
            # .persist(storageLevel=StorageLevel.MEMORY_ONLY)
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
            self.spark.read.option("mergeSchema", "true")
            .option("cacheMetadata", "true")
            .parquet(*messages_src_paths)
        )
        messages_sdf = (
            messages_sdf.where(messages_sdf.message_from.isNotNull())
            .select(
                messages_sdf.message_from.alias("user_id"),
                messages_sdf.message_ts,
                messages_sdf.datetime,
                messages_sdf.lat.alias("event_lat"),
                messages_sdf.lon.alias("event_lon"),
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
            # .repartitionByRange(92, "user_id")
            # .persist(storageLevel=StorageLevel.MEMORY_ONLY)
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
            # .persist(storageLevel=StorageLevel.MEMORY_ONLY)
        )

        sdf = self._compute_distance(
            dataframe=users_for_rec, coord_cols_prefix=["left_user", "right_user"]
        )

        users_info_sdf = self._get_users_actual_data_dataframe(keeper=keeper)

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
                "processed_dttm", f.lit(keeper.processed_dttm.replace("T", " "))
            )
            .select(
                "left_user",
                "right_user",
                "processed_dttm",
                f.col("act_city_id").alias("zone_id"),
                "local_time",
            )
        )

        self.logger.info("Datamart collected")

        self.logger.info("Writing results")

        sdf.show(200, False)

        sdf.printSchema()

        # processed_dt = datetime.strptime(
        #     keeper.processed_dttm.replace("T", " "), "%Y-%m-%d %H:%M:%S.%f"
        # ).date()
        # OUTPUT_PATH = f"{keeper.tgt_path}/date={processed_dt}"
        # try:
        #     sdf.repartition(1).write.parquet(
        #         path=OUTPUT_PATH,
        #         mode="errorifexists",
        #     )
        #     self.logger.info(f"Done! Results -> {OUTPUT_PATH}")
        # except Exception:
        #     self.logger.warning(
        #         "Notice that target path is already exists and will be overwritten!"
        #     )
        #     self.logger.info("Overwriting...")
        #     sdf.repartition(1).write.parquet(
        #         path=OUTPUT_PATH,
        #         mode="overwrite",
        #     )
        #     self.logger.info(f"Done! Results -> {OUTPUT_PATH}")

        job_end = datetime.now()
        self.logger.info(f"Job execution time: {job_end - job_start}")

    def __move_data__(self):
        "Moves data between DWH layers. This method not for public calling"

        job_start = datetime.now()

        df = (
            self.spark.read.option("mergeSchema", "true")
            .option("cacheMetadata", "true")
            .parquet("s3a://data-ice-lake-04/messager-data/analytics/geo-events")
        )
        df = df.repartition(56)

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
            .withColumn("date", f.date_format("datetime", "yyyy-MM-dd"))
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

        tgt_path = "s3a://data-ice-lake-05/messager-data/analytics/geo-events"
        df.repartition(1).write.parquet(
            path=tgt_path,
            mode="overwrite",
            partitionBy=["event_type", "date"],
            compression="gzip",
        )
        job_end = datetime.now()
        self.logger.info(f"Job execution time: {job_end - job_start}")