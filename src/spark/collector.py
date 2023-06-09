from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal, Tuple

    import pyspark

    from src.keeper import ArgsKeeper, SparkConfigKeeper

from typing import Literal
from src.keeper import SparkConfigKeeper

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.spark.runner import SparkRunner
from src.logger import SparkLogger


class DatamartCollector(SparkRunner):
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
        return super().init_session(app_name, spark_conf, log4j_level)

    def stop_session(self) -> None:
        return super().stop_session()

    def _compute_distances(
        self, df: pyspark.sql.DataFrame, coord_cols_prefix: Tuple[str, str]
    ) -> pyspark.sql.DataFrame:
        """Compute distance between two point for each row of DataFrame

        ## Parameters
        `dataframe` : `pyspark.sql.DataFrame`
        `coord_cols_prefix` : Tuple with prefix of columns names with coordinates. Must be exactly two.

        For example `('city', 'event')`. This means that DataFrame contains columns city_lat, city_lon, event_lat and event_lon with the corresponding coordinates

        ## Returns
        `pyspark.sql.DataFrame` : DataFrame with additional column `distance` which contains distance between two columns

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
        ...            coord_cols_prefix=("event", "city"),
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
        self.logger.debug("Computing distances")

        import pyspark.sql.functions as F  # type: ignore

        self.logger.debug(f"Given 'coord_cols_prefix': {coord_cols_prefix}")

        if len(coord_cols_prefix) > 2:
            raise IndexError(
                "Only two values are allowed for 'coord_cols_prefix' argument"
            )

        cols = ((col + "_lat", col + "_lon") for col in coord_cols_prefix)

        lat_1, lon_1 = next(cols)  # latitude and longitude of first point
        lat_2, lon_2 = next(cols)  # same for second point

        self.logger.debug("Checking coordinates columns existance")

        if not all(col in df.columns for col in (lat_1, lon_1, lat_2, lon_2)):
            raise KeyError(
                "DataFrame should contains coordinates columns with names listed in 'coord_cols_prefix' argument"
            )
        self.logger.debug("OK")

        self.logger.debug("Processing")

        # Computations itself
        # I splited them into parts
        distance_lat = F.radians(F.col(lat_2)) - F.radians(F.col(lat_1))
        distance_lon = F.radians(F.col(lon_2)) - F.radians(F.col(lon_1))

        part_one = (
            F.sin(distance_lat / 2) ** 2
            + F.cos(F.radians(F.col(lat_1)))
            * F.cos(F.radians(F.col(lat_2)))
            * F.sin(distance_lon / 2) ** 2
        )
        part_two = F.sin(F.sqrt(part_one))  # type: ignore
        distance = 2 * 6371 * part_two  # type: ignore

        return df.withColumn("distance", F.round(distance, 0))

    def _get_cities_coords_df(self, keeper: ArgsKeeper) -> pyspark.sql.DataFrame:
        return self.spark.read.parquet(keeper.coords_path)  # type: ignore

    def _add_event_location_to_df(
        self,
        df: pyspark.sql.DataFrame,
        cities_coord_df: pyspark.sql.DataFrame,
        event: Literal["message", "reaction", "subscription", "registration"],
    ) -> pyspark.sql.DataFrame:
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

        self.logger.debug(f"Getting event location for '{event}' event type")

        from pyspark.sql import Window as W  # type: ignore
        import pyspark.sql.functions as F  # type: ignore

        _PARTITION_BY = (
            ["user_id", "subscription_channel"]
            if event == "subscription"
            else "message_id"
        )
        self.logger.debug(f"Will partition by: {_PARTITION_BY}")

        self.logger.debug("Joining given dataframe with cities coordinates")

        sdf = df.crossJoin(
            cities_coord_df.select("city_id", "city_name", "city_lat", "city_lon")
        )
        sdf = self._compute_distances(
            df=sdf,
            coord_cols_prefix=("event", "city"),
        )
        self.logger.debug(
            "Collecting resulting dataframe of '_get_event_location' function"
        )

        w = W().partitionBy(_PARTITION_BY).orderBy(F.asc("distance"))  # type: ignore

        sdf = (
            sdf.withColumn(
                "city_dist_rnk",
                F.row_number().over(w),
            )
            .where(F.col("city_dist_rnk") == 1)
            .drop(
                "city_lat",
                "city_lon",
                "distance",
                "city_dist_rnk",
            )
        )

        return sdf

    def _get_users_actual_data_df(self, keeper: ArgsKeeper) -> pyspark.sql.DataFrame:
        """Returns dataframe with user information based on sent messages.

        ## Parameters
        `keeper` : `ArgsKeeper` with job arguments.

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
        self.logger.debug("Collecting dataframe of users actual data")

        from pyspark.sql import Window as W  # type: ignore
        import pyspark.sql.functions as F  # type: ignore

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
                F.when(F.col("message_ts").isNotNull(), F.col("message_ts")).otherwise(
                    F.col("datetime")
                ),
            )
            .drop("message_ts", "datetime")
        )
        cities_coords_sdf = self._get_cities_coords_df(keeper=keeper)

        sdf = self._add_event_location_to_df(
            df=sdf,
            cities_coord_df=cities_coords_sdf,
            event="message",
        )
        cities_coords_sdf.show(100)  # todo здесь ошибка
        sdf.show(100)
        # ошибка:
        # [2023-06-09 08:04:16] {__main__:75} ERROR:  Column city_id#180 are ambiguous
        # It's probably because you joined several Datasets together, and some of these Datasets are the same.
        # This column points to one of the Datasets but Spark is unable to figure out which one

        w = W().partitionBy("user_id").orderBy(F.desc("msg_ts"))

        sdf = (
            sdf.withColumn(
                "act_city",
                F.first(col="city_name", ignorenulls=True).over(w),
            )
            .withColumn(
                "act_city_id",
                F.first(col="city_id", ignorenulls=True).over(w),
            )
            .withColumn(
                "last_msg_ts",
                F.first(col="msg_ts", ignorenulls=True).over(w),
            )
            .join(
                cities_coords_sdf.select("city_id", "timezone"),
                on=F.col("act_city_id") == cities_coords_sdf.city_id,
                how="left",
            )
            .withColumn(
                "local_time",
                F.from_utc_timestamp(
                    timestamp=F.col("last_msg_ts"), tz=F.col("timezone")
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

    def collect_users_demographic_dm(self, keeper: ArgsKeeper) -> None:
        """Collect users info datamart and save results on s3

        ## Parameters
        `keeper` : `ArgsKeeper` with job arguments.

        ## Examples
        >>> spark = SparkRunner()
        >>> spark.init_session(app_name="testing-app", spark_conf=conf, log4j_level="INFO")
        >>> spark.collect_users_info_datamart(keeper=keeper)

        Read saved results to see how it looks:
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
        _DATAMART_NAME = "users_demographic_dm"

        self.logger.info(f"Starting collecting '{_DATAMART_NAME}'")

        from pyspark.sql import Window as W  # type: ignore
        import pyspark.sql.functions as F  # type: ignore
        from pyspark.sql.utils import AnalysisException  # type: ignore

        _job_start = datetime.now()

        sdf = self._get_users_actual_data_df(keeper=keeper)

        self.logger.debug("Collecting travels data")

        w = W().partitionBy("user_id").orderBy(F.asc("msg_ts"))

        travels_sdf = (
            sdf.withColumn(
                "prev_city",
                F.lag("city_name").over(w),
            )
            .withColumn(
                "visit_flg",
                F.when(
                    (F.col("city_name") != F.col("prev_city"))
                    | (F.col("prev_city").isNull()),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )
            .where(F.col("visit_flg") == 1)
            .groupby("user_id")
            .agg(
                F.collect_list("city_name").alias("travel_array"),
                F.collect_list("msg_ts").alias("travel_ts_array"),
            )
            .select(
                "user_id",
                "travel_array",
                F.size("travel_array").alias("travel_count"),
                "travel_ts_array",
            )
        )

        self.logger.debug("Collecting users home city")

        w = W().partitionBy("user_id").orderBy(F.asc("travel_ts"))

        home_city_sdf = (
            travels_sdf.withColumn(
                "zipped_array", F.arrays_zip("travel_array", "travel_ts_array")
            )
            .withColumn("upzipped_array", F.explode("zipped_array"))
            .withColumn("travel_city", F.col("upzipped_array").getItem("travel_array"))
            .withColumn("travel_ts", F.col("upzipped_array").getItem("travel_ts_array"))
            .withColumn(
                "prev_travel_ts",
                F.lag("travel_ts").over(w),
            )
            .withColumn(
                "prev_travel_city",
                F.lag("travel_city").over(w),
            )
            .withColumn("diff", F.datediff("travel_ts", "prev_travel_ts"))
            .where(F.col("diff") > 27)
            .withColumn(
                "rnk",
                F.row_number().over(w),
            )
            .where(F.col("rnk") == 1)
            .select("user_id", F.col("prev_travel_city").alias("home_city"))
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

        self.logger.info(f"Datamart '{_DATAMART_NAME}' collected!")

        sdf.show(10)

        self.logger.info("Writing results")

        _PROCESSED_DT = datetime.strptime(
            keeper.processed_dttm.replace("T", " "), r"%Y-%m-%d %H:%M:%S"  # type: ignore
        ).date()

        OUTPUT_PATH = f"{keeper.tgt_path}/{_DATAMART_NAME}/date={_PROCESSED_DT}"

        try:
            sdf.repartition(1).write.parquet(
                path=OUTPUT_PATH,
                mode="errorifexists",
            )
            self.logger.info(f"Done! Results -> {OUTPUT_PATH}")

        except AnalysisException as err:
            self.logger.warning(f"Notice that {str(err)}")
            self.logger.info("Overwriting...")
            sdf.repartition(1).write.parquet(
                path=OUTPUT_PATH,
                mode="overwrite",
            )
            self.logger.info(f"Done! Results -> {OUTPUT_PATH}")

        _job_end = datetime.now()
        self.logger.info(f"Job execution time: {_job_end - _job_start}")

    def collect_events_total_cnt_agg_wk_mnth_dm(self, keeper: ArgsKeeper) -> None:
        """Collects `events_total_cnt_agg_wk_mnth_dm` datamart.

        ## Parameters
        `keeper` : `ArgsKeeper` with job arguments.

        ## Examples
        >>> spark.collect_location_zone_agg_datamart(keeper=keeper) # will save results on s3

        Read saved results to see how it looks:
        >>> sdf = spark.read.parquet(keeper.tgt_path)
        >>> sdf.printSchema()
        root
        |-- zone_id: integer (nullable = false)
        |-- week: date (nullable = false)
        |-- month: date (nullable = false)
        |-- week_message: long (nullable = false)
        |-- week_reaction: long (nullable = false)
        |-- week_subscription: long (nullable = false)
        |-- week_user: long (nullable = false)
        |-- month_message: long (nullable = false)
        |-- month_reaction: long (nullable = false)
        |-- month_subscription: long (nullable = false)
        |-- month_user: long (nullable = false)
        >>> sdf.show()
        +-------+----------+----------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+
        |zone_id|week      |month     |week_message|week_reaction|week_subscription|week_user|month_message|month_reaction|month_subscription|month_user|
        +-------+----------+----------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+
        |1      |2022-02-21|2022-02-01|78          |99           |3713             |77       |105          |123           |4739              |103       |
        |1      |2022-02-28|2022-02-01|27          |24           |1026             |26       |105          |123           |4739              |103       |
        |1      |2022-02-28|2022-03-01|134         |147          |6453             |130      |659          |1098          |45580             |630       |
        |1      |2022-03-07|2022-03-01|145         |224          |8833             |143      |659          |1098          |45580             |630       |
                                                                             ...
        |3      |2022-03-07|2022-03-01|264         |192          |19320            |253      |1190         |1068          |99175             |1121      |
        |3      |2022-03-14|2022-03-01|270         |253          |22658            |254      |1190         |1068          |99175             |1121      |
        |3      |2022-03-21|2022-03-01|258         |300          |26533            |233      |1190         |1068          |99175             |1121      |
        |3      |2022-03-28|2022-03-01|160         |181          |16878            |148      |1190         |1068          |99175             |1121      |
        +-------+----------+----------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+
        """
        _DATAMART_NAME = "events_total_cnt_agg_wk_mnth_dm"

        self.logger.info(f"Staring collecting '{_DATAMART_NAME}'")

        from pyspark.sql import Window  # type: ignore
        import pyspark.sql.functions as F  # type: ignore
        from pyspark.storagelevel import StorageLevel  # type: ignore
        from pyspark.sql.types import (  # type: ignore
            StructType,
            StructField,
            IntegerType,
            DateType,
            LongType,
        )

        _job_start = datetime.now()

        self.logger.debug("Collecing messages data")

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
                F.when(F.col("message_ts").isNotNull(), F.col("message_ts")).otherwise(
                    F.col("datetime")
                ),
            )
            .drop_duplicates(subset=["user_id", "message_id", "msg_ts"])
            .drop("datetime", "message_ts")
        )

        messages_sdf = self._get_event_location(
            dataframe=messages_sdf,
            event_type="message",
            cities_coord_path=keeper.coords_path,  # type: ignore
        )

        messages_sdf = (
            messages_sdf.withColumnRenamed("city_id", "zone_id")
            .withColumn("week", F.trunc(F.col("msg_ts"), "week"))
            .withColumn("month", F.trunc(F.col("msg_ts"), "month"))
            .groupby("month", "week", "zone_id")
            .agg(F.count("message_id").alias("week_message"))
            .withColumn(
                "month_message",
                F.sum(F.col("week_message")).over(
                    Window().partitionBy(F.col("zone_id"), F.col("month"))
                ),
            )
            .persist(storageLevel=StorageLevel.MEMORY_ONLY)
        )

        self.logger.debug("Collecing reacitons data")

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
            .where(F.col("event_lat").isNotNull())
        )
        reaction_sdf = self._get_event_location(
            dataframe=reaction_sdf,
            event_type="reaction",
            cities_coord_path=keeper.coords_path,  # type: ignore
        )
        reaction_sdf = (
            reaction_sdf.withColumnRenamed("city_id", "zone_id")
            .where(F.col("event_lat").isNotNull())
            .withColumn("week", F.trunc(F.col("datetime"), "week"))
            .withColumn("month", F.trunc(F.col("datetime"), "month"))
            .groupby("month", "week", "zone_id")
            .agg(F.count("message_id").alias("week_reaction"))
            .withColumn(
                "month_reaction",
                F.sum(F.col("week_reaction")).over(
                    Window().partitionBy(F.col("zone_id"), F.col("month"))
                ),
            )
            .persist(storageLevel=StorageLevel.MEMORY_ONLY)
        )

        self.logger.debug("Collecing registrations data")

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
                F.when(F.col("message_ts").isNotNull(), F.col("message_ts")).otherwise(
                    F.col("datetime")
                ),
            )
            .drop_duplicates(subset=["user_id", "message_id", "msg_ts"])
            .drop("datetime", "message_ts")
            .withColumn(
                "registration_ts",
                F.first(col="msg_ts", ignorenulls=True).over(
                    Window().partitionBy("user_id").orderBy(F.asc("msg_ts"))
                ),
            )
            .withColumn(
                "is_reg",
                F.when(F.col("registration_ts") == F.col("msg_ts"), F.lit(1)).otherwise(
                    F.lit(0)
                ),
            )
            .where(F.col("is_reg") == F.lit(1))
            .drop("is_reg", "registration_ts")
        )
        registrations_sdf = self._get_event_location(
            dataframe=registrations_sdf,
            event_type="registration",
            cities_coord_path=keeper.coords_path,  # type: ignore
        )

        registrations_sdf = (
            registrations_sdf.withColumnRenamed("city_id", "zone_id")
            .where(F.col("event_lat").isNotNull())
            .withColumn("week", F.trunc(F.col("msg_ts"), "week"))
            .withColumn("month", F.trunc(F.col("msg_ts"), "month"))
            .groupby("month", "week", "zone_id")
            .agg(F.count("user_id").alias("week_user"))
            .withColumn(
                "month_user",
                F.sum(F.col("week_user")).over(
                    Window().partitionBy(F.col("zone_id"), F.col("month"))
                ),
            )
            .persist(storageLevel=StorageLevel.MEMORY_ONLY)
        )

        self.logger.debug("Collecing subscriptions data")

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
            .where(F.col("event_lat").isNotNull())
        )

        subscriptions_sdf = self._get_event_location(
            dataframe=subscriptions_sdf,
            event_type="subscription",
            cities_coord_path=keeper.coords_path,  # type: ignore
        )
        subscriptions_sdf = (
            subscriptions_sdf.withColumnRenamed("city_id", "zone_id")
            .where(F.col("event_lat").isNotNull())
            .withColumn("week", F.trunc(F.col("datetime"), "week"))
            .withColumn("month", F.trunc(F.col("datetime"), "month"))
            .groupby("month", "week", "zone_id")
            .agg(F.count("user_id").alias("week_subscription"))
            .withColumn(
                "month_subscription",
                F.sum(F.col("week_subscription")).over(
                    Window().partitionBy(F.col("zone_id"), F.col("month"))
                ),
            )
            .persist(storageLevel=StorageLevel.MEMORY_ONLY)
        )

        self.logger.debug("Joining dataframes")

        _COLS = ["zone_id", "week", "month"]
        sdf = (
            messages_sdf.join(other=reaction_sdf, on=_COLS)
            .join(other=registrations_sdf, on=_COLS)
            .join(other=subscriptions_sdf, on=_COLS)
            .orderBy(_COLS)  # type: ignore
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
            .dropna()
        )

        for frame in (messages_sdf, reaction_sdf, registrations_sdf, subscriptions_sdf):
            frame.unpersist()

        _SCHEMA = StructType(
            [
                StructField("zone_id", IntegerType(), nullable=False),
                StructField("week", DateType(), nullable=False),
                StructField("month", DateType(), nullable=False),
                StructField("week_message", LongType(), nullable=False),
                StructField("week_reaction", LongType(), nullable=False),
                StructField("week_subscription", LongType(), nullable=False),
                StructField("week_user", LongType(), nullable=False),
                StructField("month_message", LongType(), nullable=False),
                StructField("month_reaction", LongType(), nullable=False),
                StructField("month_subscription", LongType(), nullable=False),
                StructField("month_user", LongType(), nullable=False),
            ]
        )
        sdf = self.spark.createDataFrame(data=sdf.rdd, schema=_SCHEMA)

        self.logger.info(f"Datamart '{_DATAMART_NAME}' collected!")

        sdf.show(100, False)  # TODO remove this
        sdf.printSchema()
        # self.logger.info("Writing results")

        # processed_dt = datetime.strptime(
        #     keeper.processed_dttm.replace("T", " "), r"%Y-%m-%d %H:%M:%S"  # type: ignore
        # ).date()

        # OUTPUT_PATH = f"{keeper.tgt_path}/date={processed_dt}"

        # try:
        #     sdf.repartition(1).write.parquet(
        #         path=OUTPUT_PATH,
        #         mode="errorifexists",
        #     )
        #     self.logger.info(f"Done! Results -> {OUTPUT_PATH}")

        # except AnalysisException as err:
        #     self.logger.warning(f"Notice that {str(err)}")
        #     self.logger.info("Overwriting...")
        #     sdf.repartition(1).write.parquet(
        #         path=OUTPUT_PATH,
        #         mode="overwrite",
        #     )
        #     self.logger.info(f"Done! Results -> {OUTPUT_PATH}")

        _job_end = datetime.now()
        self.logger.info(f"Job execution time: {_job_end - _job_start}")

    def collect_add_to_friends_recommendations_dm(self, keeper: ArgsKeeper) -> None:
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

        from pyspark.sql import Window  # type: ignore
        from pyspark.sql.functions import (  # type: ignore
            array,
            asc,
            col,
            explode,
            first,
            lit,
            when,
        )
        from pyspark.storagelevel import StorageLevel  # type: ignore

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
                explode(array(col("message_from"), col("message_to"))),
            )
            .withColumn(
                "contact_id",
                when(
                    col("user_id") == col("message_from"), col("message_to")
                ).otherwise(col("message_from")),
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
            .where(col("left_user") != col("right_user"))
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
                when(col("message_ts").isNotNull(), col("message_ts")).otherwise(
                    col("datetime")
                ),
            )
            .withColumn(
                "last_msg_ts",
                first(col="msg_ts", ignorenulls=True).over(
                    Window().partitionBy("user_id").orderBy(asc("msg_ts"))
                ),
            )
            .where(col("msg_ts") == col("last_msg_ts"))
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
                    col("user_id"),
                    col("event_lat").alias("left_user_lat"),
                    col("event_lon").alias("left_user_lon"),
                ),
                how="left",
                on=[users_for_rec.left_user == messages_sdf.user_id],
            )
            .drop("user_id")
            .join(
                messages_sdf.select(
                    col("user_id"),
                    col("event_lat").alias("right_user_lat"),
                    col("event_lon").alias("right_user_lon"),
                ),
                how="left",
                on=[users_for_rec.right_user == messages_sdf.user_id],
            )
            .drop("user_id")
            .where(col("left_user_lat").isNotNull())
            .where(col("right_user_lat").isNotNull())
            # .persist(storageLevel=StorageLevel.MEMORY_ONLY)
        )

        sdf = self._compute_distance(
            dataframe=users_for_rec, coord_cols_prefix=("left_user", "right_user")
        )

        users_info_sdf = self._get_users_actual_data_df(keeper=keeper)

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
            .withColumn("processed_dttm", lit(keeper.processed_dttm.replace("T", " ")))  # type: ignore
            .select(
                "left_user",
                "right_user",
                "processed_dttm",
                col("act_city_id").alias("zone_id"),
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
