import re

from pydantic import BaseModel, validator


class SparkConfigKeeper(BaseModel):
    """Dataclass for keeping Spark configuration properties

    ## Properties
    `executor_memory` : spark.executor.memory\n
    `executor_cores` : spark.executor.cores\n
    `max_executors_num` : spark.dynamicAllocation.maxExecutors\n

    ## Notes
    For more information about Spark Properties see -> https://spark.apache.org/docs/latest/configuration.html#application-properties
    """

    executor_memory: str
    executor_cores: int
    max_executors_num: int


class ArgsKeeper(BaseModel):
    """Dataclass for keeping and validating Spark job parameters

    ## Parameters
    `date`: The starting date from which the 'depth' argument is subtracted. Format: `%Y-%m-%d`\n
    `depth`: Datamart calculation depth in days\n
    `src_path`: Path to input data on S3\n
    `tgt_path`: S3 path where collected datamart will be saved\n
    `processed_dttm`: Processed timestamp. Format: `%Y-%m-%dT%H:%M:%S`, defaults `None`

    ## Raises
    `ValueError` : Raises error if parameter don't pass validation

    ## Examples
    >>> keeper = ArgsKeeper(
    ...     date="2022-04-03",
    ...     depth=10,
    ...     src_path="s3a://data-ice-lake-05/messager-data/analytics/geo-events",
    ...     tgt_path="s3a://data-ice-lake-05/messager-data/analytics/tmp",
    ...     processed_dttm="2023-05-22T12:03:25",
    ... )

    If validation don't pass:
    >>> try:
    ...     keeper = ArgsKeeper(
    ...         date="2022-04-03",
    ...         depth=10,
    ...         src_path="s3a://data-ice-lake-05/messager-data/analytics/geo-events",
    ...         tgt_path="s3a://data-ice-lake-05/messager-data/analytics/tmp",
    ...         processed_dttm="2023-05-22", # <----- Ops!
    ...     )
    ... except ValidationError as e:
    ...     logger.error(e)
    [2023-05-26 11:12:23] {src.utils:56} ERROR: 1 validation error for ArgsKeeper
    processed_dttm
        must be '%Y-%m-%dT%H:%M:%S' format (type=value_error)
    """

    date: str
    depth: int
    src_path: str
    tgt_path: str
    processed_dttm: str = None  # type: ignore

    @validator("date")
    def validate_date(cls, v) -> str:
        if not re.match(pattern=r"^\d{4}-\d{2}-\d{2}$", string=v):
            raise ValueError("must be '%Y-%m-%d' format")
        return v

    @validator("depth")
    def validate_depth(cls, v) -> int:
        if v > 150:
            raise ValueError("must be lower than 150")
        if v < 0:
            raise ValueError("must be positive")
        return v

    @validator("src_path")
    def validate_src_path(cls, v) -> str:
        if "s3" not in v:
            raise ValueError("must be only s3 paths")
        return v

    @validator("tgt_path")
    def validate_tgt_path(cls, v) -> str:
        if "s3" not in v:
            raise ValueError("must be only s3 paths")
        return v

    @validator("processed_dttm")
    def validate_processed_dttm(cls, v) -> str:
        if v is not None:
            if not re.match(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", string=v):
                raise ValueError("must be '%Y-%m-%dT%H:%M:%S' format")
        return v
