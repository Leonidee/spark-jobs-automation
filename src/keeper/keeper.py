from __future__ import annotations

import re
from datetime import date, datetime
from typing import Literal, Union

from pydantic import BaseModel, validator


class ArgsKeeper(BaseModel):
    """Keeping and validating Spark job arguments.

    ## Parameters
    `date`: The starting date from which the 'depth' argument is subtracted. Format: `%Y-%m-%d`\n
    `depth`: Datamart calculation depth in days\n
    `src_path`: Path to input data on S3\n
    `tgt_path`: S3 path where collected datamart will be saved\n
    `processed_dttm`: Processed timestamp. Format: `%Y-%m-%dT%H:%M:%S`, defaults `None`

    ## Raises
    `ValueError` : Raises if parameter don't pass validation\n
    `UserWarning` : Raises for notify user about potenrial problems

    ## Examples
    >>> keeper = ArgsKeeper(
    ...     date="2022-04-03",
    ...     depth=10,
    ...     src_path="s3a://...",
    ...     tgt_path="s3a://...",
    ...     processed_dttm="2023-05-22T12:03:25",
    ... )
    >>> print(keeper)
        Date: 2022-04-03
        Depth: 10
        Source path: s3a://...
        Target path: s3a://...
        Processed time: 2023-05-22T12:03:25

    >>> keeper = ArgsKeeper(
    ...     date="2022-04-03",
    ...     depth=10,
    ...     src_path="s3a://...",
    ...     tgt_path="s3a://...",
    ...     processed_dttm="2023-05-22",  # <----- Ops!
    ... )
    pydantic.error_wrappers.ValidationError: 1 validation error for ArgsKeeper
    processed_dttm
        must be '%Y-%m-%dT%H:%M:%S' format (type=value_error)
    """

    date: str
    depth: int
    src_path: str
    tgt_path: str
    coords_path: Union[str, None] = None
    processed_dttm: Union[str, None] = None

    def __str__(self) -> str:
        return f"\tDate: {self.date}\n\tDepth: {self.depth}\n\tSource path: {self.src_path}\n\tTarget path: {self.tgt_path}\n\tProcessed time: {self.processed_dttm}"

    @validator("date")
    def validate_date(cls, v) -> str:
        if not isinstance(v, str):
            raise ValueError("must string")
        if not re.match(pattern=r"^\d{4}-\d{2}-\d{2}$", string=v):
            raise ValueError("must be '%Y-%m-%d' format")
        if datetime.strptime(v, r"%Y-%m-%d").date() > date.today():
            raise ValueError("date must be earlier than today")
        if datetime.strptime(v, r"%Y-%m-%d").date() < datetime(2015, 1, 1).date():
            raise ValueError("dates earlier than '2015-01-01' not allowed yet")
        if datetime.strptime(v, r"%Y-%m-%d").date() < datetime(2020, 1, 1).date():
            raise UserWarning(f"are you sure that '{v}' date is correct?")
        return v

    @validator("depth")
    def validate_depth(cls, v) -> int:
        if not isinstance(v, int):
            raise ValueError("must string")
        if v > 150:
            raise ValueError("must be lower than 150")
        if v > 100:
            raise UserWarning(
                "are you sure that you want to collect datamart with more than 100 days in depth? It't posible a very large amount of data and affect performance"
            )
        if v < 0:
            raise ValueError("must be positive")
        return v

    @validator("src_path")
    def validate_src_path(cls, v) -> str:
        if not isinstance(v, str):
            raise ValueError("must string")
        if "s3" not in v:
            raise ValueError("only S3 service paths allowed")
        return v

    @validator("tgt_path")
    def validate_tgt_path(cls, v) -> str:
        if not isinstance(v, str):
            raise ValueError("must string")
        if "s3" not in v:
            raise ValueError("only S3 service paths allowed")
        return v

    @validator("coords_path")
    def validate_coords_path(cls, v) -> str:
        if not isinstance(v, str):
            raise ValueError("must string")
        if "s3" not in v:
            raise ValueError("only S3 service paths allowed")
        return v

    @validator("processed_dttm")
    def validate_processed_dttm(cls, v) -> str:
        if v is not None:
            if not isinstance(v, str):
                raise ValueError("must string")
            if not re.match(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", string=v):
                raise ValueError("must be '%Y-%m-%dT%H:%M:%S' format")
        return v


class SparkConfigKeeper(BaseModel):
    """Keeping and validating Spark configuration properties.

    ## Properties
    `executor_memory` : `spark.executor.memory` - Amount of memory to use per executor process, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. 512m, 2g)\n
    `executor_cores` : `spark.executor.cores`\n
    `max_executors_num` : `spark.dynamicAllocation.maxExecutors`\n

    ## Raises
    `ValueError` : Raises if parameter don't pass validation\n
    `UserWarning` : Raises for notify user about potenrial problems

    ## Notes
    For more information about Spark Properties see -> https://spark.apache.org/docs/latest/configuration.html#application-properties
    """

    executor_memory: str
    executor_cores: int
    max_executors_num: int

    def __str__(self) -> str:
        return f"\tspark.executor.memory: {self.executor_memory}\n\tspark.executor.cores: {self.executor_cores}\n\tspark.dynamicAllocation.maxExecutors: {self.max_executors_num}"

    @validator("executor_memory")
    def validate_executor_memory(cls, v) -> str:
        if not isinstance(v, str):
            raise ValueError("must be string")
        if not re.match(pattern=r"^\d+[kmgt]$", string=v):
            raise ValueError(
                'must be in JVM memory strings format with a size unit suffix ("k", "m", "g" or "t") (e.g. 512m, 2g)'
            )
        return v

    @validator("executor_cores")
    def validate_executor_cores(cls, v) -> int:
        if not isinstance(v, int):
            raise ValueError("must be integer")
        if v < 0:
            raise ValueError("must be positive")
        if v > 10:
            raise ValueError("cores per executor must be lower than 10")
        if v > 5:
            raise UserWarning(
                "are you sure that each executor should use more than 5 cores?"
            )

        return v

    @validator("max_executors_num")
    def validate_max_executors_num(cls, v) -> int:
        if not isinstance(v, int):
            raise ValueError("must be integer")
        if v < 0:
            raise ValueError("must be positive")
        if v > 60:
            raise ValueError("must be lower than 60")
        if v > 40:
            raise UserWarning(
                "are you sure that Cluster can allocate more than 40 executors?"
            )

        return v
