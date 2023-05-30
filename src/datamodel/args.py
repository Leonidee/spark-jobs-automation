from __future__ import annotations

import re
from pydantic import BaseModel, validator
from datetime import datetime, date


class ArgsKeeper(BaseModel):
    """Dataclass for keeping and validating Spark job parameters

    ## Parameters
    `date`: The starting date from which the 'depth' argument is subtracted. Format: `%Y-%m-%d`\n
    `depth`: Datamart calculation depth in days\n
    `src_path`: Path to input data on S3\n
    `tgt_path`: S3 path where collected datamart will be saved\n
    `processed_dttm`: Processed timestamp. Format: `%Y-%m-%dT%H:%M:%S`, defaults `None`

    ## Raises
    `ValueError` : Raises if parameter don't pass validation
    `UserWarning` : Raises for notify user about potenrial problems

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

    @validator("processed_dttm")
    def validate_processed_dttm(cls, v) -> str:
        if v is not None:
            if not isinstance(v, str):
                raise ValueError("must string")
            if not re.match(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", string=v):
                raise ValueError("must be '%Y-%m-%dT%H:%M:%S' format")
        return v
