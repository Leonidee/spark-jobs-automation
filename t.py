if __name__ == "__main__":
    from pathlib import Path
    import sys
    import re

    sys.path.append(str(Path(__file__).parent.parent))
    from src.logger import SparkLogger
    from pydantic import BaseModel, validator, ValidationError

    class ArgsKeeper(BaseModel):
        "Dataclass for keeping Spark job parameters"
        date: str
        depth: int
        src_path: str
        tgt_path: str
        processed_dttm: str = None  # type:ignore

        @validator("date")
        def validate_date(cls, v) -> None:
            if not re.match(pattern=r"^\d{4}-\d{2}-\d{2}$", string=v):
                raise ValueError("must be '%Y-%m-%d' format")

        @validator("depth")
        def validate_depth(cls, v) -> None:
            if v > 150:
                raise ValueError("must be lower than 150")

        @validator("src_path")
        def validate_src_path(cls, v) -> None:
            if "s3" not in v:
                raise ValueError("must be only s3 paths")

        @validator("tgt_path")
        def validate_tgt_path(cls, v) -> None:
            if "s3" not in v:
                raise ValueError("must be only s3 paths")

        @validator("processed_dttm")
        def validate_processed_dttm(cls, v) -> None:
            if v is not None:
                if not re.match(
                    pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", string=v
                ):
                    raise ValueError("must be '%Y-%m-%dT%H:%M:%S' format")

    logger = SparkLogger(level="DEBUG").get_logger(logger_name=__name__)
    try:
        keeper = ArgsKeeper(
            date="2022-04-03",
            depth=10,
            src_path="s3a://data-ice-lake-05/messager-data/analytics/geo-events",
            tgt_path="s3a://data-ice-lake-05/messager-data/analytics/geo-events",
            processed_dttm="2023-05-22T12:03:25",
        )
    except ValidationError as e:
        logger.error(e)
