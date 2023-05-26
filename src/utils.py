import re
import sys
from pathlib import Path

from pydantic import BaseModel, validator

sys.path.append(str(Path(__file__).parent.parent))
from src.logger import SparkLogger
from src.config import Config


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


class EnvironManager:
    """Project environment manager.

    ## Examples
    Initialize Class instance:
    >>> env = EnvironManager()

    Find .env file in project and load variables from it:
    >>> env.load_environ()
    """

    def __init__(self) -> None:
        config = Config()
        from dotenv import find_dotenv, load_dotenv

        self._find_dotenv = find_dotenv
        self._load_dotenv = load_dotenv
        self.logger = SparkLogger(level=config.python_log_level).get_logger(
            logger_name=__name__
        )

    def load_environ(self, dotenv_file_name: str = ".env") -> bool:
        """Find .env file and load environment variables from it.

        ## Notes
        Overrides system environment variables with same names.

        `.env` file should located in root project directory. Notice the example one: `./.env.template`

        ## Parameters
        `dotenv_file_name` : Path to .env file, by default ".env"

        ## Returns
        `bool` : Returns True if found file and loaded variables and False in other case
        """
        self.logger.debug("Loading environ")

        self.logger.debug("Trying to find .env in project")
        try:
            path = self._find_dotenv(
                filename=dotenv_file_name, raise_error_if_not_found=True
            )
            self.logger.debug("File found")
        except IOError:
            self.logger.critical(".env file not found. Environ not loaded!")
            return False

        self.logger.debug("Reading .env file")
        try:
            self._load_dotenv(dotenv_path=path, verbose=True, override=True)
        except IOError:
            self.logger.critical("Enable to read .env file. Environ not loaded!")
            return False

        self.logger.debug("Environ loaded")
        return True
