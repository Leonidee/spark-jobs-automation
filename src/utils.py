import re
import sys
from pathlib import Path

from pydantic import BaseModel

sys.path.append(str(Path(__file__).parent.parent))
from src.logger import SparkLogger
from src.config import Config


class SparkConfigKeeper(BaseModel):
    """Dataclass for keeping Spark configuration properties

    ## Properties
    `executor_memory` : spark.executor.memory

    `executor_cores` : spark.executor.cores

    `max_executors_num` : spark.dynamicAllocation.maxExecutors

    ## Notes
    For more information about Spark Properties see -> https://spark.apache.org/docs/latest/configuration.html#application-properties
    """

    executor_memory: str
    executor_cores: int
    max_executors_num: int


class ArgsKeeper(BaseModel):
    "Dataclass for keeping Spark job parameters"
    date: str
    depth: int
    src_path: str
    tgt_path: str
    processed_dttm: str


class EnvironManager:
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

        Overrides system environment variables.

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


# class SparkArgsValidator:
#     def __init__(self):
#         ...

#     def validate_tags_job_args(self, holder: ArgsKeeper) -> None:
#         "Validate given arguments in `TagsJobArgsHolder` object"

#         logger.info("Validating `TagsJobArgsHolder`")

#         if not re.match(pattern="^\d*$", string=str(holder.depth)):
#             raise AssertionError(
#                 f"`depth` must be int or str(int) type. For example: 5 or '5'. Not `{holder.depth}`"
#             )

#         if not re.match(pattern="^\d*$", string=str(holder.threshold)):
#             raise AssertionError(
#                 f"`threshold` must be int or str(int) type. For example: 200 or '200'. Not `{holder.threshold}`"
#             )

#         if not re.match(pattern="^\d{4}-\d{2}-\d{2}$", string=holder.date):
#             raise AssertionError(
#                 f"`date` arg format must be like: `YYYY-MM-DD`. For example: `2022-01-01`. Not `{holder.date}`"
#             )

#         if int(holder.depth) > 150:
#             raise AssertionError("`depth` must be lower that 150")

#         if int(holder.threshold) > 5_000:
#             raise AssertionError("`threshold` must be lower that 5_000")

#         logger.info("Validation passed")
