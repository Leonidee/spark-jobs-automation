import re
import sys
from pathlib import Path

from pydantic import BaseModel

sys.path.append(str(Path(__file__).parent.parent))
from src.logger import SparkLogger

logger = SparkLogger().get_logger(logger_name=str(Path(Path(__file__).name)))


class TagsJobArgsHolder(BaseModel):
    date: str
    depth: int
    threshold: int
    tags_verified_path: str
    src_path: str
    tgt_path: str


class ArgsHolder(BaseModel):
    date: str
    depth: int
    src_path: str
    tgt_path: str
    processed_dttm: str


def load_environment(dotenv_file_name: str = ".env") -> None:
    """Find .env file and load environment variables from it\n
    Will override system environment variables

    Args:
        dotenv_file_name (str, optional): Path to .env file. Defaults to ".env".

    Returns:
        bool: Returns True if found file and loaded variables and False in other case
    """
    from dotenv import find_dotenv, load_dotenv

    logger.info("Loading environment variables")

    logger.debug("Trying to find .env file in project folder")
    try:
        path = find_dotenv(filename=dotenv_file_name, raise_error_if_not_found=True)
        logger.debug("File found")
    except IOError:
        logger.error(".env file not found!")
        sys.exit(1)

    logger.debug("Loading .env file variables")
    load_dotenv(dotenv_path=path, verbose=True, override=True)
    logger.debug("Done")


class SparkArgsValidator:
    def __init__(self):
        ...

    def validate_tags_job_args(self, holder: TagsJobArgsHolder) -> None:
        "Validate given arguments in `TagsJobArgsHolder` object"

        logger.info("Validating `TagsJobArgsHolder`")

        if not re.match(pattern="^\d*$", string=str(holder.depth)):
            raise AssertionError(
                f"`depth` must be int or str(int) type. For example: 5 or '5'. Not `{holder.depth}`"
            )

        if not re.match(pattern="^\d*$", string=str(holder.threshold)):
            raise AssertionError(
                f"`threshold` must be int or str(int) type. For example: 200 or '200'. Not `{holder.threshold}`"
            )

        if not re.match(pattern="^\d{4}-\d{2}-\d{2}$", string=holder.date):
            raise AssertionError(
                f"`date` arg format must be like: `YYYY-MM-DD`. For example: `2022-01-01`. Not `{holder.date}`"
            )

        if int(holder.depth) > 150:
            raise AssertionError("`depth` must be lower that 150")

        if int(holder.threshold) > 5_000:
            raise AssertionError("`threshold` must be lower that 5_000")

        logger.info("Validation passed")
