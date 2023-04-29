import re
import sys
from pathlib import Path

from pydantic import BaseModel

sys.path.append(str(Path(__file__).parent.parent))
from src.logger import SparkLogger

logger = SparkLogger().get_logger(logger_name=str(Path(Path(__file__).name)))


class TagsJobArgsHolder(BaseModel):
    """Holds `tags-job` Spark job arguments \n

    ## Arguments:
        `date` (str): Report start date. Format: YYYY-MM-DD\n
        `depth` (str): Report lenght in days. Report start date minus depth\n
        `threshold` (str): Users threshold\n
        `tags_verified_path` (str): s3 path with  tags_verified dataset. Tags from this dataset will be exluded from result\n
        `src_path` (str): s3 path with source dataset partitions\n
        `tgt_path` (str): s3 path where Spark will store the results
    """

    date: str
    depth: int
    threshold: int
    tags_verified_path: str
    src_path: str
    tgt_path: str


def load_environment(dotenv_file_name: str = ".env") -> bool:
    """Find .env file and load environment variables from it\n
    Will override system environment variables

    Args:
        dotenv_file_name (str, optional): Path to .env file. Defaults to ".env".

    Returns:
        bool: Returns True if found file and loaded variables and False in other case
    """
    from dotenv import find_dotenv, load_dotenv

    logger.info("Loading .env file")
    is_loaded = False
    try:
        load_dotenv(
            dotenv_path=find_dotenv(
                filename=dotenv_file_name, raise_error_if_not_found=True
            ),
            verbose=True,
            override=True,
        )
        is_loaded = True
        logger.info("Done")
    except IOError:
        logger.error(".env file not found!")
        sys.exit(1)

    return is_loaded


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

        logger.info("Validation passed!")
