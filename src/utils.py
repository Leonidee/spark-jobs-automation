import sys
import re
from pydantic import BaseModel
from botocore.exceptions import ClientError
import boto3
import os
from typing import List, Literal
from datetime import datetime, timedelta
from logging import getLogger
import yaml
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from src.logger import SparkLogger


class Config:
    def __init__(self) -> None:
        """Project config

        Contains configurations for Spark Jobs and environment
        """
        with open("config.yaml") as f:
            self.config = yaml.safe_load(f)
        self._is_prod = self.config["environ"]["IS_PROD"]

    @property
    def IS_PROD(self) -> bool:
        return self._is_prod

    @IS_PROD.setter
    def IS_PROD(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise ValueError("value must be boolean!")

        self._is_prod = value

    @IS_PROD.deleter
    def IS_PROD(self) -> None:
        self._is_prod = None

    @property
    def tags_job_config(self) -> dict:
        return self.config["spark"]["tags-job"]


config = Config()

logger = (
    getLogger("aiflow.task")
    if config.IS_PROD
    else SparkLogger().get_logger(logger_name=str(Path(Path(__file__).name)))
)


class SparkArgsHolder(BaseModel):
    date: str
    depth: str
    threshold: str
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
    from dotenv import load_dotenv, find_dotenv

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


def get_s3_instance():
    "Get boto3 S3 connection instance"

    logger.info("Getting s3 connection instace")

    load_environment()

    s3 = boto3.session.Session().client(
        service_name="s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    return s3


def get_src_paths(
    event_type: Literal["message", "reaction", "subscription"],
    date: str,
    depth: int,
    src_path: str,
) -> List[str]:
    """Get S3 paths contains dataset partitions for given arguments

    Returns:
        List[str]: List of S3 paths
    """
    logger.info("Collecting src paths")

    date = datetime.strptime(date, "%Y-%m-%d").date()
    depth = int(depth)

    paths = [
        f"{src_path}/event_type={event_type}/date=" + str(date - timedelta(days=i))
        for i in range(depth)
    ]

    logger.info("Checking if each path exists on s3")

    s3 = get_s3_instance()

    existing_paths = []
    for path in paths:
        try:
            response = s3.list_objects(
                Bucket=path.split(sep="/")[2],
                MaxKeys=5,
                Prefix="/".join(path.split(sep="/")[3:]),
            )
            if "Contents" in response.keys():
                existing_paths.append(path)
            else:
                logger.info(f"No data for `{path}` on s3. Skipping")
        except ClientError as e:
            logger.exception(e)
            sys.exit(1)

    # if returns empty list - exit
    if existing_paths == []:
        logger.error("There is no data for given arguments!")
        sys.exit(1)

    logger.info(f"Done with {len(existing_paths)} paths in total")

    return existing_paths


def validate_job_submit_args(date: str, depth: int, threshold: int) -> None:
    "Validate `tags-job` arguments for `spark-submit` command"

    logger.info("Validating given arguments: `date`, `depth` and `threshold`")

    if not re.match(pattern="^\d*$", string=str(depth)):
        raise AssertionError(
            f"`depth` must be int or str(int) type. For example: 5 or '5'. Not `{depth}`"
        )

    if not re.match(pattern="^\d*$", string=str(threshold)):
        raise AssertionError(
            f"`threshold` must be int or str(int) type. For example: 200 or '200'. Not `{threshold}`"
        )

    if not re.match(pattern="^\d{4}-\d{2}-\d{2}$", string=date):
        raise AssertionError(
            f"`date` arg format must be like: `YYYY-MM-DD`. For example: `2022-01-01`. Not `{date}`"
        )

    if int(depth) > 150:
        raise AssertionError("`depth` must be lower that 150")

    if int(threshold) > 5_000:
        raise AssertionError("`threshold` must be lower that 5_000")

    logger.info("Validation passed!")
