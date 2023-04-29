import os
import re
import sys
from datetime import datetime, timedelta
from logging import getLogger
from pathlib import Path
from typing import List, Literal

import boto3
from botocore.exceptions import ClientError
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
    holder: TagsJobArgsHolder,
) -> List[str]:
    """Get S3 paths contains dataset partitions for given arguments

    Returns:
        List[str]: List of S3 paths
    """
    logger.info("Collecting src paths")

    date = datetime.strptime(holder.date, "%Y-%m-%d").date()

    paths = [
        f"{holder.src_path}/event_type={event_type}/date="
        + str(date - timedelta(days=i))
        for i in range(int(holder.depth))
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


class SparkArgsValidator:
    def __init__(self):
        ...

    def validate_tags_job_args(self, holder: TagsJobArgsHolder) -> None:
        "Validate `tags-job` arguments for `spark-submit` command"

        logger.info("Validating given arguments: `date`, `depth` and `threshold`")

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
