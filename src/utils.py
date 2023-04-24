import sys
import re
from pathlib import Path
from pydantic import BaseModel
from botocore.exceptions import ClientError
import boto3
import os
from typing import List, Literal, Union
from datetime import datetime, timedelta


sys.path.append(str(Path(__file__).parent.parent))
from src.logger import SparkLogger

logger = SparkLogger().get_logger(logger_name=str(Path(Path(__file__).name)))


class SparkArgsHolder(BaseModel):
    date: str
    depth: str
    threshold: str
    tags_verified_path: str
    src_path: str
    tgt_path: str


def load_environment(dotenv_file_name: str = ".env") -> Union[bool, None]:
    from dotenv import load_dotenv, find_dotenv

    logger.info("Loading .env file.")
    is_loaded = False
    try:
        load_dotenv(
            dotenv_path=find_dotenv(
                filename=dotenv_file_name, raise_error_if_not_found=True
            ),
            verbose=True,
        )
        is_loaded = True
        logger.info("Done.")
    except IOError:
        logger.error(".env file not found!")
        sys.exit(1)

    return is_loaded


def get_s3_instance():
    logger.info("Getting s3 connection instace.")

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
    logger.info("Collecting paths.")

    date = datetime.strptime(date, "%Y-%m-%d").date()
    depth = int(depth)

    paths = [
        f"{src_path}/event_type={event_type}/date=" + str(date - timedelta(days=i))
        for i in range(depth)
    ]

    logger.info("Checking if each path exists on s3.")

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
                logger.info(f"No data for `{path}` on s3. Skipping.")
        except ClientError as e:
            logger.exception(e)
            sys.exit(1)

    # if returns empty list - exit
    if existing_paths == []:
        logger.error("There is no data for given arguments!")
        sys.exit(1)

    logger.info(f"Done with {len(existing_paths)} paths in total.")

    return existing_paths


def validate_job_submit_args(date: str, depth: int, threshold: int) -> None:
    logger.info("Validating given arguments: `date`, `depth` and `threshold`.")

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
        raise AssertionError("`depth` must be lower that 150.")

    if int(threshold) > 5_000:
        raise AssertionError("`threshold` must be lower that 5.000.")

    logger.info("Validation comleted.")
