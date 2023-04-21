import sys
import re
from pathlib import Path
from pydantic import BaseModel
from botocore.exceptions import ClientError
import boto3
import os
from typing import List, Literal
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv

from log import SparkLogger

logger = SparkLogger.get_logger(logger_name=str(Path(Path(__file__).name)))

load_dotenv(dotenv_path=find_dotenv(raise_error_if_not_found=True), verbose=True)

s3 = boto3.session.Session().client(
    service_name="s3",
    endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)


class SparkArgsHolder(BaseModel):
    date: str
    depth: str
    threshold: str
    tags_verified_path: str
    src_path: str
    tgt_path: str


def get_src_paths(
    event_type: Literal["message", "reaction", "subscription"],
    date: str,
    depth: int,
    src_path: str,
) -> List[str]:
    logger.info("Collecting paths.")
    date = datetime.strptime(date, "%Y-%m-%d").date()
    paths = [
        f"{src_path}/event_type={event_type}/date=" + str(date - timedelta(days=i))
        for i in range(depth)
    ]
    logger.info("Checking if each path exists on s3.")
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
                print(f"No data for `{path}` key. Skipping...")
                logger.info(f"No data for `{path}` on s3. This key will be skipped.")
        except ClientError as e:
            print(e)
            logger.exeption(e)
            sys.exit(1)
    logger.info(f"Done with {len(existing_paths)} paths in total.")

    return existing_paths


def assert_args(date: str, depth: int, threshold: int) -> None:
    logger.info("Asserting given arguments: `date`, `depth` and `threshold`.")

    if not isinstance(date, str):
        logger.exception("Wrong arg format detected. `date` arg must be string type.")
        raise AssertionError("`date` arg must be string type.")

    if not re.match(pattern="^\d*$", string=str(depth)):
        logger.exception(
            "Wrong arg format detected. `depth` must be int or str(int) type"
        )
        raise AssertionError(
            f"`depth` must be int or str(int) type. For example: 5 or '5'. Not `{depth}`"
        )

    if not re.match(pattern="^\d*$", string=str(threshold)):
        logger.exception(
            "Wrong arg format detected. `threshold` must be int or str(int) type."
        )
        raise AssertionError(
            f"`threshold` must be int or str(int) type. For example: 200 or '200'. Not `{threshold}`"
        )

    if not re.match(pattern="^\d{4}-\d{2}-\d{2}$", string=date):
        logger.exception(
            "Wrong arg format detected. `date` arg format must be like: `YYYY-MM-DD`."
        )
        raise AssertionError(
            f"`date` arg format must be like: `YYYY-MM-DD`. For example: `2022-01-01`. Not `{date}`"
        )

    if int(depth) > 150:
        logger.exception("Wrong arg format detected. `depth` must be lower that 150.")
        raise AssertionError("`depth` must be lower that 150.")

    if int(threshold) > 5_000:
        logger.exception(
            "Wrong arg format detected. `threshold` must be lower that 5.000."
        )
        raise AssertionError("`threshold` must be lower that 5.000.")

    logger.info("Assertion comleted.")
