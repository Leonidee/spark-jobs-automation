from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from typing import Literal, Tuple

    from botocore.client import BaseClient

    from src.keeper import ArgsKeeper


# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.config import Config
from src.environ import EnvironManager
from src.helper.exceptions import S3ServiceError
from src.logger import SparkLogger


class SparkHelper:
    """Helper class for Apache Spark"""

    __slots__ = (
        "config",
        "logger",
        "s3",
        "AWS_ENDPOINT_URL",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "HADOOP_CONF_DIR",
        "YARN_CONF_DIR",
        "JAVA_HOME",
        "SPARK_HOME",
        "PYTHONPATH",
    )

    def __init__(self) -> None:
        self.config = Config("config.yaml")

        self.logger = SparkLogger().get_logger(name=__name__)

        environ = EnvironManager()
        environ.load_environ()

        _REQUIRED_VARS = (
            "AWS_ENDPOINT_URL",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "HADOOP_CONF_DIR",
            "YARN_CONF_DIR",
            "JAVA_HOME",
            "SPARK_HOME",
            "PYTHONPATH",
        )

        environ.check_environ(var=_REQUIRED_VARS)

        (
            self.AWS_ENDPOINT_URL,
            self.AWS_ACCESS_KEY_ID,
            self.AWS_SECRET_ACCESS_KEY,
            self.HADOOP_CONF_DIR,
            self.YARN_CONF_DIR,
            self.JAVA_HOME,
            self.SPARK_HOME,
            self.PYTHONPATH,
        ) = map(os.getenv, _REQUIRED_VARS)

        self.s3 = self._get_s3_instance()

    def _get_s3_instance(self) -> BaseClient:
        "Gets ready-to-use boto3 connection instance for communication with s3 service"

        self.logger.debug("Getting boto3 instance")

        s3 = boto3.session.Session().client(  # type: ignore
            service_name="s3",
            endpoint_url=self.AWS_ENDPOINT_URL,
            aws_access_key_id=self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
        )
        self.logger.debug(f"Success. Boto3 instance: {s3}")

        return s3

    def check_s3_object_existence(
        self, key: str, type: Literal["object", "bucket"]
    ) -> bool:
        """Will check if given key exists on S3.

        ## Parameters
        `key` : Key to check.
            If you check a 'bucket' type should be just name of bucket.
            If some 'object' it must be full path to, for example: `s3a://data-ice-lake-05/messager-data/...`

        `type` : Type of key to check. Must be 'object' or 'bucket'.

        ## Raises
        `S3ServiceError` : Raises if bucket not exitsts on S3 (when `bucket` type specified) or `botocore.exceptions.ClientError` occured in runtime.
        `KeyError` : If wrong `type` specified.

        ## Returns
        `bool` : `True` if key exists and `False` if not.

        ## Examples
        >>> helper = SparkHelper()

        Check existing bucket:
        >>> key = "s3a://data-ice-lake-05/messager-data/analytics/geo-events/event_type=message/date=2022-01-20"
        >>> result = helper.check_s3_object_existence(key=key.split(sep="/")[2], type='bucket')
        >>> print(result)
        True

        Check non-existing bucket:
        >>> key = "s3a://data-ice-lake-10/messager-data/analytics/geo-events/event_type=message/date=2022-01-20"
        >>> result = helper.check_s3_object_existence(key=key.split(sep="/")[2], type='bucket')
        S3ServiceError: Bucket 'data-ice-lake-10' does not exists

        Check existing object:
        >>> key = "s3a://data-ice-lake-05/messager-data/analytics/geo-events/event_type=message/date=2022-01-20"
        >>> result = helper.check_s3_object_existence(key=key, type='object')
        >>> print(result)
        True

        Check non-existing object:
        >>> key = "s3a://data-ice-lake-05/messager-data/analytics/geo-events/event_type=message/date=2018-01-01"
        >>> result = helper.check_s3_object_existence(key=key, type='object')
        >>> print(result)
        False
        """
        if type not in ("object", "bucket"):
            raise KeyError("Only 'object' or 'bucket' are allowed as 'type'")

        self.logger.debug(f"Checking '{key}' {type} existence on S3")

        if type == "object":
            try:
                response = self.s3.list_objects_v2(
                    Bucket=key.split(sep="/")[2],
                    MaxKeys=1,
                    Prefix="/".join(key.split(sep="/")[3:]),
                )
                if "Contents" in response.keys():
                    self.logger.debug("OK")
                    self.logger.debug(
                        f"Key has {response.get('KeyCount')} objects inside"
                    )

                    return True

                else:
                    return False

            except ClientError as err:
                raise S3ServiceError(str(err))

        if type == "bucket":
            try:
                self.s3.head_bucket(Bucket=key)
                self.logger.debug("OK")

                return True

            except ClientError as err:
                if "HeadBucket operation: Not Found" in str(err):
                    raise S3ServiceError(f"Bucket '{key}' does not exists")
                else:
                    raise S3ServiceError(str(err))

    def _get_src_paths(
        self,
        event_type: Literal["message", "reaction", "subscription"],
        keeper: ArgsKeeper,
    ) -> Tuple[str]:
        """Get S3 paths contains dataset partitions.

        Collects paths corresponding to the passed in `keeper` object arguments and checks if each path exists on S3. Collects only existing paths.

        If no paths for given arguments raises.

        ## Parameters
        `event_type` : Event type of partitions.

        `keeper` : `ArgsKeeper` class instance with Spark Job arguments.

        ## Raises
        `S3ServiceError` : If no paths for given arguments was found on S3.

        ## Returns
        `Tuple[str]` : Tuple with unique existing partition paths on S3.

        ## Examples
        >>> keeper = ArgsKeeper(
        ...     date="2022-04-26",
        ...     depth=10,
        ...     src_path = "s3a://data-ice-lake-04/messager-data/analytics/geo-events",
        ...     ...,
        ...     )
        >>> paths = helper._get_src_paths(event_type='message', keeper=keeper)
        >>> print(len(paths))
        10
        >>> print(paths[0])
        s3a://data-ice-lake-04/messager-data/analytics/geo-events/event_type=message/date=2022-04-26
        """
        self.logger.debug(f"Collecting src paths for '{event_type}' event type")
        self.logger.debug(f"Source path: '{keeper.src_path}'")

        date = datetime.strptime(keeper.date, r"%Y-%m-%d").date()

        paths = (
            f"{keeper.src_path}/event_type={event_type}/date="
            + str(date - timedelta(days=i))
            for i in range(int(keeper.depth))
        )

        self.logger.debug("Checking if each path exists on S3")

        existing_paths = set()

        for path in paths:
            if self.check_s3_object_existence(key=path, type="object"):
                existing_paths.add(path)
            else:
                self.logger.debug(f"No data for '{path}' path. Skipping")
                continue

        if not existing_paths:
            raise S3ServiceError("No data on S3 for given arguments")

        self.logger.debug(f"Done. {len(existing_paths)} paths collected")

        return tuple(existing_paths)
