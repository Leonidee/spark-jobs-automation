import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    InvalidSchema,
    JSONDecodeError,
    Timeout,
)

# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.helper import S3ServiceError, SparkHelper
from src.keeper import ArgsKeeper


@pytest.fixture
def helper() -> SparkHelper:
    """Returns instance of `SparkHelper` class"""
    return SparkHelper()


@pytest.fixture
def keeper() -> MagicMock:
    """Returns instance of `MagicMock` imulated `ArgsKeeper` object"""
    keeper = MagicMock(
        date="2022-04-03",
        depth=10,
        src_path="s3a://...",
        tgt_path="s3a://...",
        processed_dttm="2023-05-22T12:03:25",
    )
    return keeper


def test_(helper, keeper):
    helper._get_src_paths(event_type="message", keeper=keeper)
