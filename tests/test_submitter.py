import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from requests.exceptions import HTTPError

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.config.config import Config
from src.submitter.submitter import SparkSubmitter
from src.utils import TagsJobArgsHolder

spark = SparkSubmitter()
config = Config()
tags_job_args = TagsJobArgsHolder(
    date="2022-03-24",
    depth=7,
    threshold=50,
    tags_verified_path=config.tags_job_config["TAGS_VERIFIED_PATH"],
    src_path=config.tags_job_config["SRC_PATH"],
    tgt_path=config.tags_job_config["TGT_PATH"],
)


# * Type: class
# * Name: SparkSubmitter
@patch("src.submitter.post")
def test_spark_submit_tags_job_if_ok(mock_request) -> None:
    "Test `SparkSubmitter.sumbit_tags_job()` return None if success"

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "returncode": 0,
    }
    mock_request.return_value = mock_response

    assert spark.submit_tags_job(holder=tags_job_args) == None


@patch("src.submitter.post")
def test_spark_submit_tags_job_exit_if_error(mock_request) -> None:
    "Test `SparkSubmitter.sumbit_tags_job()` exit if `HTTPError` occured"

    mock_response = Mock()
    mock_response.raise_for_status.side_effect = HTTPError()

    mock_request.return_value = mock_response

    with pytest.raises(SystemExit):
        spark.submit_tags_job(holder=tags_job_args)


@patch("src.submitter.post")
def test_spark_submit_tags_job_exit_if_nonzero_code(mock_request) -> None:
    "Test `SparkSubmitter.sumbit_tags_job()` exit if API returned non-zero code"

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "returncode": 1,
    }
    mock_request.return_value = mock_response

    with pytest.raises(SystemExit):
        spark.submit_tags_job(holder=tags_job_args)
