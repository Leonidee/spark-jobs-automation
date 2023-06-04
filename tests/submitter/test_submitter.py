import sys
from pathlib import Path
import os
import random

from requests.exceptions import (
    ConnectionError,
    HTTPError,
    InvalidSchema,
    Timeout,
    InvalidURL,
    MissingSchema,
    JSONDecodeError,
)

# tests
import pytest
from unittest.mock import patch, MagicMock

# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.submitter import (
    UnableToSubmitJob,
    UnableToSendRequest,
    UnableToGetResponse,
    SparkSubmitter,
)
from src.keeper import ArgsKeeper


@pytest.fixture
def submitter():
    os.environ["CLUSTER_API_BASE_URL"] = "http://example.com"
    return SparkSubmitter(session_timeout=1, retry_delay=1)


@pytest.fixture
def test_job_name():
    return "users_info_datamart_job"


@pytest.fixture
def keeper() -> ArgsKeeper:
    """Returns instance of `ArgsKeeper` dataclass with filled valid parameters"""
    keeper = ArgsKeeper(
        date="2022-04-03",
        depth=10,
        src_path="s3a://...",
        tgt_path="s3a://...",
        processed_dttm="2023-05-22T12:03:25",
    )
    return keeper


class TestSubmitJob:
    @patch("src.submitter.submitter.requests.post")
    def test_all_success(self, mock_post, submitter, keeper, test_job_name):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = dict(
            returncode=0, stdout="test", stderr="test"
        )
        mock_post.return_value = mock_response

        assert submitter.submit_job(job=test_job_name, keeper=keeper) is True

    @patch("src.submitter.submitter.requests.post")
    def test_raises_if_timeout_error(self, mock_post, submitter, keeper, test_job_name):
        err_msg = "Timeout error"
        mock_post.side_effect = Timeout(err_msg)

        with pytest.raises(UnableToSendRequest) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type is UnableToSendRequest
        assert f"{err_msg}. Unable to submit '{test_job_name}' job." in str(e.value)

    @patch("src.submitter.submitter.requests.post")
    def test_raises_if_schema_error(self, mock_post, submitter, keeper, test_job_name):
        err_msg = "Some schema error"

        mock_post.side_effect = random.choice(
            (InvalidSchema(err_msg), InvalidURL(err_msg), MissingSchema(err_msg))
        )

        with pytest.raises(UnableToSendRequest) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type is UnableToSendRequest
        assert (
            f"{err_msg}. Please check 'CLUSTER_API_BASE_URL' environ variable"
            in str(e.value)
        )

    @patch("src.submitter.submitter.requests.post")
    def test_retries_and_raises_if_http_errors(
        self, mock_post, submitter, keeper, test_job_name
    ):
        err_msg = "Some HTTP error"

        mock_post.side_effect = (
            HTTPError(err_msg),
            ConnectionError(err_msg),
            ConnectionError(err_msg),
        )  # 3 exceptions raised because '_MAX_RETRIES' constant set to 3 by default

        with pytest.raises(UnableToSendRequest) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type is UnableToSendRequest
        assert err_msg in str(e.value)

    @patch("src.submitter.submitter.requests.post")
    def test_raises_if_unable_to_decode_response(
        self, mock_post, submitter, keeper, test_job_name
    ):
        err_msg = "JSON decode error"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = JSONDecodeError(err_msg, "test", 0)

        mock_post.return_value = mock_response

        with pytest.raises(UnableToGetResponse) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type is UnableToGetResponse
        assert err_msg in str(e.value)

    @patch("src.submitter.submitter.requests.post")
    def test_raises_if_1_returncode(self, mock_post, submitter, keeper, test_job_name):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "returncode": 1,
            "stdout": "test",
            "stderr": "test",
        }
        mock_post.return_value = mock_response

        with pytest.raises(UnableToSubmitJob) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type is UnableToSubmitJob
        assert (
            f"Unable to submit '{test_job_name}' job! API returned 1 code. See job output in logs"
            in str(e.value)
        )

    @patch("src.submitter.submitter.requests.post")
    def test_raises_if_nonzero_returncode(
        self, mock_post, submitter, keeper, test_job_name
    ):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "returncode": 2,
            "stdout": "test",
            "stderr": "test",
        }
        mock_post.return_value = mock_response

        with pytest.raises(UnableToSubmitJob) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type is UnableToSubmitJob
        assert f"Unable to submit '{test_job_name}' job." in str(e.value)

    @patch("src.submitter.submitter.requests.post")
    def test_raises_if_invalid_status_code(
        self, mock_post, submitter, keeper, test_job_name
    ):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_post.return_value = mock_response

        with pytest.raises(UnableToGetResponse) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type is UnableToGetResponse
        assert "API response status code -> 404" in str(e.value)
