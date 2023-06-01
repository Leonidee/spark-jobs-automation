import os
import sys
from pathlib import Path

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
from src.submitter import SparkSubmitter
from src.submitter import UnableToSubmitJob


# TODO move it to conftest.py
@pytest.fixture
def submitter():
    os.environ["CLUSTER_API_BASE_URL"] = "http://example.com"
    return SparkSubmitter(session_timeout=1)


@pytest.fixture
def test_job_name():
    return "users_info_datamart_job"


class TestSubmitJob:
    @patch("src.submitter.submitter.requests.post")
    def test_all_success(self, mock_post, submitter, keeper, test_job_name):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = dict(
            returncode=0, stdout="some_value", stderr=""
        )
        mock_post.return_value = mock_response

        assert submitter.submit_job(job=test_job_name, keeper=keeper) == True

    @patch("src.submitter.submitter.requests.post")
    def test_raises_if_timeout_error(self, mock_post, submitter, keeper, test_job_name):
        mock_post.side_effect = Timeout

        with pytest.raises(UnableToSubmitJob) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type == UnableToSubmitJob

    @patch("src.submitter.submitter.requests.post")
    def test_raises_if_url_error(self, mock_post, submitter, keeper, test_job_name):
        mock_post.side_effect = (InvalidSchema, InvalidURL, MissingSchema)

        with pytest.raises(UnableToSubmitJob) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type == UnableToSubmitJob

    @pytest.mark.actual
    @patch("src.submitter.submitter.requests.post")
    def test_retries_and_raises_if_http_error(
        self, mock_post, submitter, keeper, test_job_name
    ):
        mock_post.side_effect = (
            HTTPError,
            ConnectionError,
            ConnectionError,
        )  # 3 exceptions because we '_MAX_RETRIES' set to 3 by default

        with pytest.raises(UnableToSubmitJob) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type == UnableToSubmitJob
