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
from src.submitter import (
    UnableToSubmitJob,
    UnableToSendRequest,
    UnableToGetResponse,
)


class TestSubmitJob:
    @patch("src.submitter.submitter.requests.post")
    def test_all_success(self, mock_post, submitter, keeper, test_job_name):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = dict(
            returncode=0, stdout="test", stderr="test"
        )
        mock_post.return_value = mock_response

        assert submitter.submit_job(job=test_job_name, keeper=keeper) == True

    @patch("src.submitter.submitter.requests.post")
    def test_raises_if_timeout_error(self, mock_post, submitter, keeper, test_job_name):
        mock_post.side_effect = Timeout

        with pytest.raises(UnableToSendRequest) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type == UnableToSendRequest
        assert "Timeout error" in str(e.value)

    @patch("src.submitter.submitter.requests.post")
    def test_raises_if_url_error(self, mock_post, submitter, keeper, test_job_name):
        mock_post.side_effect = (InvalidSchema, InvalidURL, MissingSchema)

        with pytest.raises(UnableToSendRequest) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type == UnableToSendRequest
        assert (
            "Invalid url or schema provided. Please check 'CLUSTER_API_BASE_URL' environ variable"
            in str(e.value)
        )

    @patch("src.submitter.submitter.requests.post")
    def test_retries_and_raises_if_http_error(
        self, mock_post, submitter, keeper, test_job_name
    ):
        mock_post.side_effect = (
            HTTPError,
            ConnectionError,
            ConnectionError,
        )  # 3 exceptions raised because '_MAX_RETRIES' constant set to 3 by default

        with pytest.raises(UnableToSendRequest) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type == UnableToSendRequest
        assert "Unable to send request to API and no more retries left" in str(e.value)

    @patch("src.submitter.submitter.requests.post")
    def test_raises_if_unable_to_decode_response(
        self, mock_post, submitter, keeper, test_job_name
    ):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = JSONDecodeError("Invalid JSON", "test", 0)

        mock_post.return_value = mock_response

        with pytest.raises(UnableToGetResponse) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type == UnableToGetResponse
        assert "Unable to decode API reponse" in str(e.value)

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

        assert e.type == UnableToSubmitJob
        assert (
            f"Unable to submit {test_job_name} job! API returned 1 code. See job output in logs"
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

        assert e.type == UnableToSubmitJob
        assert f"Unable to submit {test_job_name} job" in str(e.value)

    @patch("src.submitter.submitter.requests.post")
    def test_raises_if_invalie_status_code(
        self, mock_post, submitter, keeper, test_job_name
    ):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_post.return_value = mock_response

        with pytest.raises(UnableToGetResponse) as e:
            submitter.submit_job(job=test_job_name, keeper=keeper)

        assert e.type == UnableToGetResponse
        assert "API response status code: 404" in str(e.value)
