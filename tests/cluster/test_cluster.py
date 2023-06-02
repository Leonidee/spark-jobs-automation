import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import os

import pytest
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    InvalidSchema,
    Timeout,
    JSONDecodeError,
)


# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.cluster import YandexAPIError


class TestGetIAMToken:
    @patch("src.cluster.cluster.requests.post")
    def test_success(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"iamToken": "test_token"}
        mock_post.return_value = mock_response

        result = cluster._get_iam_token()
        assert os.environ["YC_IAM_TOKEN"] == "test_token"
        assert result == True

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_timeout_error(self, mock_post, cluster):
        mock_post.side_effect = [
            Timeout,
            Timeout,
        ]

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 2
            cluster.retry_delay = 1

            cluster._get_iam_token()

        assert e.type == YandexAPIError
        assert "Timeout error. Unable to send request" in str(e.value)

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_invalid_schema_error(self, mock_post, cluster):
        mock_post.side_effect = [
            InvalidSchema,
        ]

        with pytest.raises(YandexAPIError) as e:
            cluster._get_iam_token()

        assert e.type == YandexAPIError
        assert "Invalid url or schema provided" in str(e.value)

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_http_error(self, mock_post, cluster):
        mock_post.side_effect = [HTTPError, ConnectionError, HTTPError]

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3
            cluster.retry_delay = 1

            cluster._get_iam_token()

        assert e.type == YandexAPIError
        assert "Enable to send request to API. Possible because of exception" in str(
            e.value
        )

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_no_token(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"not_token": "some_value"}

        mock_post.return_value = mock_response

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3
            cluster.retry_delay = 1
            cluster._get_iam_token()

        assert e.type == YandexAPIError
        assert "Enable to get IAM token from API response" in str(e.value)

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_unable_to_decode_response(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = JSONDecodeError("Invalid JSON", "test", 0)

        mock_post.return_value = mock_response

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3
            cluster.retry_delay = 1
            cluster._get_iam_token()

        assert e.type == YandexAPIError
        assert "Unable to decode API reponse" in str(e.value)

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_invalid_status_code(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_post.return_value = mock_response

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 2
            cluster.retry_delay = 1
            cluster._get_iam_token()

        assert e.type == YandexAPIError
        assert "Enable to get IAM token from API" in str(e.value)

    @patch("src.cluster.cluster.requests.post")
    def test_if_errors_but_finally_success(self, mock_post, cluster):
        mock_post.side_effect = [
            HTTPError,
            Timeout,
            ConnectionError,
            MagicMock(status_code=200, json=lambda: {"iamToken": "some_value"}),
        ]

        cluster.max_retries = 4
        cluster.retry_delay = 1

        cluster._get_iam_token()
        assert os.environ["YC_IAM_TOKEN"] == "some_value"


class TestExecCommand:
    @patch("src.cluster.cluster.requests.post")
    def test_start_success(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"some_key": "some_value"}

        mock_post.return_value = mock_response

        result = cluster.exec_command("start")

        mock_post.assert_called_once()
        assert result == True

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_timeout_error(self, mock_post, cluster):
        mock_post.side_effect = (
            Timeout,
            Timeout,
            Timeout,
        )

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3
            cluster.retry_delay = 1
            cluster.exec_command(command="start")

        assert e.type == YandexAPIError
        assert "Timeout error. Unable to send request to execute command: start" in str(
            e.value
        )

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_invalid_url_error(self, mock_post, cluster):
        mock_post.side_effect = InvalidSchema

        with pytest.raises(YandexAPIError) as e:
            cluster.exec_command(command="start")

        assert e.type == YandexAPIError
        assert "Invalid url or schema provided" in str(e.value)

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_http_error(self, mock_post, cluster):
        mock_post.side_effect = (HTTPError, HTTPError, ConnectionError)

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3
            cluster.retry_delay = 1
            cluster.exec_command(command="start")

        assert e.type == YandexAPIError
        assert "Enable to send request to API. Possible because of exception" in str(
            e.value
        )

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_invalid_status_code(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 500

        mock_post.return_value = mock_response

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 2
            cluster.retry_delay = 1
            cluster.exec_command(command="start")

        assert e.type == YandexAPIError
        assert "Enable send request to API!" in str(e.value)

    @patch("src.cluster.cluster.requests.post")
    def test_if_errors_but_finally_success(self, mock_post, cluster):
        mock_post.side_effect = [
            Timeout,
            HTTPError,
            ConnectionError,
            HTTPError,
            MagicMock(status_code=200, json=lambda: {"some_key": "some_value"}),
        ]

        cluster.max_retries = 5
        cluster.retry_delay = 1
        result = cluster.exec_command(command="start")

        assert result == True


class TestCheckStatus:
    @patch("src.cluster.cluster.requests.get")
    def test_check_status_success(self, mock_get, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "RUNNING"}

        mock_get.return_value = mock_response

        result = cluster.check_status(target_status="running")
        assert result == True

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_timeout_error(self, mock_get, cluster):
        mock_get.side_effect = (
            Timeout,
            Timeout,
            Timeout,
        )

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3
            cluster.retry_delay = 1
            cluster.check_status(target_status="running")

        assert e.type == YandexAPIError
        assert "Timeout error. Unable to get cluster status" in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_invalid_url_error(self, mock_get, cluster):
        mock_get.side_effect = InvalidSchema

        with pytest.raises(YandexAPIError) as e:
            cluster.check_status(target_status="running")

        assert e.type == YandexAPIError
        assert "Invalid url or schema provided" in str(e.value)
        assert "'YC_DATAPROC_BASE_URL' and 'YC_DATAPROC_CLUSTER_ID'" in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_http_error(self, mock_get, cluster):
        mock_get.side_effect = (HTTPError, HTTPError, ConnectionError)

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3
            cluster.retry_delay = 1

            cluster.check_status(target_status="running")

        assert e.type == YandexAPIError
        assert "Enable to send request to API" in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_invalid_status_code(self, mock_get, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 500

        mock_get.return_value = mock_response

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 2
            cluster.retry_delay = 1

            cluster.check_status(target_status="running")

        assert e.type == YandexAPIError
        assert "Enable to get Cluster status from API" in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_not_target(self, mock_get, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "NOT_RUNNING"}

        mock_get.return_value = mock_response

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 2
            cluster.retry_delay = 1

            cluster.check_status(target_status="running")

        assert e.type == YandexAPIError
        assert "No more retries left to check Cluster status!" in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_wrong_response(self, mock_get, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"test": "test"}

        mock_get.return_value = mock_response

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3
            cluster.retry_delay = 1

            cluster.check_status(target_status="running")

        assert e.type == YandexAPIError
        assert "Enable to get 'status' from API response" in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_unable_to_decode_response(self, mock_get, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = JSONDecodeError("Invalid JSON", "test", 0)

        mock_get.return_value = mock_response

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3
            cluster.retry_delay = 1

            cluster.check_status(target_status="running")

        assert e.type == YandexAPIError
        assert "Unable to decode API reponse" in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_if_errors_but_finally_success(self, mock_get, cluster):
        mock_get.side_effect = [
            Timeout,
            ConnectionError,
            HTTPError,
            MagicMock(status_code=200, json=lambda: {"status": "RUNNING"}),
        ]
        cluster.retry_delay = 1
        result = cluster.check_status("running")

        assert result == True
