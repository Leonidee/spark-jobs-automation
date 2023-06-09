import os
import random
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    InvalidSchema,
    InvalidURL,
    JSONDecodeError,
    MissingSchema,
    Timeout,
)

# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.cluster import YandexAPIError


class TestGetIAMToken:
    @patch("src.cluster.cluster.requests.post")
    def test_all_success(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"iamToken": "test_token"}
        mock_post.return_value = mock_response

        result = cluster._get_iam_token()
        assert os.environ["YC_IAM_TOKEN"] == "test_token"
        assert result is True

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_timeout_error(self, mock_post, cluster):
        err_msg = "Timeout error"

        mock_post.side_effect = (
            Timeout(err_msg),
            Timeout(err_msg),
        )

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 2
            cluster.retry_delay = 1

            cluster._get_iam_token()

        assert e.type is YandexAPIError
        assert err_msg in str(e.value)

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_invalid_schema_error(self, mock_post, cluster):
        err_msg = "Some schema error"

        mock_post.side_effect = random.choice(
            (InvalidSchema(err_msg), InvalidURL(err_msg), MissingSchema(err_msg))
        )

        with pytest.raises(YandexAPIError) as e:
            cluster._get_iam_token()

        assert e.type is YandexAPIError
        assert err_msg in str(e.value)

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_http_errors(self, mock_post, cluster):
        err_msg = "Some HTTP error"

        mock_post.side_effect = (
            HTTPError(err_msg),
            ConnectionError(err_msg),
            HTTPError(err_msg),
        )

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3
            cluster.retry_delay = 1

            cluster._get_iam_token()

        assert e.type is YandexAPIError
        assert err_msg in str(e.value)

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

        assert e.type is YandexAPIError
        assert "Unable to get IAM token from API response" in str(e.value)

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_unable_to_decode_response(self, mock_post, cluster):
        err_msg = "Invalid JSON"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = JSONDecodeError(err_msg, "test", 0)

        mock_post.return_value = mock_response

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3
            cluster.retry_delay = 1
            cluster._get_iam_token()

        assert e.type is YandexAPIError
        assert err_msg in str(e.value)

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_invalid_status_code(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_post.return_value = mock_response

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 2
            cluster.retry_delay = 1
            cluster._get_iam_token()

        assert e.type is YandexAPIError
        assert "Unable to get IAM token" in str(e.value)

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

        result = cluster._get_iam_token()
        assert os.environ["YC_IAM_TOKEN"] == "some_value"
        assert result is True


class TestExecCommand:
    @patch("src.cluster.cluster.requests.post")
    def test_start_success(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"some_key": "some_value"}

        mock_post.return_value = mock_response

        result = cluster.exec_command("start")

        assert result is True

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_timeout_error(self, mock_post, cluster):
        err_msg = "Timeout error"

        mock_post.side_effect = (
            Timeout(err_msg),
            Timeout(err_msg),
            Timeout(err_msg),
        )

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3
            cluster.retry_delay = 1
            cluster.exec_command(command="start")

        assert e.type is YandexAPIError
        assert err_msg in str(e.value)

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_schema_error(self, mock_post, cluster):
        err_msg = "Some schema error"

        mock_post.side_effect = random.choice(
            (InvalidSchema(err_msg), InvalidURL(err_msg), MissingSchema(err_msg))
        )

        with pytest.raises(YandexAPIError) as e:
            cluster.exec_command(command="start")

        assert e.type is YandexAPIError
        assert (
            f"{err_msg}. Please check 'YC_DATAPROC_BASE_URL' and 'YC_DATAPROC_CLUSTER_ID' environment variables"
            in str(e.value)
        )

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_http_error(self, mock_post, cluster):
        err_msg = "Some HTTP error"

        mock_post.side_effect = (
            HTTPError(err_msg),
            HTTPError(err_msg),
            ConnectionError(err_msg),
        )

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3
            cluster.retry_delay = 1
            cluster.exec_command(command="start")

        assert e.type is YandexAPIError
        assert err_msg in str(e.value)

    @patch("src.cluster.cluster.requests.post")
    def test_raises_if_invalid_status_code(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 500

        mock_post.return_value = mock_response

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 2
            cluster.retry_delay = 1
            cluster.exec_command(command="start")

        assert e.type is YandexAPIError
        assert "Unable send request to Yandex Cloud API" in str(e.value)

    @patch("src.cluster.cluster.requests.post")
    def test_if_errors_but_finally_success(self, mock_post, cluster):
        mock_post.side_effect = (
            Timeout,
            HTTPError,
            ConnectionError,
            HTTPError,
            MagicMock(status_code=200, json=lambda: {"some_key": "some_value"}),
        )

        cluster.max_retries = 5
        cluster.retry_delay = 1
        result = cluster.exec_command(command="start")

        assert result is True

    @patch("src.cluster.cluster.requests.post")
    def test_success_if_unable_to_decode_response(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = JSONDecodeError("Invalid JSON", "test", 0)

        mock_post.return_value = mock_response

        result = cluster.exec_command(command="start")

        assert result is True


class TestCheckStatus:
    @patch("src.cluster.cluster.requests.get")
    def test_all_success(self, mock_get, cluster):
        mock_get.side_effect = (
            MagicMock(status_code=200, json=lambda: {"status": "RUNNING"}),
        )

        result = cluster.check_status(target_status="running")
        assert result is True

    @patch("src.cluster.cluster.requests.get")
    def test_all_success_as_real(self, mock_get, cluster):
        mock_get.side_effect = (
            MagicMock(status_code=200, json=lambda: {"status": "STARTING"}),
            MagicMock(status_code=200, json=lambda: {"status": "STARTING"}),
            MagicMock(status_code=200, json=lambda: {"status": "STARTING"}),
            MagicMock(status_code=200, json=lambda: {"status": "RUNNING"}),
        )

        result = cluster.check_status(target_status="running")
        assert result is True

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_timeout_error(self, mock_get, cluster):
        err_msg = "Timeout error"

        mock_get.side_effect = (
            Timeout(err_msg),
            Timeout(err_msg),
            Timeout(err_msg),
        )

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3
            cluster.check_status(target_status="running")

        assert e.type is YandexAPIError
        assert err_msg in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_invalid_schema_error(self, mock_get, cluster):
        err_msg = "Some schema error"

        mock_get.side_effect = random.choice(
            (InvalidSchema(err_msg), InvalidURL(err_msg), MissingSchema(err_msg))
        )

        with pytest.raises(YandexAPIError) as e:
            cluster.check_status(target_status="running")

        assert e.type is YandexAPIError
        assert err_msg in str(e.value)
        assert "'YC_DATAPROC_BASE_URL' and 'YC_DATAPROC_CLUSTER_ID'" in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_http_error(self, mock_get, cluster):
        err_msg = "Some schema error"

        mock_get.side_effect = random.choice(
            (HTTPError(err_msg), ConnectionError(err_msg))
        )

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 1

            cluster.check_status(target_status="running")

        assert e.type is YandexAPIError
        assert err_msg in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_invalid_status_code(self, mock_get, cluster):
        mock_get.side_effect = (
            MagicMock(status_code=404),
            MagicMock(status_code=404),
            MagicMock(status_code=404),
        )

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3

            cluster.check_status(target_status="running")

        assert e.type is YandexAPIError
        assert "Unable to get 'status' from API response" in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_not_target_status(self, mock_get, cluster):
        mock_get.side_effect = (
            MagicMock(status_code=200, json=lambda: {"status": "STARTING"}),
            MagicMock(status_code=200, json=lambda: {"status": "STARTING"}),
            MagicMock(status_code=200, json=lambda: {"status": "STARTING"}),
        )

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3

            cluster.check_status(target_status="running")

        assert e.type is YandexAPIError
        assert "No more retries left to check Cluster status!" in str(e.value)
        assert "Last received status was: 'STARTING'" in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_not_target_status_and_no_status(self, mock_get, cluster):
        mock_get.side_effect = (
            MagicMock(status_code=200, json=lambda: {"status": "STARTING"}),
            MagicMock(status_code=200, json=lambda: {"status": "STARTING"}),
            MagicMock(status_code=200, json=lambda: {"test": "test"}),
        )

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3

            cluster.check_status(target_status="running")

        assert e.type is YandexAPIError
        assert "Unable to get 'status' from API response" in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_no_status_all(self, mock_get, cluster):
        mock_get.side_effect = (
            MagicMock(status_code=200, json=lambda: {"test": "test"}),
            MagicMock(status_code=200, json=lambda: {"test": "test"}),
            MagicMock(status_code=200, json=lambda: {"test": "test"}),
        )

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3

            cluster.check_status(target_status="running")

        assert e.type is YandexAPIError
        assert "Unable to get 'status' from API response" in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_raises_if_unable_to_decode_response(self, mock_get, cluster):
        err_msg = "Invalid JSON"

        mock_get.side_effect = (
            MagicMock(
                status_code=200,
                json=MagicMock(side_effect=JSONDecodeError(err_msg, "test", 0)),
            ),
            MagicMock(
                status_code=200,
                json=MagicMock(side_effect=JSONDecodeError(err_msg, "test", 0)),
            ),
            MagicMock(
                status_code=200,
                json=MagicMock(side_effect=JSONDecodeError(err_msg, "test", 0)),
            ),
        )
        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 3

            cluster.check_status(target_status="running")

        assert e.type is YandexAPIError
        assert err_msg in str(e.value)

    @patch("src.cluster.cluster.requests.get")
    def test_if_all_errors_but_finally_success(self, mock_get, cluster):
        mock_get.side_effect = (
            Timeout,
            ConnectionError,
            HTTPError,
            MagicMock(
                status_code=200,
                json=MagicMock(side_effect=JSONDecodeError("Invalid JSON", "test", 0)),
            ),
            MagicMock(status_code=404),
            MagicMock(status_code=200, json=lambda: {"test": "test"}),
            MagicMock(status_code=200, json=lambda: {"status": "NOT RUNNING"}),
            MagicMock(status_code=200, json=lambda: {"status": "RUNNING"}),
        )

        result = cluster.check_status("running")

        assert result == True
