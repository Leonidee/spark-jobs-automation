import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import os

import pytest
from requests.exceptions import ConnectionError, HTTPError, InvalidSchema, Timeout

# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.cluster import YandexAPIError


class TestGetIAMToken:
    @patch("src.cluster.cluster.requests.post")
    def test_get_iam_token(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"iamToken": "test_token"}
        mock_post.return_value = mock_response

        cluster._get_iam_token()
        assert os.environ["YC_IAM_TOKEN"] == "test_token"

    @patch("src.cluster.cluster.requests.post")
    def test_get_iam_token_raises_if_error(self, mock_post, cluster):
        msg = "Some error"
        mock_post.side_effect = [
            HTTPError(msg),
            Timeout(msg),
            ConnectionError(msg),
            InvalidSchema(msg),
        ]

        with pytest.raises(YandexAPIError):
            cluster.max_retries = 4
            cluster.retry_delay = 1
            cluster._get_iam_token()

    @patch("src.cluster.cluster.requests.post")
    def test_get_iam_token_raises_if_no_token(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"not_token": "some_value"}

        mock_post.return_value = mock_response

        with pytest.raises(YandexAPIError):
            cluster.max_retries = 3
            cluster.retry_delay = 1
            cluster._get_iam_token()

    @patch("src.cluster.cluster.requests.post")
    def test_get_iam_raises_if_wrong_status_code(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_post.return_value = mock_response

        with pytest.raises(YandexAPIError):
            cluster.max_retries = 2
            cluster.retry_delay = 1
            cluster._get_iam_token()

    @patch("src.cluster.cluster.requests.post")
    def test_get_iam_if_errors_but_finally_success(self, mock_post, cluster):
        msg = "Some error"
        mock_post.side_effect = [
            HTTPError(msg),
            Timeout(msg),
            ConnectionError(msg),
            InvalidSchema(msg),
            MagicMock(status_code=200, json=lambda: {"iamToken": "some_value"}),
        ]

        cluster.max_retries = 5
        cluster.retry_delay = 1

        cluster._get_iam_token()
        assert os.environ["YC_IAM_TOKEN"] == "some_value"


class TestExecCommand:
    @patch("src.cluster.cluster.requests.post")
    def test_exec_command_start_success(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"some_key": "some_value"}

        mock_post.return_value = mock_response

        cluster.exec_command("start")

        mock_post.assert_called_once()

    @patch("src.cluster.cluster.requests.post")
    def test_exec_command_raises_if_error(self, mock_post, cluster):
        msg = "Some error"
        mock_post.side_effect = [
            HTTPError(msg),
            Timeout(msg),
            ConnectionError(msg),
            InvalidSchema(msg),
        ]

        with pytest.raises(YandexAPIError):
            cluster.max_retries = 4
            cluster.retry_delay = 1
            cluster.exec_command(command="start")

    @patch("src.cluster.cluster.requests.post")
    def test_exec_command_raises_if_max_retries_achieved(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 500

        mock_post.return_value = mock_response

        with pytest.raises(YandexAPIError):
            cluster.max_retries = 2
            cluster.retry_delay = 1
            cluster.exec_command(command="start")

    @patch("src.cluster.cluster.requests.post")
    def test_exec_command_if_errors_but_finally_success(self, mock_post, cluster):
        msg = "Some error"
        mock_post.side_effect = [
            HTTPError(msg),
            Timeout(msg),
            ConnectionError(msg),
            InvalidSchema(msg),
            MagicMock(status_code=200, json=lambda: {"some_key": "some_value"}),
        ]

        cluster.max_retries = 5
        cluster.retry_delay = 1
        cluster.exec_command(command="start")

        mock_post.assert_called()


class TestCheckStatus:
    @patch("src.cluster.cluster.requests.get")
    def test_check_status_success(self, mock_get, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "RUNNING"}

        mock_get.return_value = mock_response

        cluster.check_status(target_status="running")

        mock_get.assert_called_once_with(
            url=f"{cluster._BASE_URL}/{cluster._CLUSTER_ID}",
            headers={"Authorization": f"Bearer {cluster._IAM_TOKEN}"},
            timeout=60 * 2,
        )

    @patch("src.cluster.cluster.requests.get")
    def test_check_status_raises_if_error(self, mock_post, cluster):
        msg = "Some error"
        mock_post.side_effect = [
            HTTPError(msg),
            Timeout(msg),
            ConnectionError(msg),
            InvalidSchema(msg),
        ]

        with pytest.raises(YandexAPIError):
            cluster.max_retries = 4
            cluster.retry_delay = 1

            cluster.check_status(target_status="running")

    @patch("src.cluster.cluster.requests.get")
    def test_check_status_raises_if_max_retries_achieved(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 500

        mock_post.return_value = mock_response

        with pytest.raises(YandexAPIError):
            cluster.max_retries = 2
            cluster.retry_delay = 1

            cluster.check_status(target_status="running")

    @patch("src.cluster.cluster.requests.get")
    def test_check_status_raises_if_not_target(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "NOT_RUNNING"}

        mock_post.return_value = mock_response

        with pytest.raises(YandexAPIError):
            cluster.max_retries = 2
            cluster.retry_delay = 1

            cluster.check_status(target_status="running")

    @patch("src.cluster.cluster.requests.get")
    def test_check_status_raises_if_wrong_response(self, mock_post, cluster):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"not_status": "..."}

        mock_post.return_value = mock_response

        with pytest.raises(YandexAPIError):
            cluster.max_retries = 2
            cluster.retry_delay = 1

            cluster.check_status(target_status="running")

    @patch("src.cluster.cluster.requests.get")
    def test_check_status_success_after_errors(self, mock_get, cluster):
        mock_get.side_effect = [
            Timeout(),
            ConnectionError(),
            InvalidSchema(),
            HTTPError(),
            MagicMock(status_code=200, json=lambda: {"status": "RUNNING"}),
        ]
        cluster.retry_delay = 1
        cluster.check_status("running")

        mock_get.assert_called()

    @patch("src.cluster.cluster.requests.get")
    def test_check_status_raises_after_errors_if_no_status(self, mock_get, cluster):
        mock_get.side_effect = [
            Timeout(),
            ConnectionError(),
            InvalidSchema(),
            HTTPError(),
            MagicMock(status_code=200, json=lambda: {"not_status": "..."}),
            MagicMock(status_code=200, json=lambda: {"not_status": "..."}),
        ]

        with pytest.raises(YandexAPIError):
            cluster.max_retries = 6
            cluster.retry_delay = 1

            cluster.check_status(target_status="running")

    @patch("src.cluster.cluster.requests.get")
    def test_check_status_raises_after_errors_if_wrong_status_code(
        self, mock_get, cluster
    ):
        mock_get.side_effect = [
            Timeout(),
            ConnectionError(),
            InvalidSchema(),
            HTTPError(),
            MagicMock(status_code=500),
            MagicMock(status_code=500),
        ]

        with pytest.raises(YandexAPIError) as e:
            cluster.max_retries = 6
            cluster.retry_delay = 1

            cluster.check_status(target_status="running")
