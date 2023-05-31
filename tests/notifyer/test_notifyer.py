import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

from requests.exceptions import HTTPError, InvalidSchema, Timeout

import pytest

# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.notifyer import EnableToSendMessageError, AirflowContextError


class TestNotifyer:
    @patch("src.utils.notifyer.requests.post")
    def test_notify_on_task_failure_valid(self, mock_post, notifyer, airflow_context):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"ok": True}
        mock_post.return_value = mock_response

        notifyer.notify_on_task_failure(airflow_context)

        mock_post.assert_called_once()

    @patch("src.utils.notifyer.requests.post")
    def test_notify_on_task_failure_raises_if_keyerror(self, mock_post, notifyer):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"ok": True}
        mock_post.return_value = mock_response

        context = dict(
            task_instance=MagicMock(
                task_id="my_task_id",
                dag_id="my_dag_id",
            ),
            some_key="some_value",
        )
        with pytest.raises(AirflowContextError):
            notifyer.notify_on_task_failure(context)

    @patch("src.utils.notifyer.requests.post")
    def test_notify_on_task_failure_raises_if_wrong_data_format(
        self, mock_post, notifyer
    ):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"ok": True}
        mock_post.return_value = mock_response

        context = dict(
            task_instance=MagicMock(
                task_id="my_task_id",
                dag_id="my_dag_id",
            ),
            execution_date="2022-01-01",
        )
        with pytest.raises(AirflowContextError):
            notifyer.notify_on_task_failure(context)

    @patch("src.utils.notifyer.requests.post")
    def test_notify_on_task_failure_if_errors_but_finally_success(
        self, mock_post, notifyer, airflow_context
    ):
        msg = "Some error"
        mock_post.side_effect = [
            HTTPError(msg),
            Timeout(msg),
            MagicMock(status_code=200, json=lambda: {"ok": True}),
        ]

        notifyer.notify_on_task_failure(airflow_context)
        mock_post.assert_called()

    @patch("src.utils.notifyer.requests.post")
    def test_notify_on_task_failure_raises_if_errors(
        self, mock_post, notifyer, airflow_context
    ):
        msg = "Some error"
        mock_post.side_effect = [
            HTTPError(msg),
            Timeout(msg),
            InvalidSchema(msg),
        ]

        with pytest.raises(EnableToSendMessageError):
            notifyer.notify_on_task_failure(airflow_context)

    @patch("src.utils.notifyer.requests.post")
    def test_notify_on_task_failure_raises_if_not_valid_response(
        self, mock_post, notifyer, airflow_context
    ):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"not_ok": True}

        mock_post.return_value = mock_response

        with pytest.raises(EnableToSendMessageError):
            notifyer.notify_on_task_failure(airflow_context)

    @patch("src.utils.notifyer.requests.post")
    def test_notify_on_task_failure_raises_if_false_response(
        self, mock_post, notifyer, airflow_context
    ):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"ok": False}

        mock_post.return_value = mock_response

        with pytest.raises(EnableToSendMessageError):
            notifyer.notify_on_task_failure(airflow_context)

    @patch("src.utils.notifyer.requests.post")
    def test_notify_on_task_failure_raises_if_not_valid_status_code(
        self, mock_post, notifyer, airflow_context
    ):
        mock_response = MagicMock()
        mock_response.status_code = 500

        mock_post.return_value = mock_response

        with pytest.raises(EnableToSendMessageError):
            notifyer.notify_on_task_failure(airflow_context)
