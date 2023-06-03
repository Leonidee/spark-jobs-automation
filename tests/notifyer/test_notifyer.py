import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime

from requests.exceptions import HTTPError, InvalidSchema, Timeout

import pytest

# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.notifyer import (
    EnableToSendMessage,
    AirflowContextError,
    AirflowTaskData,
    TelegramNotifyer,
)


@pytest.fixture
def notifyer():
    """Returns instance of `TelegramNotifyer` class"""
    return TelegramNotifyer(max_retries=3, retry_delay=1)


@pytest.fixture
def airflow_context():
    """Returns Airflow context dictionary"""

    return dict(
        task_instance=MagicMock(
            task_id="my_task_id",
            dag_id="my_dag_id",
        ),
        execution_date="2022-01-01T00:00:00.000000",
    )


class TestCollectTaskContext:
    def test_all_success(self, notifyer, airflow_context):
        result = notifyer._collect_task_context(raw_context=airflow_context)

        assert isinstance(result, AirflowTaskData)
        assert isinstance(result.execution_dt, datetime)
        assert "my_task_id" in result.task
        assert "my_dag_id" in result.dag


class Old:
    @patch("src.notifyer.notifyer.requests.post")
    def test_all_success_(self, mock_post, notifyer, airflow_context):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"ok": True}
        mock_post.return_value = mock_response

        result = notifyer.notify_on_task_failure(airflow_context)

        assert result == True

    @patch("src.notifyer.notifyer.requests.post")
    def test_raises_if_invalid_airflow_context(self, notifyer):
        context = dict(
            task_instance=MagicMock(
                task_id="my_task_id",
                dag_id="my_dag_id",
            ),
            test="test",
        )
        with pytest.raises(AirflowContextError):
            notifyer.notify_on_task_failure(context)

    @pytest.mark.actual
    @patch("src.notifyer.notifyer.requests.post")
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

    @patch("src.notifyer.notifyer.requests.post")
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

    @patch("src.notifyer.notifyer.requests.post")
    def test_notify_on_task_failure_raises_if_errors(
        self, mock_post, notifyer, airflow_context
    ):
        msg = "Some error"
        mock_post.side_effect = [
            HTTPError(msg),
            Timeout(msg),
            InvalidSchema(msg),
        ]

        with pytest.raises(EnableToSendMessage):
            notifyer.notify_on_task_failure(airflow_context)

    @patch("src.notifyer.notifyer.requests.post")
    def test_notify_on_task_failure_raises_if_not_valid_response(
        self, mock_post, notifyer, airflow_context
    ):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"not_ok": True}

        mock_post.return_value = mock_response

        with pytest.raises(EnableToSendMessage):
            notifyer.notify_on_task_failure(airflow_context)

    @patch("src.notifyer.notifyer.requests.post")
    def test_notify_on_task_failure_raises_if_false_response(
        self, mock_post, notifyer, airflow_context
    ):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"ok": False}

        mock_post.return_value = mock_response

        with pytest.raises(EnableToSendMessage):
            notifyer.notify_on_task_failure(airflow_context)

    @patch("src.notifyer.notifyer.requests.post")
    def test_notify_on_task_failure_raises_if_not_valid_status_code(
        self, mock_post, notifyer, airflow_context
    ):
        mock_response = MagicMock()
        mock_response.status_code = 500

        mock_post.return_value = mock_response

        with pytest.raises(EnableToSendMessage):
            notifyer.notify_on_task_failure(airflow_context)
