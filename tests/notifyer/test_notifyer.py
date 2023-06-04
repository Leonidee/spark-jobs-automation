import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime

from requests.exceptions import (
    ConnectionError,
    HTTPError,
    InvalidSchema,
    Timeout,
    JSONDecodeError,
)


import pytest

# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.notifyer import (
    UnableToSendMessage,
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

    def test_raises_if_no_execution_dt_key(self, notifyer):
        context = dict(
            task_instance=MagicMock(
                task_id="my_task_id",
                dag_id="my_dag_id",
            ),
            test="test",
        )
        with pytest.raises(AirflowContextError) as e:
            notifyer._collect_task_context(raw_context=context)

        assert e.type is AirflowContextError
        assert "Unable to get ('execution_date',) key/keys from Aiflow context" in str(
            e.value
        )

    def test_raises_if_wrong_date_format_1(self, notifyer):
        context = dict(
            task_instance=MagicMock(
                task_id="my_task_id",
                dag_id="my_dag_id",
            ),
            execution_date="2022-01-01",
        )
        with pytest.raises(AirflowContextError) as e:
            notifyer._collect_task_context(raw_context=context)

        assert e.type is AirflowContextError
        assert (
            f"time data '{context['execution_date']}' does not match format '%Y-%m-%d %H:%M:%S'"
            in str(e.value)
        )

    def test_raises_if_wrong_date_format_2(self, notifyer):
        context = dict(
            task_instance=MagicMock(
                task_id="my_task_id",
                dag_id="my_dag_id",
            ),
            execution_date="2022",
        )
        with pytest.raises(AirflowContextError) as e:
            notifyer._collect_task_context(raw_context=context)

        assert e.type is AirflowContextError
        assert (
            f"time data '{context['execution_date']}' does not match format '%Y-%m-%d %H:%M:%S'"
            in str(e.value)
        )

    def test_raises_if_wrong_date_format_3(self, notifyer):
        context = dict(
            task_instance=MagicMock(
                task_id="my_task_id",
                dag_id="my_dag_id",
            ),
            execution_date="00:00:00",
        )
        with pytest.raises(AirflowContextError) as e:
            notifyer._collect_task_context(raw_context=context)

        assert e.type is AirflowContextError
        assert (
            f"time data '{context['execution_date']}' does not match format '%Y-%m-%d %H:%M:%S'"
            in str(e.value)
        )


class TestSendMessage:
    @patch("src.notifyer.notifyer.requests.post")
    def test_all_success(self, mock_post, notifyer):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"ok": True}
        mock_post.return_value = mock_response

        result = notifyer._send_message(url=MagicMock())

        assert result is True

    @patch("src.notifyer.notifyer.requests.post")
    def test_raises_if_invalid_url(self, mock_post, notifyer):
        err_msg = "Invalid schema provided"
        mock_post.side_effect = InvalidSchema(err_msg)

        with pytest.raises(UnableToSendMessage) as e:
            notifyer._send_message(url=MagicMock())

        assert e.type is UnableToSendMessage
        assert (
            f"{err_msg}. Check 'TG_BOT_TOKEN' and 'TG_CHAT_ID' or returning URL of '__make_url' function"
            in str(e.value)
        )

    @patch("src.notifyer.notifyer.requests.post")
    def test_raises_if_timeout_error(self, mock_post, notifyer):
        err_msg = "No more time to wait!"
        mock_post.side_effect = Timeout(err_msg)

        with pytest.raises(UnableToSendMessage) as e:
            notifyer._send_message(url=MagicMock())

        assert e.type is UnableToSendMessage
        assert err_msg in str(e.value)

    @patch("src.notifyer.notifyer.requests.post")
    def test_raises_if_http_error(self, mock_post, notifyer):
        err_msg = "Some HTTP error"
        mock_post.side_effect = HTTPError(err_msg)

        with pytest.raises(UnableToSendMessage) as e:
            notifyer._send_message(url=MagicMock())

        assert e.type is UnableToSendMessage
        assert err_msg in str(e.value)

    @patch("src.notifyer.notifyer.requests.post")
    def test_raises_if_many_errors(self, mock_post, notifyer):
        err_msg = "Some error"
        mock_post.side_effect = (
            HTTPError(err_msg),
            ConnectionError(err_msg),
            Timeout(err_msg),
        )

        with pytest.raises(UnableToSendMessage) as e:
            notifyer._send_message(url=MagicMock())

        assert e.type is UnableToSendMessage
        assert err_msg in str(e.value)

    @patch("src.notifyer.notifyer.requests.post")
    def test_raises_if_not_valid_status_code(
        self,
        mock_post,
        notifyer,
    ):
        err_msg = "Unable to send message"
        mock_response = MagicMock()
        mock_response.status_code = 500

        mock_post.return_value = mock_response

        with pytest.raises(UnableToSendMessage) as e:
            notifyer._send_message(url=MagicMock())

        assert e.type is UnableToSendMessage
        assert err_msg in str(e.value)

    @patch("src.notifyer.notifyer.requests.post")
    def test_raises_if_unable_to_decode_response(
        self,
        mock_post,
        notifyer,
    ):
        err_msg = "Invalid JSON"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = JSONDecodeError("Invalid JSON", "test", 0)

        mock_post.return_value = mock_response

        with pytest.raises(UnableToSendMessage) as e:
            notifyer._send_message(url=MagicMock())

        assert e.type is UnableToSendMessage
        assert err_msg in str(e.value)

    @patch("src.notifyer.notifyer.requests.post")
    def test_raises_if_not_valid_response(self, mock_post, notifyer):
        err_msg = "Unable to send message"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"not_ok": True}

        mock_post.return_value = mock_response

        with pytest.raises(UnableToSendMessage) as e:
            notifyer._send_message(url=MagicMock())

        assert e.type is UnableToSendMessage
        assert err_msg in str(e.value)

    @patch("src.notifyer.notifyer.requests.post")
    def test_if_errors_but_finally_success(self, mock_post, notifyer):
        mock_post.side_effect = [
            HTTPError("Some HTTP error"),
            ConnectionError("Another error"),
            MagicMock(status_code=200, json=lambda: {"ok": True}),
        ]

        assert notifyer._send_message(url=MagicMock()) is True

    @patch("src.notifyer.notifyer.requests.post")
    def test_raises_if_false_response(self, mock_post, notifyer):
        err_msg = "Unable to send message"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"ok": False}

        mock_post.return_value = mock_response

        with pytest.raises(UnableToSendMessage) as e:
            notifyer._send_message(url=MagicMock())

        assert e.type is UnableToSendMessage
        assert err_msg in str(e.value)


class TestNotifyOnTaskFailure:
    @patch("src.notifyer.notifyer.TelegramNotifyer._send_message")
    def test_with_mock(self, mock_send_message, notifyer, airflow_context):
        mock_send_message.return_value = True

        result = notifyer.notify_on_task_failure(airflow_context=airflow_context)

        assert result is True

    def test_send_real_message(self, notifyer):
        airflow_context = dict(
            task_instance=MagicMock(
                task_id="test",
                dag_id="test",
            ),
            execution_date="2022-01-01T00:00:00.000000",
        )

        result = notifyer.notify_on_task_failure(airflow_context=airflow_context)

        assert result is True
