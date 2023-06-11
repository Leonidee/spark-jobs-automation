from __future__ import annotations

import os
import sys
import time
from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any, Dict

import requests
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
from src.base import BaseRequestHandler
from src.environ import EnvironManager
from src.logger import SparkLogger
from src.notifyer.datamodel import AirflowTaskData, MessageType, TelegramMessage
from src.notifyer.exceptions import AirflowContextError, UnableToSendMessage


class TelegramNotifyer(BaseRequestHandler):
    """Project's Telegram notifyer. Sends messages about Airflow DAG health.

    ## Notes
    The chat to which the messages will be sent and credentials should be specified in `.env` or as global evironment variables.

    Required variables are: `TG_CHAT_ID` and `TG_BOT_TOKEN`

    See `.env.template` for more details.
    """

    __slots__ = "_CHAT_ID", "_BOT_TOKEN", "logger"

    def __init__(
        self,
        *,
        max_retries: int = 3,
        retry_delay: int = 5,
        session_timeout: int = 60,
    ) -> None:
        """

        ## Parameters
        `max_retries` : Max retries to send message, by default 3\n
        `retry_delay` : Delay between retries in seconds, by default 5\n
        `session_timeout` : Session timeout in seconds, by default 60
        """
        super().__init__(
            max_retries=max_retries,
            retry_delay=retry_delay,
            session_timeout=session_timeout,
        )
        self.logger = (
            getLogger("aiflow.task")
            if self.config.IS_PROD
            else SparkLogger().get_logger(name=__name__)
        )

        environ = EnvironManager()
        environ.load_environ()

        _REQUIRED_VARS = (
            "TG_CHAT_ID",
            "TG_BOT_TOKEN",
        )

        self._CHAT_ID, self._BOT_TOKEN = map(os.getenv, _REQUIRED_VARS)
        environ.check_environ(var=_REQUIRED_VARS)  # type: ignore

    def _make_url(self, message: TelegramMessage) -> str:
        """Make Telegram API URL to send message

        ## Parameters
        `message` : `TelegramMessage` instance contains message to send

        ## Returns
        `str` : Created URL
        """
        return f"https://api.telegram.org/bot{self._BOT_TOKEN}/sendMessage?chat_id={self._CHAT_ID}&text={message}"

    def _collect_task_context(self, raw_context: Dict[Any, Any]) -> AirflowTaskData:  # type: ignore
        """Collects given raw Airflow context object into `AirflowTaskData` instance

        ## Parameters
        `raw_context` : Raw Airflow context

        ## Returns
        `AirflowTaskData` : Airflow task instance data taken from raw context

        ## Raises
        `AirflowContextError` : Raises if unable to get context or one of the required keys
        """
        for _TRY in range(1, self._MAX_RETRIES + 1):
            self.logger.debug("Collecting Airflow task context")
            self.logger.debug(f"{raw_context=}")

            try:
                task = str(raw_context["task_instance"].task_id)  # type: ignore
                dag = str(raw_context["task_instance"].dag_id)  # type: ignore
                execution_dt = str(raw_context["execution_date"])[:19].replace("T", " ")

                execution_dt = datetime.strptime(execution_dt, r"%Y-%m-%d %H:%M:%S")

                self.logger.debug(f"Context collected")

                return AirflowTaskData(task=task, dag=dag, execution_dt=execution_dt)

            except KeyError as err:
                if _TRY == self._MAX_RETRIES:
                    raise AirflowContextError(
                        f"Unable to get {err.args} key/keys from Aiflow context"
                    )

                else:
                    self.logger.warning(
                        f"Unable to get {err.args} key/keys from Aiflow context. Retrying..."
                    )
                    time.sleep(self._DELAY)

                    continue

            except ValueError as err:
                if _TRY == self._MAX_RETRIES:
                    raise AirflowContextError(str(err))

                else:
                    self.logger.warning(err)
                    time.sleep(self._DELAY)

                    continue

    def _send_message(self, url: str) -> bool:  # type: ignore
        """Sends message to given URL

        ## Notes
        Message which you want to send must be inside URL string. Use `__make_url` method to make one.

        ## Parameters
        `url` : Telegram API URL

        ## Returns
        `bool` : True if message was sent

        ## Raises
        `UnableToSendMessage` : Raises if unable to send message for some reason. Will provide detailed description about occured error
        """
        self.logger.debug("Sending message")
        for _TRY in range(1, self._MAX_RETRIES + 1):
            try:
                self.logger.debug(f"Requesting... Try: {_TRY}")
                response = requests.post(url=url, timeout=self._SESSION_TIMEOUT)

                response.raise_for_status()

            except (InvalidSchema, InvalidURL, MissingSchema) as err:
                raise UnableToSendMessage(
                    f"{err}. Check 'TG_BOT_TOKEN' and 'TG_CHAT_ID' or returning URL of '__make_url' function"
                )

            except (HTTPError, ConnectionError, Timeout) as err:
                if _TRY == self._MAX_RETRIES:
                    raise UnableToSendMessage(str(err))
                else:
                    self.logger.warning(f"{err}. Retrying...")
                    time.sleep(self._DELAY)

                    continue

            if response.status_code == 200:
                self.logger.debug("Response received")

                try:
                    self.logger.debug("Decoding response")
                    response = response.json()
                    self.logger.debug(f"{response=}")

                except JSONDecodeError as err:
                    if _TRY == self._MAX_RETRIES:
                        raise UnableToSendMessage(str(err))
                    else:
                        self.logger.warning(f"{err}. Retrying...")
                        time.sleep(self._DELAY)

                        continue

                if "ok" in response.keys() and response["ok"]:
                    self.logger.debug("Success! Message sent")

                    return True

                else:
                    if _TRY == self._MAX_RETRIES:
                        raise UnableToSendMessage("Unable to send message")
                    else:
                        self.logger.warning(
                            "Message not sent for some reason. Retrying..."
                        )
                    time.sleep(self._DELAY)

                    continue
            else:
                if _TRY == self._MAX_RETRIES:
                    raise UnableToSendMessage("Unable to send message")

                else:
                    self.logger.warning(
                        "Ops, seems like something went wrong. Retrying..."
                    )
                    time.sleep(self._DELAY)

                    continue

    def notify_on_task_failure(self, airflow_context: Dict[Any, Any]) -> bool:
        """This function is designed to be used in the Airflow ecosystem and should be called from `default_args` `on_failure_callback` argument of either a DAG or Airflow task.

        The function is responsible for handling failures that occur during task execution.

        ## Parameters
        `airflow_context` : Airflow task context dictionary

        ## Examples
        Example of usage with Airflow task:
        >>> notifyer = TelegramNotifyer()
        >>> @task(
        ...    default_args={
        ...         "retries": 3,
        ...          "retry_delay": timedelta(seconds=45),
        ...          "on_failure_callback": notifyer.notify_on_task_failure, # <---- here
        ...      },
        ... )
        ... def some_task() -> None:
        ...      ...
        """

        message = TelegramMessage(
            task_data=self._collect_task_context(raw_context=airflow_context),
            type=MessageType("failed"),
        )

        self.logger.debug(f"{message=}")

        output = self._send_message(url=self._make_url(message=message))

        return output
