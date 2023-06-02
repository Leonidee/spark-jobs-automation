from __future__ import annotations

import sys
from datetime import datetime
import os
from pathlib import Path
import time

import requests
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    InvalidSchema,
    Timeout,
    InvalidURL,
    MissingSchema,
    JSONDecodeError,
)

# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.environ import EnvironManager
from src.notifyer.exceptions import EnableToSendMessage, AirflowContextError
from src.base import BaseRequestHandler


class TelegramNotifyer(BaseRequestHandler):
    """Project Telegram notifyer. Sends messages about Airflow DAG health.

    ## Notes
    The chat to which the messages will be sent and credentials should be specified in `.env` or as global evironment variables.

    See `.env.template` for more details.
    """

    def __init__(
        self,
        *,
        max_retries: int = 10,
        retry_delay: int = 60,
        session_timeout: int = 60 * 2,
    ) -> None:
        super().__init__(
            max_retries=max_retries,
            retry_delay=retry_delay,
            session_timeout=session_timeout,
        )

        environ = EnvironManager()
        environ.load_environ()

        _REQUIRED_VARS = (
            "TG_CHAT_ID",
            "TG_BOT_TOKEN",
        )

        self._CHAT_ID, self._BOT_TOKEN = map(os.getenv, _REQUIRED_VARS)
        environ.check_environ(var=_REQUIRED_VARS)  # type: ignore

    def notify_on_task_failure(self, context: dict) -> bool:  # type: ignore
        """This function is designed to be used in the Airflow ecosystem and should be called from `default_args` `on_failure_callback` argument of either a DAG or Airflow task.

        The function is responsible for handling failures that occur during task execution.

        ## Parameters
        `context` : Airflow task context. Will be parsed to get information about task execution, errors etc.

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
        ... def start_cluster() -> None:
        ...      "Start DataProc Cluster"
        ...      cluster.start()

        """

        _TRY = 1
        _OK = False

        while not _OK:
            self.logger.debug("Getting task context")

            try:
                task = context["task_instance"].task_id  # type: ignore
                dag = context["task_instance"].dag_id  # type: ignore
                dt = str(context["execution_date"])[:19].replace("T", " ")
                execution_time = datetime.strptime(dt, r"%Y-%m-%d %H:%M:%S")

                self.logger.debug("Context collected")
                self.logger.debug(f"Full task context: {context}")
                break

            except (KeyError, ValueError) as e:
                if _TRY == self._MAX_RETRIES:
                    raise AirflowContextError(
                        f"Enable to get one of the key from Aiflow context or context itself and no more retries left. Possible because of exception:\n{e}"
                    )
                else:
                    self.logger.warning(
                        "Enable to get one of the key from Aiflow context or context itself because of error, see traceback below. Retrying..."
                    )
                    self.logger.exception(e)
                    _TRY += 1
                    time.sleep(self._DELAY)
                    continue

        MSG = f"❌ TASK FAILED!\n\nTask: {task}\nDAG: {dag}\nExecution time: {execution_time}"  # type: ignore
        URL = f"https://api.telegram.org/bot{self._BOT_TOKEN}/sendMessage?chat_id={self._CHAT_ID}&text={MSG}"

        self.logger.debug(f"Message to send: {MSG}")

        _TRY = 1

        while not _OK:
            try:
                self.logger.debug(f"Sending request. Try: {_TRY}")
                response = requests.post(url=URL, timeout=self._SESSION_TIMEOUT)

                response.raise_for_status()

            except Timeout:
                if _TRY == self._MAX_RETRIES:
                    raise EnableToSendMessage(f"Timeout error. Unable to send message")
                self.logger.warning(
                    "Timeout error occured. Will make another try after delay"
                )
                _TRY += 1
                time.sleep(self._DELAY)
                continue

            except (InvalidSchema, InvalidURL, MissingSchema):
                raise EnableToSendMessage(
                    "Invalid url or schema provided. Please check 'TG_BOT_TOKEN' and 'TG_CHAT_ID' or requested url"
                )

            except (HTTPError, ConnectionError) as e:
                if _TRY == self._MAX_RETRIES:
                    raise EnableToSendMessage(
                        f"Enable to send message. Possible because of exception:\n{e}"
                    )
                else:
                    self.logger.warning(
                        "An error occured while trying to send message! See traceback below. Will make another try after delay"
                    )
                    self.logger.exception(e)
                    _TRY += 1
                    time.sleep(self._DELAY)
                    continue

            if response.status_code == 200:
                self.logger.debug("Response received")
                response = response.json()

                if "ok" in response.keys() and response["ok"]:
                    self.logger.debug("Success! Message sent")
                    _OK = True
                    return _OK

                else:
                    if _TRY == self._MAX_RETRIES:
                        raise EnableToSendMessage(f"Enable to send message")
                    else:
                        self.logger.warning(
                            "Message not sent for some reason. Retrying..."
                        )
                    _TRY += 1
                    time.sleep(self._DELAY)
                    continue
            else:
                if _TRY == self._MAX_RETRIES:
                    raise EnableToSendMessage(f"Enable to send message")
                else:
                    self.logger.warning(
                        "Ops, seems like something went wrong. Will make another try after delay"
                    )
                    _TRY += 1
                    time.sleep(self._DELAY)
                    continue
