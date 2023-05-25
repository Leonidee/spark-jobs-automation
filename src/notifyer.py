import sys
from datetime import datetime
from logging import getLogger
from os import getenv
from pathlib import Path

import requests
from requests.exceptions import ConnectionError, HTTPError, InvalidSchema, Timeout

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.logger import SparkLogger
from src.utils import EnvironManager


class TelegramNotifyer:
    """Project Telegram notifyer. Sends messages about Airflow DAG health.

    ## Notes
    The chat to which the messages will be sent and credentials should be specified in `.env` or as global evironment variables.

    See `.env.template` for more details.
    """

    def __init__(self) -> None:
        config = Config()
        env = EnvironManager()
        env.load_environ()

        self.logger = (
            getLogger("aiflow.task")
            if config.IS_PROD
            else SparkLogger(level=config.python_log_level).get_logger(
                logger_name=__name__
            )
        )
        self.chat_id = getenv("TG_CHAT_ID")
        self.bot_token = getenv("TG_BOT_TOKEN")

    def notify_on_task_failure(self, context: dict) -> None:
        """This function is designed to be used in the Airflow ecosystem and should be called from default_args on_failure_callback argument of either a DAG or Airflow task.

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
        self.logger.debug("Getting task context")
        try:
            task = context.get("task_instance").task_id  # type: ignore
            dag = context.get("task_instance").dag_id  # type: ignore
            dt = str(context.get("execution_date"))[:19].replace("T", " ")
            execution_time = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")

            self.logger.debug(f"Full task context: {context}")
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)

        MSG = f"❌ TASK FAILED!\n\nTask: {task}\nDAG: {dag}\nExecution time: {execution_time}"
        URL = f"https://api.telegram.org/bot{self.bot_token}/sendMessage?chat_id={self.chat_id}&text={MSG}"

        self.logger.debug(f"Message to send: {MSG}")
        self.logger.debug(f"Target URL: {URL}")

        try:
            self.logger.debug("Sending request")
            response = requests.post(url=URL)
            response.raise_for_status()
            self.logger.debug("Done")

        except (HTTPError, InvalidSchema, ConnectionError, Timeout) as e:
            self.logger.error(e)
            sys.exit(1)
