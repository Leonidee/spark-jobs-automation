import sys
from datetime import datetime
from logging import getLogger
from os import getenv
from pathlib import Path

import requests
from requests.exceptions import ConnectionError, HTTPError, InvalidSchema, Timeout

sys.path.append(str(Path(__file__).parent.parent))

from src.config import Config
from src.logger import SparkLogger
from src.utils import load_environment

config = Config()
load_environment()

logger = (
    getLogger("aiflow.task")
    if config.IS_PROD
    else SparkLogger(level=config.log_level).get_logger(
        logger_name=str(Path(Path(__file__).name))
    )
)


class TelegramNotifyer:
    def __init__(self) -> None:
        self.chat_id = getenv("TG_CHAT_ID")
        self.bot_token = getenv("TG_BOT_TOKEN")

    def notify_on_task_failure(self, context: dict) -> None:
        logger.debug("Getting task context")
        try:
            task = context.get("task_instance").task_id
            dag = context.get("task_instance").dag_id
            dt = str(context.get("execution_date"))[:19].replace("T", " ")
            execution_time = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")

            logger.debug(f"Full task context: {context}")
        except Exception as e:
            logger.error(e)
            sys.exit(1)

        MSG = f"❌ TASK FAILED!\n\nTask: {task}\nDAG: {dag}\nExecution time: {execution_time}"
        URL = f"https://api.telegram.org/bot{self.bot_token}/sendMessage?chat_id={self.chat_id}&text={MSG}"

        logger.debug(f"Message to send: {MSG}")
        logger.debug(f"Target URL: {URL}")

        try:
            logger.debug("Sending request")
            response = requests.post(url=URL)
            response.raise_for_status()
            logger.debug("Done")

        except (HTTPError, InvalidSchema, ConnectionError, Timeout) as e:
            logger.error(e)
            sys.exit(1)
