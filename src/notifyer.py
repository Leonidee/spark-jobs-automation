import os
import sys
from datetime import datetime
from pathlib import Path

from requests import post

sys.path.append(str(Path(__file__).parent.parent))
from src.utils import load_environment

load_environment()


class TelegramNotifyer:
    def __init__(self) -> None:
        self.chat_id = os.getenv("TG_CHAT_ID")
        self.bot_token = os.getenv("TG_BOT_TOKEN")

    def notify_on_task_failure(self, context: dict) -> None:
        task = context.get("task_instance").task_id
        dag = context.get("task_instance").dag_id

        dt = str(context.get("execution_date"))[:19].replace("T", " ")
        execution_time = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")

        MSG = f"❌ TASK FAILED!\n\nTask: {task}\nDAG: {dag}\nExecution time: {execution_time}"

        URL = f"https://api.telegram.org/bot{self.bot_token}/sendMessage?chat_id={self.chat_id}&text={MSG}"

        response = post(url=URL)
        response.raise_for_status()
