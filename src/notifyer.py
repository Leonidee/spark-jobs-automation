import os

from requests import post

from src.utils import load_environment

load_environment()


class TelegramNotifyer:
    def __init__(self) -> None:
        self.chat_id = os.getenv("TG_CHAT_ID")
        self.bot_token = os.getenv("TG_BOT_TOKEN")

    def notify_on_failure(self, context: dict) -> None:
        MSG = f"ALARM! \n task failed while running! {context.get('dag_run')} \n {str(context['dag'])} {context.get('task')} {context.get('exception')} {context.get('execution_date')}"

        URL = f"https://api.telegram.org/bot{self.bot_token}/sendMessage?chat_id={self.chat_id}&text={MSG}"

        response = post(url=URL)
        response.raise_for_status()
