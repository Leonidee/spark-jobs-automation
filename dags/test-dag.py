import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests

# airflow
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.notifyer import TelegramNotifyer

notifyer = TelegramNotifyer()


@task()
def success():
    print("hello")


@task(
    default_args={
        "on_failure_callback": notifyer.notify_on_task_failure,
        "retries": 3,
        "retry_delay": timedelta(seconds=1),
    }
)
def failed():
    print(1231 / 0)


@dag(
    dag_id="testing",
    schedule=None,
    start_date=datetime(2023, 4, 3),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["test"],
    default_args={
        "owner": "leonide",
    },
    default_view="grid",
)
def taskflow() -> None:
    begin = EmptyOperator(task_id="begining")

    s = success()
    f = failed()

    end = EmptyOperator(task_id="ending")

    begin >> s >> f >> end


taskflow()
