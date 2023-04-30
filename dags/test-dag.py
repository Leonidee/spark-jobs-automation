import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests

# airflow
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.context import Context

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.main import DataProcCluster, SparkSubmitter, YandexCloudAPI
from src.notifyer import AirflowNotifyer
from src.utils import TagsJobArgsHolder, load_environment

load_environment()

YC_DATAPROC_CLUSTER_ID = os.getenv("YC_DATAPROC_CLUSTER_ID")
YC_DATAPROC_BASE_URL = os.getenv("YC_DATAPROC_BASE_URL")
YC_OAUTH_TOKEN = os.getenv("YC_OAUTH_TOKEN")
FAST_API_BASE_URL = os.getenv("FAST_API_BASE_URL")

TG_CHAT_ID = os.getenv("TG_CHAT_ID")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")

notifyer = AirflowNotifyer()


# def send_message_on_failed(context: dict):
#     FAILED_MSG = f"ALARM! \n task failed while running! {context.get('dag_run')} \n {str(context['dag'])} {context.get('task')} {context.get('exception')} {context.get('execution_date')}"

#     URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage?chat_id={TG_CHAT_ID}&text={FAILED_MSG}"

#     response = requests.post(url=URL)
#     response.raise_for_status()


@task()
def success():
    print("hello")


@task(
    default_args={
        "on_failure_callback": notifyer.send_message_on_failed,
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
