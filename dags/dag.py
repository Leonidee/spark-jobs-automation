import sys
import logging

from datetime import datetime, timedelta
from pathlib import Path

# airflow
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator


@dag(
    dag_id="bash",
    schedule="0 1 * * *",
    start_date=datetime(2023, 4, 3),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["test"],
    default_args={
        "owner": "leonide",
    },
    default_view="graph",
)
def dag() -> None:
    bash = BashOperator(task_id="bash-test", bash_command="yc compute instance list")
    install = BashOperator(
        task_id="instalation",
        bash_command="curl -sSL https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash",
        skip_exit_code=99,
    )
    token = BashOperator(
        task_id="set-token",
        bash_command="yc config set token y0_AgAAAAA5BcMJAATuwQAAAADgiVNpFazlpCWdQQOmR0J9LxsiXmMb-VY",
    )
    begin = EmptyOperator(task_id="begining")
    end = EmptyOperator(task_id="ending")

    chain(begin, install, token, bash, end)


dag()
