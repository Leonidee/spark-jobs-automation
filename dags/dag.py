import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
import yaml

# airflow
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.main import YandexCloudAPI, DataProcCluster, SparkSubmitter
from src.utils import load_environment

load_environment()

YC_DATAPROC_CLUSTER_ID = os.getenv("YC_DATAPROC_CLUSTER_ID")
YC_DATAPROC_BASE_URL = os.getenv("YC_DATAPROC_BASE_URL")
YC_OAUTH_TOKEN = os.getenv("YC_OAUTH_TOKEN")

FAST_API_BASE_URL = os.getenv("FAST_API_BASE_URL")

with open("jobs-config.yaml") as f:
    config = yaml.safe_load(f)

SPARK_REPORT_DATE = str(datetime.today().date())
TAGS_VERIFIED_PATH = config["TAGS-JOB"]["TAGS_VERIFIED_PATH"]
SRC_PATH = config["TAGS-JOB"]["SRC_PATH"]
TGT_PATH = config["TAGS-JOB"]["TGT_PATH"]

SUBMIT_TASK_DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(seconds=45),
}

yc = YandexCloudAPI()
token = yc.get_iam_token(oauth_token=YC_OAUTH_TOKEN)

cluster = DataProcCluster(
    token=token,
    cluster_id=YC_DATAPROC_CLUSTER_ID,
    base_url=YC_DATAPROC_BASE_URL,
    max_attempts_to_check_status=20,
)
spark = SparkSubmitter(api_base_url=FAST_API_BASE_URL)


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
)
def start_cluster() -> None:
    "Start DataProc Cluster"
    cluster.start()


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
)
def wait_until_cluster_running() -> None:
    "Wait until Cluster is ready to use"
    cluster.is_running()


@task(default_args=SUBMIT_TASK_DEFAULT_ARGS)
def submit_tags_job_for_7d() -> None:
    "Submit tags job for 7 days"
    spark.submit_tags_job(
        date=SPARK_REPORT_DATE,
        depth=7,
        threshold=50,
        tags_verified_path=TAGS_VERIFIED_PATH,
        src_path=SRC_PATH,
        tgt_path=TGT_PATH,
    )


@task(default_args=SUBMIT_TASK_DEFAULT_ARGS)
def submit_tags_job_for_60d() -> None:
    "Submit tags job for 60 days"
    spark.submit_tags_job(
        date=SPARK_REPORT_DATE,
        depth=60,
        threshold=200,
        tags_verified_path=TAGS_VERIFIED_PATH,
        src_path=SRC_PATH,
        tgt_path=TGT_PATH,
    )


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
    trigger_rule="all_success",
)
def stop_cluster_success_way() -> None:
    "Stop Cluster if every of upstream tasks successfully executed, if not - skipped"
    cluster.stop()


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
    trigger_rule="one_failed",
)
def stop_cluster_failed_way() -> None:
    "Stop Cluster if one of the upstream tasks failed, if not - skipped"
    cluster.stop()


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    trigger_rule="all_done",
)
def wait_until_cluster_stopped() -> None:
    "Wait until Cluster is stopped"
    cluster.is_stopped()


@dag(
    dag_id="tags-job",
    schedule="0 2 * * *",
    start_date=datetime(2023, 4, 3),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["spark"],
    default_args={
        "owner": "leonide",
    },
    default_view="grid",
)
def taskflow() -> None:
    begin = EmptyOperator(task_id="begining")

    start = start_cluster()
    is_running = wait_until_cluster_running()

    job_7d = submit_tags_job_for_7d()
    job_60d = submit_tags_job_for_60d()

    stop_if_failed = stop_cluster_failed_way()
    stop_if_success = stop_cluster_success_way()

    is_stopped = wait_until_cluster_stopped()

    end = EmptyOperator(task_id="ending")

    chain(
        begin,
        start,
        is_running,
        job_7d,
        job_60d,
        [stop_if_failed, stop_if_success],
        is_stopped,
        end,
    )


taskflow()
