import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

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

SPARK_REPORT_DATE = "2022-03-05"  # str(datetime.today().date())
TAGS_VERIFIED_PATH = (
    "s3a://data-ice-lake-04/messager-data/snapshots/tags_verified/actual"
)
SRC_PATH = "s3a://data-ice-lake-04/messager-data/analytics/cleaned-events"
TGT_PATH = "s3a://data-ice-lake-04/messager-data/analytics/tag-candidates"

SUBMIT_TASK_DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(seconds=45),
    "on_failure_callback": "stop_dataproc_cluster",
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
        "retry_delay": timedelta(minutes=2),
    }
)
def start_dataproc_cluster() -> None:
    cluster.start()


@task(
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    }
)
def wait_until_running() -> None:
    cluster.is_running()


@task(default_args=SUBMIT_TASK_DEFAULT_ARGS)
def submit_tags_job_for_7d() -> None:
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
        "retry_delay": timedelta(minutes=2),
    }
)
def stop_dataproc_cluster() -> None:
    cluster.stop()


@task(
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    }
)
def wait_until_stopped() -> None:
    cluster.is_stopped()


@dag(
    dag_id="tags-job",
    schedule="0 2 * * *",
    start_date=datetime(2023, 4, 3),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["spark"],
    default_args={
        "owner": "leonide",
    },
    default_view="grid",
)
def taskflow() -> None:
    start = start_dataproc_cluster()
    stop = stop_dataproc_cluster()
    is_running = wait_until_running()
    is_stopped = wait_until_stopped()
    job_7d = submit_tags_job_for_7d()
    job_60d = submit_tags_job_for_60d()

    begin = EmptyOperator(task_id="begining")
    end = EmptyOperator(task_id="ending")

    chain(begin, start, is_running, job_7d, job_60d, stop, is_stopped, end)


taskflow()
