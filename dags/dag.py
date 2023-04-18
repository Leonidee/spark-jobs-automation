import sys
import os
from dotenv import find_dotenv, load_dotenv

from datetime import datetime, timedelta
from pathlib import Path

# airflow
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain

# package
sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.main import YandexCloudAPI, DataProcCluster, Spark

find_dotenv(raise_error_if_not_found=True)
load_dotenv()

YC_DATAPROC_CLUSTER_ID = os.getenv("YC_DATAPROC_CLUSTER_ID")
YC_DATAPROC_BASE_URL = os.getenv("YC_DATAPROC_BASE_URL")
YC_OAUTH_TOKEN = os.getenv("YC_OAUTH_TOKEN")
FAST_API_BASE_URL = os.getenv("FAST_API_BASE_URL")

SPARK_REPORT_DATE = str(datetime.today().date())
TAGS_VERIFIED_PATH = (
    "s3a://data-ice-lake-04/messager-data/snapshots/tags_verified/actual"
)
SRC_PATH = "s3a://data-ice-lake-04/messager-data/analytics/cleaned-events"
TGT_PATH = "s3a://data-ice-lake-04/messager-data/analytics/tag-candidates"

yc = YandexCloudAPI(oauth_token=YC_OAUTH_TOKEN)
token = yc.get_iam_token()

cluster = DataProcCluster(
    token=token, cluster_id=YC_DATAPROC_CLUSTER_ID, base_url=YC_DATAPROC_BASE_URL
)
spark = Spark(base_url=FAST_API_BASE_URL, session_timeout=60 * 60)


@task(
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "depends_on_past": True,
    }
)
def start_dataproc_cluster():
    cluster.start()
    cluster.is_running()


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "depends_on_past": True,
    }
)
def submit_tags_job_for_7d():
    spark.do_tags_job(
        date=SPARK_REPORT_DATE,
        depth="7",
        threshold="100",
        tags_verified_path=TAGS_VERIFIED_PATH,
        src_path=SRC_PATH,
        tgt_path=TGT_PATH,
    )


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "depends_on_past": True,
    }
)
def submit_tags_job_for_60d():
    spark.do_tags_job(
        date=SPARK_REPORT_DATE,
        depth="60",
        threshold="100",
        tags_verified_path=TAGS_VERIFIED_PATH,
        src_path=SRC_PATH,
        tgt_path=TGT_PATH,
    )


@task(
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "depends_on_past": True,
    }
)
def stop_dataproc_cluster():
    cluster.stop()
    cluster.is_stopped()


@dag(
    dag_id="tags-job",
    schedule=None,
    start_date=datetime(2023, 4, 3),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["spark"],
    default_args={
        "owner": "leonide",
    },
    default_view="graph",
)
def taskflow() -> None:
    start = start_dataproc_cluster()
    stop = stop_dataproc_cluster()
    job_7d = submit_tags_job_for_7d()
    job_60d = submit_tags_job_for_60d()

    begin = EmptyOperator(task_id="begining")
    end = EmptyOperator(task_id="ending")

    chain(begin, start, job_7d, job_60d, stop, end)


taskflow()
