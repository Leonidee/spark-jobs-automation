import sys
from datetime import datetime, timedelta
from pathlib import Path

# airflow
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.cluster import DataProcCluster
from src.config import Config
from src.notifyer import TelegramNotifyer
from src.submiter import SparkSubmitter
from src.utils import TagsJobArgsHolder

config = Config()

tags_job_args = TagsJobArgsHolder(
    date="2022-03-24",
    depth=7,
    threshold=50,
    tags_verified_path=config.tags_job_config["TAGS_VERIFIED_PATH"],
    src_path=config.tags_job_config["SRC_PATH"],
    tgt_path=config.tags_job_config["TGT_PATH"],
)


notifyer = TelegramNotifyer()

cluster = DataProcCluster(
    max_attempts_to_check_status=20,
)
spark = SparkSubmitter()


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=45),
        "on_failure_callback": notifyer.notify_on_task_failure,
    },
)
def start_cluster() -> None:
    "Start DataProc Cluster"
    cluster.start()


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notifyer.notify_on_task_failure,
    },
)
def wait_until_cluster_running() -> None:
    "Wait until Cluster is ready to use"
    cluster.is_running()


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=45),
        "on_failure_callback": notifyer.notify_on_task_failure,
    }
)
def submit_tags_job_for_7d() -> None:
    "Submit tags job for 7 days"

    spark.submit_tags_job(holder=tags_job_args)


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=45),
        "on_failure_callback": notifyer.notify_on_task_failure,
    }
)
def submit_tags_job_for_60d() -> None:
    "Submit tags job for 60 days"
    tags_job_args.depth = 60
    tags_job_args.threshold = 200

    spark.submit_tags_job(tags_job_args)


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": notifyer.notify_on_task_failure,
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
        "on_failure_callback": notifyer.notify_on_task_failure,
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
        "on_failure_callback": notifyer.notify_on_task_failure,
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
