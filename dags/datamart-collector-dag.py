import sys
from datetime import datetime, timedelta
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING

# airflow
from airflow.decorators import dag, task  # type: ignore
from airflow.models.baseoperator import chain  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore

if TYPE_CHECKING:
    from typing import Dict

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.cluster.cluster import DataProcCluster
from src.config.config import Config
from src.keeper import ArgsKeeper
from src.notifyer import TelegramNotifyer
from src.submitter import SparkSubmitter

notifyer = TelegramNotifyer()
logger = getLogger("aiflow.task")

DEFAULT_ARGS = dict(
    retries=3,
    retry_delay=timedelta(seconds=45),
    on_failure_callback=notifyer.notify_on_task_failure,
)


@task(default_args=DEFAULT_ARGS)
def start_cluster(cluster: DataProcCluster) -> ...:
    "Start DataProc Cluster"
    cluster.exec_command(command="start")


@task(default_args=DEFAULT_ARGS)
def wait_until_cluster_running(cluster: DataProcCluster) -> ...:
    "Wait until Cluster is ready to use"
    cluster.check_status(target_status="running")


@task(default_args=DEFAULT_ARGS)
def collect_users_demographic_dm_job(
    spark_submitter: SparkSubmitter, job_args: Dict
) -> ...:
    try:
        keeper = ArgsKeeper(
            date=str(job_args["date"]),
            depth=job_args["depth"],  # type:ignore
            src_path=job_args["src_path"],  # type:ignore
            tgt_path=job_args["tgt_path"],  # type:ignore
            coords_path=job_args["coords_path"],  # type:ignore
            processed_dttm=datetime.now().strftime(r"%Y-%m-%d %H:%M:%S").replace(" ", "T"),  # type: ignore
        )
    except ValueError as err:
        logger.error(err)
        sys.exit(1)
    except UserWarning as err:
        logger.warning(err)
        pass

    spark_submitter.submit_job(job="collect_users_demographic_dm_job", keeper=keeper)  # type: ignore


@task(default_args=DEFAULT_ARGS)
def collect_events_total_cnt_agg_wk_mnth_dm_job(
    spark_submitter: SparkSubmitter, job_args: Dict
) -> ...:
    try:
        keeper = ArgsKeeper(
            date=str(config.get_job_config["collect_users_demographic_dm_job"]["date"]),
            depth=config.get_job_config["collect_users_demographic_dm_job"][
                "depth"
            ],  # type:ignore
            src_path=config.get_job_config["collect_users_demographic_dm_job"][
                "src_path"
            ],  # type:ignore
            tgt_path=config.get_job_config["collect_users_demographic_dm_job"][
                "tgt_path"
            ],  # type:ignore
            coords_path=config.get_job_config["collect_users_demographic_dm_job"][
                "coords_path"
            ],  # type:ignore
            processed_dttm=datetime.now().strftime(r"%Y-%m-%d %H:%M:%S").replace(" ", "T"),  # type: ignore
        )
    except ValueError as err:
        logger.error(err)
        sys.exit(1)
    except UserWarning as err:
        logger.warning(err)
        pass

    spark_submitter.submit_job(job="collect_events_total_cnt_agg_wk_mnth_dm_job", keeper=keeper)  # type: ignore


@task(default_args=DEFAULT_ARGS)
def collect_add_to_friends_recommendations_dm_job(
    spark_submitter: SparkSubmitter, job_args: Dict
) -> ...:
    try:
        keeper = ArgsKeeper(
            date=str(config.get_job_config["collect_users_demographic_dm_job"]["date"]),
            depth=config.get_job_config["collect_users_demographic_dm_job"][
                "depth"
            ],  # type:ignore
            src_path=config.get_job_config["collect_users_demographic_dm_job"][
                "src_path"
            ],  # type:ignore
            tgt_path=config.get_job_config["collect_users_demographic_dm_job"][
                "tgt_path"
            ],  # type:ignore
            coords_path=config.get_job_config["collect_users_demographic_dm_job"][
                "coords_path"
            ],  # type:ignore
            processed_dttm=datetime.now().strftime(r"%Y-%m-%d %H:%M:%S").replace(" ", "T"),  # type: ignore
        )
    except ValueError as err:
        logger.error(err)
        sys.exit(1)
    except UserWarning as err:
        logger.warning(err)
        pass

    spark_submitter.submit_job(job="collect_add_to_friends_recommendations_dm_job", keeper=keeper)  # type: ignore


@task(
    default_args=DEFAULT_ARGS,
    trigger_rule="all_success",
)
def stop_cluster_success_way(cluster: DataProcCluster) -> None:
    "Stop Cluster if every of upstream tasks successfully executed, if not - skipped"
    cluster.exec_command(command="stop")


@task(
    default_args=DEFAULT_ARGS,
    trigger_rule="one_failed",
)
def stop_cluster_failed_way(cluster: DataProcCluster) -> None:
    "Stop Cluster if one of the upstream tasks failed, if not - skipped"
    cluster.exec_command(command="stop")


@task(
    default_args=DEFAULT_ARGS,
    trigger_rule="all_done",
)
def wait_until_cluster_stopped(cluster: DataProcCluster) -> None:
    "Wait until Cluster is stopped"
    cluster.check_status(target_status="stopped")


@dag(
    dag_id="datamart-collector-dag",
    schedule="0 2 * * *",
    start_date=datetime(2023, 4, 3),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["spark", "de-dataproc-06"],
    default_args={
        "owner": "@leonidgrishenkov",
    },
    default_view="grid",
)
def taskflow() -> None:
    cluster = DataProcCluster()
    submitter = SparkSubmitter()
    config = Config()
    config.get_job_config["collect_add_to_friends_recommendations_dm_job"]

    begin = EmptyOperator(task_id="begining")

    start = start_cluster(cluster=cluster)
    is_running = wait_until_cluster_running(cluster=cluster)

    user_info = collect_users_demographic_dm_job(
        spark_submitter=submitter, config=config
    )
    location_zone_agg = submit_location_zone_agg_datamart_job(
        spark_submitter=submitter, config=config
    )
    # friend_reco = submit_friend_recommendation_datamart_job(spark_submitter=submitter, config=config)

    stop_if_failed = stop_cluster_failed_way(cluster=cluster)
    stop_if_success = stop_cluster_success_way(cluster=cluster)

    is_stopped = wait_until_cluster_stopped(cluster=cluster)

    end = EmptyOperator(task_id="ending")

    chain(
        begin,
        start,
        is_running,
        user_info,
        location_zone_agg,
        # friend_reco,
        [stop_if_failed, stop_if_success],
        is_stopped,
        end,
    )


taskflow()
