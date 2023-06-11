from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING

# airflow
from airflow.decorators import dag, task  # type: ignore
from airflow.models.baseoperator import chain as chain_tasks  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore

if TYPE_CHECKING:
    from typing import Dict

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.cluster import DataProcCluster, YandexAPIError
from src.config import Config, UnableToGetConfig
from src.environ import DotEnvError, EnvironNotSet
from src.keeper import ArgsKeeper
from src.notifyer import TelegramNotifyer
from src.submitter import (
    SparkSubmitter,
    UnableToGetResponse,
    UnableToSendRequest,
    UnableToSubmitJob,
)

notifyer = TelegramNotifyer()
logger = getLogger("aiflow.task")

DEFAULT_ARGS = dict(
    retries=3,
    retry_delay=timedelta(seconds=45),
    on_failure_callback=notifyer.notify_on_task_failure,
)


@task(default_args=DEFAULT_ARGS)
def start_cluster(cluster: DataProcCluster) -> ...:
    "Starts DataProc Cluster"
    try:
        cluster.exec_command(command="start")
    except (YandexAPIError, UnableToGetConfig, DotEnvError, EnvironNotSet) as err:
        logger.exception(err)
        sys.exit(1)


@task(default_args=DEFAULT_ARGS)
def wait_until_cluster_running(cluster: DataProcCluster) -> ...:
    "Waits until Cluster will be ready to use"
    try:
        cluster.check_status(target_status="running")
    except (YandexAPIError, UnableToGetConfig, DotEnvError, EnvironNotSet) as err:
        logger.exception(err)
        sys.exit(1)


@task(default_args=DEFAULT_ARGS)
def collect_users_demographic_dm_job(
    spark_submitter: SparkSubmitter, job_args: Dict[str, str | int | date]
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

    try:
        spark_submitter.submit_job(job="collect_users_demographic_dm_job", keeper=keeper)  # type: ignore
    except (
        UnableToGetResponse,
        UnableToSendRequest,
        UnableToSubmitJob,
        UnableToGetConfig,
        DotEnvError,
        EnvironNotSet,
    ) as err:
        logger.exception(err)
        sys.exit(1)


@task(default_args=DEFAULT_ARGS)
def collect_events_total_cnt_agg_wk_mnth_dm_job(
    spark_submitter: SparkSubmitter, job_args: Dict[str, str | int | date]
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

    try:
        spark_submitter.submit_job(job="collect_events_total_cnt_agg_wk_mnth_dm_job", keeper=keeper)  # type: ignore
    except (
        UnableToGetResponse,
        UnableToSendRequest,
        UnableToSubmitJob,
        UnableToGetConfig,
        DotEnvError,
        EnvironNotSet,
    ) as err:
        logger.exception(err)
        sys.exit(1)


@task(default_args=DEFAULT_ARGS)
def collect_add_to_friends_recommendations_dm_job(
    spark_submitter: SparkSubmitter, job_args: Dict[str, str | int | date]
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

    try:
        spark_submitter.submit_job(job="collect_add_to_friends_recommendations_dm_job", keeper=keeper)  # type: ignore
    except (
        UnableToGetResponse,
        UnableToSendRequest,
        UnableToSubmitJob,
        UnableToGetConfig,
        DotEnvError,
        EnvironNotSet,
    ) as err:
        logger.exception(err)
        sys.exit(1)


@task(
    default_args=DEFAULT_ARGS,
    trigger_rule="all_success",
)
def stop_cluster_success_way(cluster: DataProcCluster) -> ...:
    "Stops Cluster if every of upstream tasks successfully executed, if not - skipped"
    try:
        cluster.exec_command(command="stop")
    except (YandexAPIError, UnableToGetConfig, DotEnvError, EnvironNotSet) as err:
        logger.exception(err)
        sys.exit(1)


@task(
    default_args=DEFAULT_ARGS,
    trigger_rule="one_failed",
)
def stop_cluster_failed_way(cluster: DataProcCluster) -> ...:
    "Stops Cluster if one of the upstream tasks failed, if not - skipped"
    try:
        cluster.exec_command(command="stop")
    except (YandexAPIError, UnableToGetConfig, DotEnvError, EnvironNotSet) as err:
        logger.exception(err)
        sys.exit(1)


@task(
    default_args=DEFAULT_ARGS,
    trigger_rule="all_done",
)
def wait_until_cluster_stopped(cluster: DataProcCluster) -> ...:
    "Waits until Cluster will be stopped"
    try:
        cluster.check_status(target_status="stopped")
    except (YandexAPIError, UnableToGetConfig, DotEnvError, EnvironNotSet) as err:
        logger.exception(err)
        sys.exit(1)


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
def taskflow() -> ...:
    cluster = DataProcCluster()
    submitter = SparkSubmitter()
    config = Config()

    begin = EmptyOperator(task_id="begining")

    start = start_cluster(cluster=cluster)
    is_running = wait_until_cluster_running(cluster=cluster)

    users_demographic_dm = collect_users_demographic_dm_job(
        spark_submitter=submitter,
        job_args=config.get_job_config["collect_users_demographic_dm_job"],
    )
    events_total_cnt_agg_wk_mnth_dm = collect_events_total_cnt_agg_wk_mnth_dm_job(
        spark_submitter=submitter,
        job_args=config.get_job_config["collect_events_total_cnt_agg_wk_mnth_dm_job"],
    )
    add_to_friends_recommendations_dm = collect_add_to_friends_recommendations_dm_job(
        spark_submitter=submitter,
        job_args=config.get_job_config["collect_add_to_friends_recommendations_dm_job"],
    )

    stop_cluster_failed = stop_cluster_failed_way(cluster=cluster)
    stop_cluster_success = stop_cluster_success_way(cluster=cluster)

    is_stopped = wait_until_cluster_stopped(cluster=cluster)

    end = EmptyOperator(task_id="ending")

    chain_tasks(
        begin,
        start,
        is_running,
        users_demographic_dm,
        events_total_cnt_agg_wk_mnth_dm,
        add_to_friends_recommendations_dm,
        [stop_cluster_failed, stop_cluster_success],
        is_stopped,
        end,
    )


if __name__ == "__main__":
    try:
        taskflow()
    except Exception as err:
        logger.exception(err)
        sys.exit(1)
