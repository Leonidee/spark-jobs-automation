import sys
from datetime import datetime, timedelta
from pathlib import Path
from logging import getLogger

# airflow
from airflow.decorators import dag, task  # type: ignore
from airflow.models.baseoperator import chain  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.cluster import DataProcCluster
from src.config import Config
from src.notifyer import TelegramNotifyer
from src.submitter import SparkSubmitter
from src.datamodel import ArgsKeeper

notifyer = TelegramNotifyer()
logger = getLogger("aiflow.task")


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=45),
        "on_failure_callback": notifyer.notify_on_task_failure,
    },
)
def start_cluster(cluster: DataProcCluster) -> None:
    "Start DataProc Cluster"
    cluster.exec_command(command="start")


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notifyer.notify_on_task_failure,
    },
)
def wait_until_cluster_running(cluster: DataProcCluster) -> None:
    "Wait until Cluster is ready to use"
    cluster.check_status(target_status="running")


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=45),
        "on_failure_callback": notifyer.notify_on_task_failure,
    }
)
def submit_users_info_datamart_job(
    spark_submitter: SparkSubmitter, config: Config
) -> None:
    try:
        keeper = ArgsKeeper(
            date=config.get_users_info_datamart_config["DATE"],
            depth=config.get_users_info_datamart_config["DEPTH"],
            src_path=config.get_users_info_datamart_config["SRC_PATH"],
            tgt_path=config.get_users_info_datamart_config["TGT_PATH"],
            processed_dttm=datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace(" ", "T"),  # type: ignore
        )
    except ValueError as e:
        logger.error(e)
        sys.exit(1)
    except UserWarning as e:
        logger.warning(e)
        pass

    spark_submitter.submit_job(job="users_info_datamart_job", keeper=keeper)  # type: ignore


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=45),
        "on_failure_callback": notifyer.notify_on_task_failure,
    }
)
def submit_location_zone_agg_datamart_job(
    spark_submitter: SparkSubmitter, config: Config
) -> None:
    try:
        keeper = ArgsKeeper(
            date=config.get_location_zone_agg_datamart_config["DATE"],
            depth=config.get_location_zone_agg_datamart_config["DEPTH"],
            src_path=config.get_location_zone_agg_datamart_config["SRC_PATH"],
            tgt_path=config.get_location_zone_agg_datamart_config["TGT_PATH"],
            processed_dttm=datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace(" ", "T"),  # type: ignore
        )
    except ValueError as e:
        logger.error(e)
        sys.exit(1)
    except UserWarning as e:
        logger.warning(e)
        pass

    spark_submitter.submit_job(job="location_zone_agg_datamart_job", keeper=keeper)  # type: ignore


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=45),
        "on_failure_callback": notifyer.notify_on_task_failure,
    }
)
def submit_friend_recommendation_datamart_job(
    spark_submitter: SparkSubmitter, config: Config
) -> None:
    try:
        keeper = ArgsKeeper(
            date=config.get_friend_recommendation_datamart_config["DATE"],
            depth=config.get_friend_recommendation_datamart_config["DEPTH"],
            src_path=config.get_friend_recommendation_datamart_config["SRC_PATH"],
            tgt_path=config.get_friend_recommendation_datamart_config["TGT_PATH"],
            processed_dttm=datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace(" ", "T"),  # type: ignore
        )
    except ValueError as e:
        logger.error(e)
        sys.exit(1)
    except UserWarning as e:
        logger.warning(e)
        pass

    spark_submitter.submit_job(job="friend_recommendation_datamart_job", keeper=keeper)  # type: ignore


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": notifyer.notify_on_task_failure,
    },
    trigger_rule="all_success",
)
def stop_cluster_success_way(cluster: DataProcCluster) -> None:
    "Stop Cluster if every of upstream tasks successfully executed, if not - skipped"
    cluster.exec_command(command="stop")


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": notifyer.notify_on_task_failure,
    },
    trigger_rule="one_failed",
)
def stop_cluster_failed_way(cluster: DataProcCluster) -> None:
    "Stop Cluster if one of the upstream tasks failed, if not - skipped"
    cluster.exec_command(command="stop")


@task(
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notifyer.notify_on_task_failure,
    },
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

    begin = EmptyOperator(task_id="begining")

    start = start_cluster(cluster=cluster)
    is_running = wait_until_cluster_running(cluster=cluster)

    user_info = submit_users_info_datamart_job(spark_submitter=submitter, config=config)
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
