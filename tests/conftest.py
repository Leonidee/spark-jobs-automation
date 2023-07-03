import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.cluster import DataProcCluster
from src.config import Config
from src.environ import EnvironManager
from src.helper import SparkHelper
from src.keeper import ArgsKeeper, SparkConfigKeeper
from src.notifyer import TelegramNotifyer
from src.submitter import SparkSubmitter


@pytest.fixture
def cluster() -> DataProcCluster:
    """Returns instance of `DataProcCluster` class"""
    return DataProcCluster(retry_delay=1)


@pytest.fixture
def helper() -> SparkHelper:
    """Returns instance of `SparkHelper` class"""
    return SparkHelper()


@pytest.fixture
def keeper() -> ArgsKeeper:
    """Returns filled instance of `ArgsKeeper` class"""
    keeper = ArgsKeeper(
        date="2022-04-03",
        depth=10,
        src_path="s3a://...",
        tgt_path="s3a://...",
        processed_dttm="2023-05-22T12:03:25",
    )
    return keeper


@pytest.fixture
def config_keeper() -> SparkConfigKeeper:
    """Returns instance of `SparkConfigKeeper` dataclass with filled valid parameters"""
    conf = SparkConfigKeeper(
        executor_memory="2g", executor_cores=1, max_executors_num=24
    )
    return conf


@pytest.fixture
def notifyer() -> TelegramNotifyer:
    """Returns instance of `TelegramNotifyer` class"""
    return TelegramNotifyer(max_retries=3, retry_delay=1)


@pytest.fixture
def airflow_context():
    """Returns Airflow context dictionary"""

    return dict(
        task_instance=MagicMock(
            task_id="my_task_id",
            dag_id="my_dag_id",
        ),
        execution_date="2022-01-01T00:00:00.000000",
    )


@pytest.fixture
def submitter() -> SparkSubmitter:
    os.environ["CLUSTER_API_BASE_URL"] = "http://example.com"
    return SparkSubmitter(session_timeout=1, retry_delay=1)


@pytest.fixture
def test_job_name():
    return "users_info_datamart_job"


@pytest.fixture
def config() -> Config:
    return Config(config_name="config.yaml")


@pytest.fixture
def environ():
    return EnvironManager()
