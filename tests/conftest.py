from __future__ import annotations

import sys
from pathlib import Path
import os

import pytest
from unittest.mock import MagicMock

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.cluster import DataProcCluster
from src.datamodel import ArgsKeeper, SparkConfigKeeper
from src.notifyer import TelegramNotifyer
from src.environ import EnvironManager
from src.config import Config
from src.submitter import SparkSubmitter


@pytest.fixture
def cluster() -> DataProcCluster:
    """Returns instance of `DataProcCluster` class"""
    return DataProcCluster()


@pytest.fixture
def keeper() -> ArgsKeeper:
    """Returns instance of `ArgsKeeper` dataclass with filled valid parameters"""
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
def notifyer():
    """Returns instance of `TelegramNotifyer` class"""
    return TelegramNotifyer()


@pytest.fixture
def airflow_context():
    """Returns Airflow context dictionary"""
    context = dict(
        task_instance=MagicMock(
            task_id="my_task_id",
            dag_id="my_dag_id",
        ),
        execution_date="2022-01-01T00:00:00.000000",
    )
    return context


@pytest.fixture
def environ():
    """Returns instance of `EnvironManager` class"""
    return EnvironManager()


@pytest.fixture
def config():
    """Returns instance of `Config` class"""
    return Config(config_name="config.yaml")


@pytest.fixture
def submitter():
    os.environ["CLUSTER_API_BASE_URL"] = "http://example.com"
    return SparkSubmitter(session_timeout=1)


@pytest.fixture
def test_job_name():
    return "users_info_datamart_job"
