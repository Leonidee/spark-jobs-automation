from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.cluster import DataProcCluster
from src.datamodel import ArgsKeeper, SparkConfigKeeper
from src.utils import TelegramNotifyer


@pytest.fixture
def cluster() -> DataProcCluster:
    return DataProcCluster()


@pytest.fixture
def keeper() -> ArgsKeeper:
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
    conf = SparkConfigKeeper(
        executor_memory="2g", executor_cores=1, max_executors_num=24
    )
    return conf


@pytest.fixture
def notifyer():
    return TelegramNotifyer()


@pytest.fixture
def airflow_context():
    context = dict(
        task_instance=MagicMock(
            task_id="my_task_id",
            dag_id="my_dag_id",
        ),
        execution_date="2022-01-01T00:00:00.000000",
    )
    return context
