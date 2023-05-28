from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.cluster import DataProcCluster
from src.datamodel import ArgsKeeper, SparkConfigKeeper


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
