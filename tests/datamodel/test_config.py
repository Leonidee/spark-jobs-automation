import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.datamodel import SparkConfigKeeper


def test_spark_conf_keeper_exec_mem_valid(config_keeper):
    assert config_keeper.executor_memory == "2g"


def test_spark_conf_keeper_exec_cores_valid(config_keeper):
    assert config_keeper.executor_cores == 1


def test_spark_conf_keeper_max_exec_num_valid(config_keeper):
    assert config_keeper.max_executors_num == 24


def test_spark_conf_keeper_exec_mem_raises_if_wrong_pat():
    with pytest.raises(ValueError):
        SparkConfigKeeper(
            executor_memory="20gb", executor_cores=1, max_executors_num=24
        )


def test_spark_conf_keeper_exec_cores_raises_if_not_valid():
    with pytest.raises(ValueError):
        SparkConfigKeeper(executor_memory="2g", executor_cores=15, max_executors_num=24)


def test_spark_conf_keeper_exec_cores_warns_if_not_valid():
    with pytest.raises(UserWarning):
        SparkConfigKeeper(executor_memory="2g", executor_cores=6, max_executors_num=24)


def test_spark_conf_keeper_max_exec_num_raises_if_not_valid():
    with pytest.raises(ValueError):
        SparkConfigKeeper(executor_memory="2g", executor_cores=4, max_executors_num=66)
