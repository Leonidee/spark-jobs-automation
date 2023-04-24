import pytest
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from src.utils import (
    validate_job_submit_args,
    get_src_paths,
    get_s3_instance,
    load_environment,
)


# * Type: function
# * Name: assert_args
def test_assert_args_raises_date_arg():
    with pytest.raises(AssertionError):
        validate_job_submit_args(date="WRONG", depth="10", threshold=100)


def test_assert_args_raises_depth_arg():
    with pytest.raises(AssertionError):
        validate_job_submit_args(date="2022-03-03", depth="WRONG", threshold=100)


def test_assert_args_raises_threshold_arg():
    with pytest.raises(AssertionError):
        validate_job_submit_args(date="2022-03-03", depth="10", threshold="WRONG")


# * Type: function
# * Name: load_environment
def test_load_environment_main():
    assert load_environment()


def test_load_environment_exit_if_not_found():
    with pytest.raises(SystemExit) as ex:
        load_environment(dotenv_file_name="wrong")

    assert ex.type == SystemExit
    assert ex.value.code == 1


# * Type: function
# * Name: get_s3_instance
def test_get_s3_instance_main():
    assert get_s3_instance()


# * Type: function
# * Name: get_src_paths
def test_get_src_paths_main():
    assert get_src_paths(
        event_type="message",
        date="2022-04-03",
        depth="10",
        src_path="s3a://data-ice-lake-04/messager-data/analytics/cleaned-events",
    )


def test_get_src_paths_return_type():
    paths = get_src_paths(
        event_type="message",
        date="2022-04-03",
        depth="10",
        src_path="s3a://data-ice-lake-04/messager-data/analytics/cleaned-events",
    )
    assert isinstance(paths, list)


def test_get_src_paths_exit_if_return_empty_list_date_arg():
    "If there is no data for given `date` should exit"

    with pytest.raises(SystemExit) as ex:
        get_src_paths(
            event_type="message",
            date="2019-09-20",  # <--- wrong value
            depth="10",
            src_path="s3a://data-ice-lake-04/messager-data/analytics/cleaned-events",
        )
    assert ex.type == SystemExit
    assert ex.value.code == 1


def test_get_src_paths_exit_if_wrong_bucket():
    "If there is no bukects for given name in `src_path` should exit"

    with pytest.raises(SystemExit) as ex:
        get_src_paths(
            event_type="message",
            date="2021-09-20",
            depth="5",
            src_path="s3a://wrong/messager-data/analytics/cleaned-events",  # <--- wrong bucket name
        )
    assert ex.type == SystemExit
    assert ex.value.code == 1


def test_get_src_paths_exit_if_wrong_key_1():
    "If there is no keys for given name in `src_path` should exit. First case"

    with pytest.raises(SystemExit) as ex:
        get_src_paths(
            event_type="message",
            date="2021-09-20",
            depth="5",
            src_path="s3a://data-ice-lake-04/wrong-path/analytics/cleaned-events",  # <--- wrong key
        )
    assert ex.type == SystemExit
    assert ex.value.code == 1


def test_get_src_paths_exit_if_wrong_key_2():
    "If there is no keys for given name in `src_path` should exit. Second case"

    with pytest.raises(SystemExit) as ex:
        get_src_paths(
            event_type="message",
            date="2021-09-20",
            depth="5",
            src_path="s3a://data-ice-lake-04/messager-data/wrong-path/cleaned-events",  # <--- wrong key in another place
        )
    assert ex.type == SystemExit
    assert ex.value.code == 1
