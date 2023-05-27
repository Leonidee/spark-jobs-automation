import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).parent.parent))
from src.datamodel.datamodel import (
    SparkArgsValidator,
    TagsJobArgsHolder,
    load_environment,
)

spark_validator = SparkArgsValidator()

tags_job_args = TagsJobArgsHolder(
    date="2022-03-24",
    depth=7,
    threshold=50,
    tags_verified_path="",
    src_path="",
    tgt_path="",
)


# * Type: class
# * Name: SparkArgsValidator
def test_validator_raises_date_arg():
    with pytest.raises(AssertionError):
        tags_job_args.date = "wrong"
        spark_validator.validate_tags_job_args(holder=tags_job_args)


def test_validator_raises_depth_arg():
    with pytest.raises(AssertionError):
        tags_job_args.depth = "wrong"
        spark_validator.validate_tags_job_args(holder=tags_job_args)


def test_validator_raises_threshold_arg():
    with pytest.raises(AssertionError):
        tags_job_args.threshold = "wrong"
        spark_validator.validate_tags_job_args(holder=tags_job_args)


def test_validator_raises_if_depth_gt():
    with pytest.raises(AssertionError):
        tags_job_args.depth = 151
        spark_validator.validate_tags_job_args(holder=tags_job_args)


def test_validator_raises_if_threshold_gt():
    with pytest.raises(AssertionError):
        tags_job_args.threshold = 5_001
        spark_validator.validate_tags_job_args(holder=tags_job_args)


# * Type: function
# * Name: load_environment
def test_load_environment_raise_if_wrong_path():
    with pytest.raises(SystemExit):
        load_environment(dotenv_file_name="wrong")


def test_load_environment_return_none_if_ok():
    assert load_environment() == None
