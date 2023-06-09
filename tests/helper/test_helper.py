import sys
from pathlib import Path

import pytest

# package
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.helper import S3ServiceError


class TestCheckS3ObjectExistence:
    def test_raises_if_wrong_type(self, helper):
        with pytest.raises(KeyError) as err:
            helper.check_s3_object_existence(key="test", type="test")

        assert err.type is KeyError
        assert "Only 'object' or 'bucket' are allowed as 'type'" in str(err.value)

    def test_object_success(self, helper, keeper):
        keeper.src_path = "s3a://data-ice-lake-05/messager-data/analytics/geo-events/event_type=message/date=2022-01-20"  # existing object

        result = helper.check_s3_object_existence(key=keeper.src_path, type="object")

        assert result is True

    def test_object_failed(self, helper, keeper):
        keeper.src_path = "s3a://data-ice-lake-05/messager-data/analytics/geo-events/event_type=message/date=2022-01-01"  # non existing object

        result = helper.check_s3_object_existence(key=keeper.src_path, type="object")

        assert result == False

    def test_bucket_success(self, helper, keeper):
        keeper.src_path = "s3a://data-ice-lake-05/messager-data/analytics/geo-events/event_type=message/date=2022-01-01"  # existing bucket

        result = helper.check_s3_object_existence(
            key=keeper.src_path.split(sep="/")[2], type="bucket"
        )
        assert result is True

    def test_bucket_failed(self, helper, keeper):
        keeper.src_path = "s3a://data-ice-lake-10/messager-data/analytics/geo-events/event_type=message/date=2022-01-01"  # non existing bucket

        with pytest.raises(S3ServiceError) as err:
            helper.check_s3_object_existence(
                key=keeper.src_path.split(sep="/")[2], type="bucket"
            )

        assert err.type is S3ServiceError
        assert f"Bucket '{keeper.src_path.split(sep='/')[2]}' does not exists" in str(
            err.value
        )


class TestGetSrcPath:
    def test_success(self, helper, keeper):
        keeper.src_path = "s3a://data-ice-lake-05/messager-data/analytics/geo-events"

        paths = helper._get_src_paths(event_type="message", keeper=keeper)

        assert isinstance(paths, tuple)
        assert len(paths) == 10

    def test_failed(self, helper, keeper):
        keeper.src_path = "s3a://data-ice-lake-05/messager-data/analytics/test-path"

        with pytest.raises(S3ServiceError) as err:
            helper._get_src_paths(event_type="message", keeper=keeper)

        assert err.type is S3ServiceError
        assert "No data on S3 for given arguments" in str(err.value)
