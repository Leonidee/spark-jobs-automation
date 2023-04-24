import sys
from pathlib import Path
import os
import requests

import pytest
from unittest.mock import patch, Mock

sys.path.append(str(Path(__file__).parent.parent))
from src.main import YandexCloudAPI, DataProcCluster, SparkSubmitter
from src.utils import load_environment

load_environment()

YC_DATAPROC_CLUSTER_ID = os.getenv("YC_DATAPROC_CLUSTER_ID")
YC_DATAPROC_BASE_URL = os.getenv("YC_DATAPROC_BASE_URL")
YC_OAUTH_TOKEN = os.getenv("YC_OAUTH_TOKEN")
FAST_API_BASE_URL = os.getenv("FAST_API_BASE_URL")

TAGS_VERIFIED_PATH = (
    "s3a://data-ice-lake-04/messager-data/snapshots/tags_verified/actual"
)
SRC_PATH = "s3a://data-ice-lake-04/messager-data/analytics/cleaned-events"
TGT_PATH = "s3a://data-ice-lake-04/messager-data/analytics/tag-candidates"

yc = YandexCloudAPI()

cluster = DataProcCluster(
    token=yc.get_iam_token(oauth_token=YC_OAUTH_TOKEN),
    cluster_id=YC_DATAPROC_CLUSTER_ID,
    base_url=YC_DATAPROC_BASE_URL,
)

spark = SparkSubmitter(api_base_url=FAST_API_BASE_URL)


# * Type: class
# * Name: YandexCloudAPI
@patch("src.main.requests.post")
def test_get_iam_token_main(mock_request) -> None:
    "Test main `YandexCloudAPI.get_iam_token()` functionality with mock request"
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"iamToken": "12345"}

    mock_request.return_value = mock_response

    assert yc.get_iam_token(oauth_token="12345") == "12345"


def test_get_iam_token_exit_if_wrong_oauth_token() -> None:
    "Test if raise `SystemExit` if wrong OAuthToken was specified in request"
    with pytest.raises(SystemExit) as ex:
        yc.get_iam_token(oauth_token="wrong_token")

    assert ex.type == SystemExit
    assert ex.value.code == 1


@patch("src.main.requests.post")
def test_get_iam_token_exit_if_not_token(mock_request) -> None:
    "Test if raise `SystemExit` if no iamToken in API responce"

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "wrong_key": "wrong_value",
    }

    mock_request.return_value = mock_response

    with pytest.raises(SystemExit) as ex:
        yc.get_iam_token(oauth_token="12345")

    assert ex.type == SystemExit
    assert ex.value.code == 1


# * Type: class
# * Name: DataProcCluster
@patch("src.main.requests.post")
def test_start_cluster_main(mock_request) -> None:
    "Test main `DataProcCluster.start()` functionality with mock request"

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "key": "value",
    }

    mock_request.return_value = mock_response

    assert cluster.start()


def test_start_cluster_exit_if_invalid_schema():
    "Test if raise `SystemExit` when invalid schema was specified"

    with pytest.raises(SystemExit) as ex:
        cluster.base_url = "wrong_url"

        cluster.start()


def test_start_cluster_exit_if_invalid_auth():
    "Test if raise `SystemExit` when invalid auth data was passed"

    with pytest.raises(SystemExit):
        cluster.base_url = YC_DATAPROC_BASE_URL
        cluster.cluster_id = "12345"

        cluster.start()


@patch("src.main.requests")
def test_cluster_is_runnig_main(mock_request) -> None:
    "Test main `DataProcCluster.start()` functionality with mock request"

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "status": "RUNNING",
    }

    mock_request.get.return_value = mock_response

    assert cluster.is_running()


@patch("src.main.requests")
def test_cluster_is_runnig_exit_if_no_more_attempts(mock_request) -> None:
    "Test `DataProcCluster.is_running()` exit if no more attempt left to check Cluster status"

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "status": "STARTING",
    }

    mock_request.get.return_value = mock_response

    with pytest.raises(SystemExit):
        cluster.max_attempts_to_check_status = 1
        cluster.is_running()


@patch("src.main.requests")
def test_cluster_is_runnig_exit_if_error_status_code(mock_request) -> None:
    "Test `DataProcCluster.is_running()` exit if error code in API response"
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()

    mock_request.get.return_value = mock_response

    with pytest.raises(SystemExit):
        cluster.is_running()


@patch("src.main.requests")
def test_cluster_is_runnig_exit_if_schema_error(mock_request) -> None:
    "Test `DataProcCluster.is_running()` exit if `InvalidSchema` was raised"
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.InvalidSchema()

    mock_request.get.return_value = mock_response

    with pytest.raises(SystemExit):
        cluster.is_running()


@patch("src.main.requests")
def test_cluster_is_runnig_exit_if_connection_error(mock_request) -> None:
    "Test `DataProcCluster.is_running()` exit if `ConnectionError` was raised"
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.ConnectionError()

    mock_request.get.return_value = mock_response

    with pytest.raises(SystemExit):
        cluster.is_running()


@patch("src.main.requests")
def test_test(mock_request) -> None:
    """"""

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "returncode": 0,
    }
    mock_request.get.return_value = mock_response

    assert ... == None
