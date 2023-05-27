import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from requests.exceptions import HTTPError

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.cluster import DataProcCluster
from src.config import Config

config = Config()


@pytest.fixture
def cluster() -> DataProcCluster:
    return DataProcCluster()


@patch("src.cluster.requests.post")
def test_get_iam_token(mock_post, cluster):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"iamToken": "test_token"}
    mock_post.return_value = mock_response

    token = cluster._get_iam_token()

    assert token == "test_token"


# # * Type: class
# # * Name: DataProcCluster
# @patch("src.cluster.requests")
# def test_start_cluster_if_ok(mock_request) -> None:
#     "Test main `DataProcCluster.start()` functionality"

#     mock_response = Mock()
#     mock_response.status_code = 200
#     mock_response.json.return_value = {
#         "key": "value",
#     }

#     mock_request.post.return_value = mock_response

#     assert cluster.start() == None


# def test_cluster_start_exit_if_invalid_schema():
#     "Test if raise `SystemExit` when invalid schema was specified"

#     with pytest.raises(SystemExit):
#         cluster.base_url = "wrong_url"

#         cluster.start()


# @patch("src.cluster.requests")
# def test_cluster_start_exit_if_error(mock_request) -> None:
#     "Test `DataProcCluster.start()` exit if error API response"
#     mock_response = Mock()
#     mock_response.raise_for_status.side_effect = HTTPError("Mock HTTPError")

#     mock_request.post.return_value = mock_response

#     with pytest.raises(SystemExit):
#         cluster.start()


# @patch("src.cluster.requests")
# def test_cluster_is_runnig_if_ok(mock_request) -> None:
#     "Test main `DataProcCluster.is_running()` functionality"

#     mock_response = Mock()
#     mock_response.status_code = 200
#     mock_response.json.return_value = {
#         "status": "RUNNING",
#     }

#     mock_request.get.return_value = mock_response

#     assert cluster.is_running() == None


# @patch("src.cluster.requests")
# def test_cluster_is_runnig_exit_if_no_more_attempts(mock_request) -> None:
#     "Test `DataProcCluster.is_running()` exit if no more attempt left to check Cluster status"

#     mock_response = Mock()
#     mock_response.status_code = 200
#     mock_response.json.return_value = {
#         "status": "STARTING",
#     }

#     mock_request.get.return_value = mock_response

#     with pytest.raises(SystemExit):
#         cluster.max_attempts_to_check_status = 1
#         cluster.is_running()


# @patch("src.cluster.requests")
# def test_cluster_is_runnig_exit_if_error(mock_request) -> None:
#     "Test `DataProcCluster.is_running()` exit if error API response"
#     mock_response = Mock()
#     mock_response.raise_for_status.side_effect = HTTPError("Mock HTTPError")

#     mock_request.get.return_value = mock_response

#     with pytest.raises(SystemExit):
#         cluster.is_running()
