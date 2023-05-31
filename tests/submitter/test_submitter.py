import os
import sys
from pathlib import Path

import pytest
from unittest.mock import patch, MagicMock

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.submitter import SparkSubmitter


@pytest.fixture
def submitter():
    os.environ["CLUSTER_API_BASE_URL"] = "http://example.com"
    return SparkSubmitter(session_timeout=30)


class TestSubmitJob:
    @patch("src.submitter.submitter.requests.post")
    def test_submit_job_success(self, mock_post, submitter, keeper):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = dict(
            returncode=0, stdout="some_value", stderr=""
        )
        mock_post.return_value = mock_response

        assert (
            submitter.submit_job(job="users_info_datamart_job", keeper=keeper) == True
        )
