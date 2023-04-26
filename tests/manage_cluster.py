import sys
from pathlib import Path
import os

sys.path.append(str(Path(__file__).parent.parent))

from src.main import DataProcCluster, YandexCloudAPI
from src.utils import load_environment

load_environment()

YC_DATAPROC_CLUSTER_ID = os.getenv("YC_DATAPROC_CLUSTER_ID")
YC_DATAPROC_BASE_URL = os.getenv("YC_DATAPROC_BASE_URL")
YC_OAUTH_TOKEN = os.getenv("YC_OAUTH_TOKEN")
FAST_API_BASE_URL = os.getenv("FAST_API_BASE_URL")

if __name__ == "__main__":
    yc = YandexCloudAPI()
    token = yc.get_iam_token(oauth_token=YC_OAUTH_TOKEN)

    cluster = DataProcCluster(
        token=token,
        cluster_id=YC_DATAPROC_CLUSTER_ID,
        base_url=YC_DATAPROC_BASE_URL,
        max_attempts_to_check_status=20,
    )
    cluster.start()
    cluster.is_running()
