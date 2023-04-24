import sys
from pathlib import Path
import os

sys.path.append(str(Path(__file__).parent.parent))

from src.main import SparkSubmitter
from src.utils import load_environment

load_environment()

FAST_API_BASE_URL = os.getenv("FAST_API_BASE_URL")
TAGS_VERIFIED_PATH = (
    "s3a://data-ice-lake-04/messager-data/snapshots/tags_verified/actual"
)
SRC_PATH = "s3a://data-ice-lake-04/messager-data/analytics/cleaned-events"
TGT_PATH = "s3a://data-ice-lake-04/messager-data/analytics/tag-candidates"

spark = SparkSubmitter(api_base_url=FAST_API_BASE_URL)


if __name__ == "__main__":
    spark.submit_tags_job(
        date="2022-01-19",
        depth=10,
        threshold=100,
        tags_verified_path=TAGS_VERIFIED_PATH,
        src_path=SRC_PATH,
        tgt_path=TGT_PATH,
    )
