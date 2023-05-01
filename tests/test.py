import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.logger import SparkLogger
from src.submiter import SparkSubmitter
from src.utils import TagsJobArgsHolder, load_environment

config = Config()

SPARK_REPORT_DATE = "2022-03-24"
TAGS_VERIFIED_PATH = config.tags_job_config["TAGS_VERIFIED_PATH"]
SRC_PATH = config.tags_job_config["SRC_PATH"]
TGT_PATH = config.tags_job_config["TGT_PATH"]

load_environment()

YC_DATAPROC_CLUSTER_ID = os.getenv("YC_DATAPROC_CLUSTER_ID")
YC_DATAPROC_BASE_URL = os.getenv("YC_DATAPROC_BASE_URL")
YC_OAUTH_TOKEN = os.getenv("YC_OAUTH_TOKEN")

FAST_API_BASE_URL = os.getenv("FAST_API_BASE_URL")


if __name__ == "__main__":
    spark = SparkSubmitter()
    holder = TagsJobArgsHolder(
        date=SPARK_REPORT_DATE,
        depth=2,
        threshold=20,
        tags_verified_path=TAGS_VERIFIED_PATH,
        src_path=SRC_PATH,
        tgt_path=TGT_PATH,
    )
    spark.submit_tags_job(holder=holder)
