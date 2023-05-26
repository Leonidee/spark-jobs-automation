import subprocess
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.utils import ArgsKeeper

app = FastAPI()

SPARK_SUBMIT_PATH = "/usr/bin/spark-submit"
PROJECT_PATH = Path(__file__).parent


@app.post("/submit_users_info_datamart_job")
def submit_users_info_datamart_job(keeper: ArgsKeeper):
    CMD = [
        SPARK_SUBMIT_PATH,
        f"{PROJECT_PATH}/jobs/users_info_datamart_job.py",
        keeper.date,
        str(keeper.depth),
        keeper.src_path,
        keeper.tgt_path,
        keeper.processed_dttm,
    ]
    output = subprocess.run(args=CMD, capture_output=True, text=True, encoding="utf-8")

    return output


@app.post("/submit_location_zone_agg_datamart_job")
def submit_location_zone_agg_datamart_job(keeper: ArgsKeeper):
    CMD = [
        SPARK_SUBMIT_PATH,
        f"{PROJECT_PATH}/jobs/location_zone_agg_datamart_job.py",
        keeper.date,
        str(keeper.depth),
        keeper.src_path,
        keeper.tgt_path,
        keeper.processed_dttm,
    ]
    output = subprocess.run(args=CMD, capture_output=True, text=True, encoding="utf-8")

    return output


@app.post("/submit_friend_recommendation_datamart_job")
def submit_friend_recommendation_datamart_job(keeper: ArgsKeeper):
    CMD = [
        SPARK_SUBMIT_PATH,
        f"{PROJECT_PATH}/jobs/friend_recommendation_datamart_job.py",
        keeper.date,
        str(keeper.depth),
        keeper.src_path,
        keeper.tgt_path,
        keeper.processed_dttm,
    ]
    output = subprocess.run(args=CMD, capture_output=True, text=True, encoding="utf-8")

    return output


if __name__ == "__main__":
    config = uvicorn.Config("api:app", host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config=config)
    server.run()
