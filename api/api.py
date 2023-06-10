import subprocess
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.keeper import ArgsKeeper

app = FastAPI()

SPARK_SUBMIT_PATH = "/usr/bin/spark-submit"
PROJECT_PATH = str(Path(__file__).parent.parent)


@app.post("/submit_collect_users_demographic_dm_job")
def submit_collect_users_demographic_dm_job(keeper: ArgsKeeper):
    CMD = [
        SPARK_SUBMIT_PATH,
        f"{PROJECT_PATH}/jobs/collect_users_demographic_dm_job.py",
        keeper.date,
        str(keeper.depth),
        keeper.src_path,
        keeper.tgt_path,
        keeper.coords_path,
        keeper.processed_dttm,
    ]
    output = subprocess.run(args=CMD, capture_output=True, text=True, encoding="utf-8")

    return output


@app.post("/submit_collect_events_total_cnt_agg_wk_mnth_dm_job")
def submit_collect_events_total_cnt_agg_wk_mnth_dm_job(keeper: ArgsKeeper):
    CMD = [
        SPARK_SUBMIT_PATH,
        f"{PROJECT_PATH}/jobs/collect_events_total_cnt_agg_wk_mnth_dm_job.py",
        keeper.date,
        str(keeper.depth),
        keeper.src_path,
        keeper.tgt_path,
        keeper.coords_path,
        keeper.processed_dttm,
    ]
    output = subprocess.run(args=CMD, capture_output=True, text=True, encoding="utf-8")

    return output


@app.post("/submit_collect_add_to_friends_recommendations_dm_job")
def submit_collect_add_to_friends_recommendations_dm_job(keeper: ArgsKeeper):
    CMD = [
        SPARK_SUBMIT_PATH,
        f"{PROJECT_PATH}/jobs/collect_add_to_friends_recommendations_dm_job.py",
        keeper.date,
        str(keeper.depth),
        keeper.src_path,
        keeper.tgt_path,
        keeper.coords_path,
        keeper.processed_dttm,
    ]
    output = subprocess.run(args=CMD, capture_output=True, text=True, encoding="utf-8")

    return output


if __name__ == "__main__":
    config = uvicorn.Config("api:app", host="0.0.0.0", port=8000, log_level="debug")
    server = uvicorn.Server(config=config)
    server.run()
