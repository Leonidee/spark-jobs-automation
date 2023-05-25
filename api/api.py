import subprocess
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.utils import ArgsKeeper

app = FastAPI()


@app.post("/submit_tags_job")
def submit_tags_job(holder: TagsJobArgsHolder):
    CMD = [
        "/usr/bin/spark-submit",
        "/home/ubuntu/code/spark-jobs-automation/jobs/tags_job.py",
        holder.date,
        str(holder.depth),
        str(holder.threshold),
        holder.tags_verified_path,
        holder.src_path,
        holder.tgt_path,
    ]
    output = subprocess.run(args=CMD, capture_output=True, text=True, encoding="utf-8")

    return output


@app.post("/submit_users_info_datamart_job")
def submit_users_info_datamart_job(keeper: ArgsKeeper):
    CMD = [
        "/usr/bin/spark-submit",
        "/home/ubuntu/code/spark-jobs-automation/jobs/users_info_datamart_job.py",
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
