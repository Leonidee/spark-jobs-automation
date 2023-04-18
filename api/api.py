from fastapi import FastAPI
import uvicorn
import subprocess
import sys
from pathlib import Path

# package
sys.path.append(str(Path(__file__).resolve().parent.parent))
from jobs.utils import SparkArgsHolder

app = FastAPI()


@app.post("/submit_tags_job")
def submit_tags_job(holder: SparkArgsHolder):
    CMD = [
        "spark-submit",
        "/home/ubuntu/code/spark-jobs-automation/jobs/tags_job.py",
        holder.date,
        holder.depth,
        holder.threshold,
        holder.tags_verified_path,
        holder.src_path,
        holder.tgt_path,
    ]

    output = subprocess.run(args=CMD)

    return output


if __name__ == "__main__":
    config = uvicorn.Config("api:app", host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config=config)
    server.run()
