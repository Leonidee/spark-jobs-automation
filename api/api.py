import subprocess
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.keeper import ArgsKeeper
from src.logger import SparkLogger

app = FastAPI()

SPARK_SUBMIT_EXEC = "/usr/bin/spark-submit"
PROJECT_PATH = str(Path(__file__).parent.parent)
JOBS = (
    "collect_users_demographic_dm_job",
    "collect_events_total_cnt_agg_wk_mnth_dm_job",
    "collect_add_to_friends_recommendations_dm_job",
)

config = Config(
    config_path=Path(Path.home(), "code/spark-jobs-automation/config/config.yaml")
)
logger = SparkLogger(level=config.get_logging_level["python"]).get_logger(name=__name__)


@app.post(f"/submit_{JOBS[0]}")
def submit_collect_users_demographic_dm_job(keeper: ArgsKeeper):
    CMD = [
        SPARK_SUBMIT_EXEC,
        f"{PROJECT_PATH}/jobs/{JOBS[0]}.py",
        keeper.date,
        str(keeper.depth),
        keeper.src_path,
        keeper.tgt_path,
        keeper.coords_path,
        keeper.processed_dttm,
    ]
    output = subprocess.run(args=CMD, capture_output=True, text=True, encoding="utf-8")

    return output


@app.post(f"/submit_{JOBS[1]}")
def submit_collect_events_total_cnt_agg_wk_mnth_dm_job(keeper: ArgsKeeper):
    CMD = [
        SPARK_SUBMIT_EXEC,
        f"{PROJECT_PATH}/jobs/{JOBS[1]}.py",
        keeper.date,
        str(keeper.depth),
        keeper.src_path,
        keeper.tgt_path,
        keeper.coords_path,
        keeper.processed_dttm,
    ]
    output = subprocess.run(args=CMD, capture_output=True, text=True, encoding="utf-8")

    return output


@app.post(f"/submit_{JOBS[2]}")
def submit_collect_add_to_friends_recommendations_dm_job(keeper: ArgsKeeper):
    CMD = [
        SPARK_SUBMIT_EXEC,
        f"{PROJECT_PATH}/jobs/{JOBS[2]}.py",
        keeper.date,
        str(keeper.depth),
        keeper.src_path,
        keeper.tgt_path,
        keeper.coords_path,
        keeper.processed_dttm,
    ]
    output = subprocess.run(args=CMD, capture_output=True, text=True, encoding="utf-8")

    return output


def main() -> ...:
    config = uvicorn.Config(
        "api:app", host="0.0.0.0", port=8000, log_level="info", reload=True
    )
    server = uvicorn.Server(config=config)
    server.run()


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        logger.exception(err)
        sys.exit(1)
