#!/usr/bin/bash
# This script activates project virtual environment
# and run `api.py` python file to deploy FastAPI and Uvicorn server.
# Used by supervisor on Hadoop cluster side

source /home/ubuntu/code/spark-jobs-automation/.venv/bin/activate
exec python /home/ubuntu/code/spark-jobs-automation/api/api.py
