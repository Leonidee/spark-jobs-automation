#!/usr/bin/bash
# This script activates project virtual environment
# and run `api.py` python file to deploy FastAPI and Uvicorn server.
# Used by supervisor on Hadoop cluster side

PROJECT_DIR=$(dirname "$(pwd)")

source $PROJECT_DIR/.venv/bin/activate
exec python $PROJECT_DIR/api/api.py


