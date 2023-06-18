#!/usr/bin/env bash

SPARK_SUBMIT_BIN=$(which spark-submit)
PROJECT_PATH=/home/ubuntu/code/spark-jobs-automation

echo  "SPARK_SUBMIT_BIN=$SPARK_SUBMIT_BIN" >> $PROJECT_PATH/.env
echo  "PROJECT_PATH=$PROJECT_PATH" >> $PROJECT_PATH/.env