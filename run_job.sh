#!/usr/bin/bash

spark-submit jobs/tags_job.py "2022-03-28" "5" "100" "s3a://data-ice-lake-04/messager-data/snapshots/tags_verified/actual" "s3a://data-ice-lake-04/messager-data/analytics/cleaned-events" "s3a://data-ice-lake-04/messager-data/analytics/tag-candidates"