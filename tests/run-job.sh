#!/usr/bin/bash
# This is a scrpit for testing spark-submit operation on cluster side.
# The only argument is name of job.
# "job" means one of the python files in `jobs` folder

date="2022-04-26"
depth=62
src_path="s3a://data-ice-lake-05/messager-data/analytics/geo-events"
tgt_path="s3a://data-ice-lake-05/messager-data/analytics/tmp/location_zone_agg_datamart"
processed_dttm="2023-05-22T12:03:25"

spark-submit $1 $date $depth $src_path $tgt_path $processed_dttm 
