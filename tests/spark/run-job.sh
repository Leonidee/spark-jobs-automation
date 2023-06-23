#!/usr/bin/env bash
# this is a scrpit for testing spark-submit operation on cluster side
# in manual mode only, not for automate testing
# the only argument is name of job
# 'job' means one of the python files in 'jobs' folder

date="2022-04-26"
depth=10
src_path="s3a://data-ice-lake-05/master/data/source/messenger-yp/events"
tgt_path="s3a://data-ice-lake-05/master/tmp/messenger-yp"
coords_path="s3a://data-ice-lake-05/prod/dictionary/messenger-yp/cities-coordinates-dict"
processed_dttm="2023-05-22T12:03:25"

/usr/bin/spark-submit $1 $date $depth $src_path $tgt_path $coords_path $processed_dttm
