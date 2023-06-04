#!/bin/zsh

CURR_TEST_DIR=$1 # path to testing file

pytest -s -v --disable-warnings  --color=yes --code-highlight=yes --full-trace --tb=native $CURR_TEST_DIR
