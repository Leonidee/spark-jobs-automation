#!/bin/zsh

CURR_TEST_DIR=$1 # path to testing file
PYTEST_TAG="actual"

pytest -s -v -m "$PYTEST_TAG" --disable-warnings  --color=yes --code-highlight=yes --full-trace --tb=native $CURR_TEST_DIR
