#!/usr/bin/env bash
# 
# This is an entry point for configuring a new environment required to deploy a project.
# The project requires two main environments: a Dataproc cluster, host with Airflow
# and some kind of development or testing environment, which can be either of them.
# After running the script, you will be prompted to choose which environment you want to set up.
#
# Usage:
#   $ ./utils/setup-project.sh   dataproc      # for DataProc cluster configuration 
#   $ ./utils/setup-project.sh   airflow       # for configuration host where Airflow will be deployed
#   $ ./utils/setup-project.sh   dev           # for development and testing environment
# 

source ./utils/utils.sh

if [ -z "$1" ]; then
  echo "Usage: $0 [dataproc|airflow|dev]"
  exit 1
fi

case "$1" in
  "dataproc")
    echo "Setting up dataproc cluster environment"
    ;;
  "airflow")
    echo "Setting up airflow environment"
    ;;
  "dev")
    echo "Setting up development environment"
    ;;
  *)
    echo "Unknown mode: $1. Usage: $0 [dataproc|airflow|dev]"
    exit 1
    ;;
esac

ENVIRON=$1

if [[ "$ENVIRON" == "dataproc" ]]; then
  setup_dataproc
elif [[ "$ENVIRON" == "airflow" ]]; then
  setup_airflow
elif [[ "$ENVIRON" == "dev" ]]; then
  setup_dev
fi
