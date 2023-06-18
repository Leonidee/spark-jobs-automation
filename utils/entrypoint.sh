#!/bin/bash
#
# Usage:
#   $ ./utils/entrypoint.sh dataproc
#   $ ./utils/entrypoint.sh airflow

if [ -z "$1" ]; then
  echo "Usage: $0 [dataproc|airflow]"
  exit 1
fi

case "$1" in
  "dataproc")
    echo "Setting up dataproc cluster environment"
    ;;
  "airflow")
    echo "Setting up airflow environment"
    ;;
  *)
    echo "Unknown mode: $1. Usage: $0 [dataproc|airflow]"
    exit 1
    ;;
esac

ENVIRON=$1

if $ENVIRON = "dataproc"; echo "." # TODO make logic branch here
fi

# Instaling packages
sudo apt-get update
sudo apt-get upgrade -y

PYTHON_PATH=$(which python)


# Check if supervisor is already installed
if command -v poetry > /dev/null; then
    echo "poetry is already installed"
else
    # Install poetry
    if command -v apt-get > /dev/null; then
        # Debian-based system
        curl -sSL https://install.python-poetry.org | $PYTHON_PATH -
    else
        echo "Unsupported operating system. Please manually install poetry and re-run script"
        exit 1
    fi
fi

echo "export PATH=$HOME/.poetry/bin:$PATH" >> ~/.bashrc
source ~/.bashrc

