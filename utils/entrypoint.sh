#!/usr/bin/env bash
#
# Usage:
#   $ ./utils/entrypoint.sh dataproc      # for DataProc cluster configuration 
#   $ ./utils/entrypoint.sh airflow       # for configuration host where Airflow will be deployed
# 

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

if [[ "$ENVIRON" == "dataproc" ]]; then

# Setting up required environment variables
read -p "Enter a project path:  " PROJECT_PATH

if grep -q "^export PROJECT_PATH=$PROJECT_PATH" ~/.bashrc; then
    echo "The variable PROJECT_PATH is already defined in .bashrc"
else
    echo "Setting up variable PROJECT_PATH into .bashrc"
    echo "export PROJECT_PATH=$PROJECT_PATH" >> ~/.bashrc
fi

SPARK_SUBMIT_BIN=$(which spark-submit)
if grep -q "^export SPARK_SUBMIT_BIN=$SPARK_SUBMIT_BIN" ~/.bashrc; then
    echo "The variable SPARK_SUBMIT_BIN is already defined in .bashrc"
else
    echo "Setting up variable SPARK_SUBMIT_BIN into .bashrc"
    echo "export SPARK_SUBMIT_BIN=$SPARK_SUBMIT_BIN" >> ~/.bashrc
fi

source ~/.bashrc

# Setting these to .env also. 
# This is necessary for the proper operation of the supervisor service
# I usee this method to work around some of the errors of supervisor
if grep -q "^SPARK_SUBMIT_BIN=$SPARK_SUBMIT_BIN" $PROJECT_PATH/.env; then
  :
else
    echo "SPARK_SUBMIT_BIN=$SPARK_SUBMIT_BIN" >> $PROJECT_PATH/.env
fi
if grep -q "^PROJECT_PATH=$PROJECT_PATH" $PROJECT_PATH/.env; then
  :
else
    echo "PROJECT_PATH=$PROJECT_PATH" >> $PROJECT_PATH/.env
fi

PYSPARK_PYTHON=${PYSPARK_PYTHON:-$(which python)}

$PYSPARK_PYTHON -m pip install --upgrade pip

# Instaling packages
if command -v apt > /dev/null; then
  sudo apt update
  sudo apt upgrade -y
  if command -v supervisorctl > /dev/null; then
    echo "supervisor is already installed"
  else
    sudo apt install -y supervisor
  fi
elif command -v apt-get > /dev/null; then
  sudo apt-get update
  sudo apt-get upgrade -y
  if command -v supervisorctl > /dev/null; then
    echo "supervisor is already installed"
  else
    sudo apt-get install -y supervisor
  fi
else
  echo "Unsupported OS. Please, manually install supervisor"
  exit 1
fi

# Parse the dependencies from the pyproject.toml file and install it
while IFS='=' read -r key value; do
  if [[ $key == *"["* ]]; then
    section=$(echo $key | tr -d '[]')
  elif [[ $key != "" ]]; then
    if [[ $section == "tool.poetry.group.dataproc.dependencies" ]]; then
        package=$(echo $key | tr -d '[:space:]')
        version=$(echo $value | tr -d '[:space:]"')
      if [[ $package != "python" ]]; then
        if [[ $package == "uvicorn"* ]]; then # Huston, we here some problem here with parsing, thats why I done this
          package=$(echo $key | cut -d' ' -f1)
          extras=$(echo $value | sed 's/.*extras = \[\([^]]*\)\].*/\1/')
          version=$(echo $value | sed 's/.*version = "\(.*\)".*/\1/')
          $PYSPARK_PYTHON -m pip install $package==$version
        else
          $PYSPARK_PYTHON -m pip install $package==$version
        fi
      fi
    fi
  fi
done < $PROJECT_PATH/pyproject.toml

# Preparing configuration files
if test -f "$PROJECT_PATH/supervisor/api.conf"; then
  rm "$PROJECT_PATH/supervisor/api.conf"
fi
touch "$PROJECT_PATH/supervisor/api.conf"

cat <<EOF > "$PROJECT_PATH/supervisor/api.conf"
[program:api] 
command=$PROJECT_PATH/api/run-api.sh
user=$(whoami)
autostart=true
autorestart=true
stderr_logfile=/var/log/uvicorn.err.log
stdout_logfile=/var/log/uvicorn.out.log
EOF

if test -f "$PROJECT_PATH/api/run-api.sh"; then
    rm "$PROJECT_PATH/api/run-api.sh"
fi
touch $PROJECT_PATH/api/run-api.sh
chmod +x $PROJECT_PATH/api/run-api.sh

cat <<EOF > "$PROJECT_PATH/api/run-api.sh"
#!/usr/bin/env bash
exec $PYSPARK_PYTHON $PROJECT_PATH/api/api.py
EOF

if test -f "/etc/supervisor/conf.d/api.conf"; then
    sudo rm -rf "/etc/supervisor/conf.d/api.conf"
fi

# Coping supervisor config into path where supervisor will search for it
sudo cp $PROJECT_PATH/supervisor/api.conf /etc/supervisor/conf.d/api.conf

# Starting or restarting supervisor 
if ps -ef | grep -v grep | grep supervisord > /dev/null; then
    echo "Restarting supervisor service"
    sudo service supervisor restart
    if ps -ef | grep -v grep | grep supervisord > /dev/null; then
        echo "Success"
    fi

else
    echo "Starting supervisor service"
    sudo service supervisor start
    if ps -ef | grep -v grep | grep supervisord > /dev/null; then
        echo "Success"
    fi
fi


elif [[ "$ENVIRON" == "airflow" ]]; then
echo "some logic here"
fi
