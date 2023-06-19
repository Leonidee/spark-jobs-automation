#!/usr/bin/env bash
#
# Usage:
#   $ ./utils/entrypoint.sh dataproc      # for DataProc cluster configuration 
#   $ ./utils/entrypoint.sh airflow       # for configuration host where Airflow will be deployed
#   $ ./utils/entrypoint.sh dev           #
# 

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

# Instaling supervisor
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

# Parse the Cluster dependencies from the pyproject.toml file and install it
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


if grep -q "^PROJECT_PATH=/opt/airflow" ./.env; then
  :
elif grep -q "^PROJECT_PATH=.*" ./.env; then
  sed -i "s|^PROJECT_PATH=.*|PROJECT_PATH=/opt/airflow|" ./.env
else
  echo -e "PROJECT_PATH=/opt/airflow\n" >> ./.env
fi

if grep -q "^AIRFLOW_UID=.*" ./.env; then
  :
else
  echo -e "AIRFLOW_UID=$(id -u)\n" >> ./.env
fi

if grep -q "^AIRFLOW_GID=.*" ./.env; then
  :
else
  echo -e "AIRFLOW_GID=0\n" >> ./.env
fi

if [ -d "./logs" ]; then
  :
else
  mkdir "./logs"
fi
if [ -d "./plugins" ]; then
  :
else
  mkdir "./plugins"
fi

if test -f "requirements.txt"; then
    rm "requirements.txt"
fi
touch requirements.txt

# Parse Airflow dependencies from pyproject.toml
# and store it in requirements.txt that will be used
# to build Airlfow Docker containers
while IFS='=' read -r key value; do
  if [[ $key == *"["* ]]; then
    section=$(echo $key | tr -d '[]')
  elif [[ $key != "" ]]; then
    if [[ $section == "tool.poetry.group.airflow.dependencies" ]]; then
        package=$(echo $key | tr -d '[:space:]')
        version=$(echo $value | tr -d '[:space:]"')
      if [[ $package != "python" ]]; then
          echo $package==$version >> requirements.txt
      fi
    fi
  fi
done < pyproject.toml

if test -f "Dockerfile"; then
    rm "Dockerfile"
fi
touch Dockerfile

# Creating Dockerfile
cat <<EOF > "Dockerfile"
FROM apache/airflow:2.6.2-python3.11

ENV AIRFLOW_HOME=/opt/airflow

COPY ./requirements.txt .

RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir -r ./requirements.txt

USER airflow
WORKDIR ${AIRFLOW_HOME}
EOF

# Instaling docker
if command -v apt > /dev/null; then
  sudo apt update
  sudo apt upgrade -y
  if command -v docker > /dev/null; then 
    echo "docker is already installed"
  else
    echo "Installing docker"
    sudo apt install -y ca-certificates curl gnupg
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
      sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    
    sudo apt update
    sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  fi
elif command -v apt-get > /dev/null; then
  sudo apt-get update
  sudo apt-get upgrade -y
  if command -v docker > /dev/null; then
    echo "docker is already installed"
  else
    echo "Installing docker"
    sudo apt-get install -y ca-certificates curl gnupg
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
      sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    
    sudo apt-get update
    sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  fi
else
  echo "Unsupported OS. Please, manually install docker"
  exit 1
fi

# Up docker containers
docker compose build

docker compose up airflow-init

docker compose up -d

rm "requirements.txt"

elif [[ "$ENVIRON" == "dev" ]]; then

read -p "Enter a project path:  " PROJECT_PATH

if grep -q "^PROJECT_PATH=.*" ./.env; then
  sed -i "s|^PROJECT_PATH=.*|PROJECT_PATH=$PROJECT_PATH|" ./.env
else
  echo "PROJECT_PATH=$PROJECT_PATH\n" >> ./.env
fi

fi
