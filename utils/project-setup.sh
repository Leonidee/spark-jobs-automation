#!/usr/bin/bash

read -p "Enter a project directory:  " PROJECT_DIR

echo "export PROJECT_DIR=$PROJECT_DIR" >> ~/.bashrc

source ~/.bashrc

echo "command=$PROJECT_DIR/api/run-api.sh" >> $PROJECT_DIR/supervisor/api.conf
echo "user=$(whoami)" >> $PROJECT_DIR/supervisor/api.conf

echo "source $PROJECT_DIR/.venv/bin/activate" >> $PROJECT_DIR/api/run-api.sh
echo "exec python $PROJECT_DIR/api/api.py" >> $PROJECT_DIR/api/run-api.sh

sudo cp $PROJECT_DIR/supervisor/api.conf /etc/supervisor/conf.d/api.conf

sudo service supervisor start
