#!/usr/bin/bash

PROJECT_DIR=$(dirname "$(pwd)")

sudo cp $PROJECT_DIR/supervisor/api.conf /etc/supervisor/conf.d/api.conf

sudo service supervisor start