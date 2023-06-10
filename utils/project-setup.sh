read -p "Enter a project directory:  " PROJECT_DIR

if grep -q "^export PROJECT_DIR=$PROJECT_DIR" ~/.bashrc; then
    echo "The variable PROJECT_DIR is already defined in .bashrc"
else
    echo "Setting up variable PROJECT_DIR into .bashrc"
    echo "export PROJECT_DIR=$PROJECT_DIR" >> ~/.bashrc
    source ~/.bashrc
fi

if test -f "$PROJECT_DIR/supervisor/api.conf"; then
    rm "$PROJECT_DIR/supervisor/api.conf"
fi
touch "$PROJECT_DIR/supervisor/api.conf"

cat <<EOF > "$PROJECT_DIR/supervisor/api.conf"
[program:api] 
command=$PROJECT_DIR/api/run-api.sh
user=$(whoami)
autostart=true
autorestart=true
stderr_logfile=/var/log/uvicorn.err.log
stdout_logfile=/var/log/uvicorn.out.log
EOF

if test -f "$PROJECT_DIR/api/run-api.sh"; then
    rm "$PROJECT_DIR/api/run-api.sh"
fi
touch $PROJECT_DIR/api/run-api.sh
chmod +x $PROJECT_DIR/api/run-api.sh

cat <<EOF > "$PROJECT_DIR/api/run-api.sh"
#!$(which bash)
source $PROJECT_DIR/.venv/bin/activate
exec python $PROJECT_DIR/api/api.py
EOF


# Check if supervisor is already installed
if command -v supervisorctl > /dev/null; then
    echo "supervisor is already installed. Skipping installation"
else
    # Install supervisor
    if command -v apt-get > /dev/null; then
        # Debian-based system
        sudo apt-get update
        sudo apt-get install -y supervisor
    else
        echo "Unsupported operating system. Please manually install supervisor and re-run script"
        exit 1
    fi
fi


if test -f "/etc/supervisor/conf.d/api.conf"; then
    sudo rm -rf "/etc/supervisor/conf.d/api.conf"
fi

sudo cp $PROJECT_DIR/supervisor/api.conf /etc/supervisor/conf.d/api.conf

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
