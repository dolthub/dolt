#!/bin/bash

# This script installs starts a dolt server on your Unix compatible computer.

if test -z "$BASH_VERSION"; then
  echo "Please run this script using bash, not sh or any other shell. It should be run as root." >&2
  exit 1
fi

_() {

install_dolt() {
  # Install Dolt if it already doesn't exist
  echo "Installing Dolt..."

  if ! command -v dolt &> /dev/null
  then
    sudo bash -c 'curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh | bash'
  fi
}

setup_configs() {
   # Set up the dolt user along with core dolt configurations
  echo "Setting up Configurations..."

  # Check if the user "dolt" already exists. If it exists double check that it is okay to continue
  if id -u "dolt" &> /dev/null; then
    echo "The user dolt already exists"
    read -r -p "Do you want to continue adding privileges to the existing user dolt? " response

    response=${response,,} # tolower
    if ! ([[ $response =~ ^(yes|y| ) ]] || [[ -z $response ]]); then
      exit 1
    fi

  else
    # add the user if `dolt` doesn't exist
    useradd -r -m -d /var/lib/doltdb dolt
  fi

  cd /var/lib/doltdb

  read -e -p "Enter an email associated with your user: " -i "dolt-user@dolt.com" email
  read -e -p "Enter a username associated with your user: " -i "Dolt Server Account" username

  sudo -u dolt dolt config --global --add user.email $email
  sudo -u dolt dolt config --global --add user.name $username
}

# Database creation
database_configuration() {
  echo "Setting up the dolt database..."

  read -e -p "Input the name of your database: " -i "mydb" db_name
  local db_dir="databases/$db_name"

  cd /var/lib/doltdb
  sudo -u dolt mkdir -p $db_dir

  cd $db_dir
  sudo -u dolt dolt init
}

# Setup and Start daemon
start_server() {
  echo "Starting the server"

  cd ~
  cat > dolt_config.yaml<<EOF
log_level: info
behavior:
  read_only: false
  autocommit: true
user:
  name: root
  password: ""
listener:
  host: localhost
  port: 3306
  max_connections: 100
  read_timeout_millis: 28800000
  write_timeout_millis: 28800000
  tls_key: null
  tls_cert: null
  require_secure_transport: null
databases: []
performance:
  query_parallelism: null
EOF

  cat > doltdb.service<<EOF
[Unit]
Description=dolt SQL server
After=network.target
[Install]
WantedBy=multi-user.target
[Service]
User=dolt
Group=dolt
ExecStart=/usr/local/bin/dolt sql-server --config=dolt_config.yaml
WorkingDirectory=/var/lib/doltdb/databases/$db_name
KillSignal=SIGTERM
SendSIGKILL=no
EOF

  sudo chown root:root doltdb.service
  sudo chmod 644 doltdb.service
  sudo mv doltdb.service /etc/systemd/system
  sudo cp dolt_config.yaml /var/lib/doltdb/databases/$db_name

  sudo systemctl daemon-reload
  sudo systemctl enable doltdb.service
  sudo systemctl start doltdb
}

validate_status() {
  if systemctl --state=active | grep "doltdb.service"; then
    echo "Sever successfully started..."
  else
    echo "ERROR: Server did not start properly..."
  fi
}

install_dolt
setup_configs
database_configuration
start_server
validate_status
}


_ "$0" "$@"