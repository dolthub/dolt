#!/bin/bash
set -eo pipefail

# mysql_log logs messages with a timestamp and optional color formatting.
# Arguments:
#   $1 - Log type (e.g., Note, Warn, ERROR)
#   $@ - Log message (if empty, reads from stdin)
# Output:
#   Prints a formatted log line to stdout or stderr, with color for Warn and ERROR.
mysql_log() {
  local type="$1"
  shift
  local text="$*"
  if [ "$#" -eq 0 ]; then text="$(cat)"; fi
  local dt
  dt="$(date --rfc-3339=seconds)"
  local color_reset="\033[0m"
  local color
  case "$type" in
  Warn) color="\033[1;33m" ;;  # yellow
  ERROR) color="\033[1;31m" ;; # red
  *) color="" ;;
  esac
  printf '%b%s [%s] [Entrypoint]: %s%b\n' "$color" "$dt" "$type" "$text" "$color_reset"
}

# mysql_note logs a message of type 'Note' using mysql_log.
mysql_note() {
  mysql_log Note "$@"
}

# mysql_warn logs a message of type 'Warn' using mysql_log and writes to stderr.
mysql_warn() {
  mysql_log Warn "$@" >&2
}

# mysql_error logs a message of type 'ERROR' using mysql_log, write to stderr, prints a container removal hint, and
# exits with status 1.
mysql_error() {
  mysql_log ERROR "$@" >&2
  mysql_note "Remove this container with 'docker rm -f <container_name>' before retrying"
  exit 1
}

# exec_mysql executes a SQL query using Dolt, retrying until it succeeds or a timeout is reached.
# On timeout, it prints the provided error message prefix followed by the command output.
# Containers and system resources can hang during startup, so this function helps ensure
# that the SQL command is executed successfully before proceeding.
# Arguments:
#   $1 - SQL command to execute
#   $2 - Error message prefix to print on timeout
#   $3 - (Optional) If set to 1, prints command output on success
exec_mysql() {
  local sql_command="$1"
  local error_message="$2"
  local show_output="${3:-0}"
  local timeout="${DOLT_SERVER_TIMEOUT:-300}"
  local start_time now output

  start_time=$(date +%s)

  while true; do
    if output=$(dolt sql -q "$sql_command" 2>&1); then
      [ "$show_output" -eq 1 ] && echo "$output"
      return 0
    fi

    if echo "$output" | grep -qi "syntax error"; then
      mysql_error "$error_message$output"
    fi

    if [ "$timeout" -ne 0 ]; then
      now=$(date +%s)
      if [ $((now - start_time)) -ge "$timeout" ]; then
        mysql_error "$error_message$output"
      fi
    fi

    sleep 1
  done
}

CONTAINER_DATA_DIR="/var/lib/dolt"
INIT_COMPLETED="$CONTAINER_DATA_DIR/.init_completed"
DOLT_CONFIG_DIR="/etc/dolt/doltcfg.d"
SERVER_CONFIG_DIR="/etc/dolt/servercfg.d"
DOLT_ROOT_PATH="/.dolt"

# check_for_dolt_binary verifies that the dolt binary is present and executable in the system PATH.
# If not found or not executable, it logs an error and exits.
check_for_dolt_binary() {
  local dolt_bin
  dolt_bin=$(which dolt)
  if [ ! -x "$dolt_bin" ]; then
    mysql_error "dolt binary executable not found"
  fi
}

# get_env_var returns the value of an environment variable, preferring DOLT_* over MYSQL_*.
# Arguments:
#   $1 - The base variable name (e.g., "USER" for MYSQL_USER or DOLT_USER)
# Output:
#   Prints the value of the first set variable, or an empty string if neither is set.
get_env_var() {
  local var_name="$1"
  local dolt_var="DOLT_${var_name}"
  local mysql_var="MYSQL_${var_name}"

  if [ -n "${!dolt_var}" ]; then
    echo "${!dolt_var}"
  elif [ -n "${!mysql_var}" ]; then
    echo "${!mysql_var}"
  else
    echo ""
  fi
}

# get_env_var_name returns the name of the environment variable that is set, preferring DOLT_* over MYSQL_*.
# Arguments:
#   $1 - The base variable name (e.g., "USER" for MYSQL_USER or DOLT_USER)
# Output:
#   Prints the name of the first set variable, or both names if neither is set.
get_env_var_name() {
  local var_name="$1"
  local dolt_var="DOLT_${var_name}"
  local mysql_var="MYSQL_${var_name}"

  if [ -n "${!dolt_var}" ]; then
    echo "DOLT_${var_name}"
  elif [ -n "${!mysql_var}" ]; then
    echo "MYSQL_${var_name}"
  else
    echo "MYSQL_${var_name}/DOLT_${var_name}"
  fi
}

# get_config_file_path_if_exists checks for config files of a given type in a directory.
# Arguments:
#   $1 - Directory to search in
#   $2 - File type/extension to search for (e.g., 'json', 'yaml')
# Output:
#   Sets CONFIG_PROVIDED to the path of the config file if exactly one is found, or empty otherwise.
#   Logs a warning if multiple config files are found and uses the default config.
get_config_file_path_if_exists() {
  CONFIG_PROVIDED=
  local CONFIG_DIR=$1
  local FILE_TYPE=$2
  if [ -d "$CONFIG_DIR" ]; then
    mysql_note "Checking for config provided in $CONFIG_DIR"
    local number_of_files_found
    number_of_files_found=$(find "$CONFIG_DIR" -type f -name "*.$FILE_TYPE" | wc -l)
    if [ "$number_of_files_found" -gt 1 ]; then
      CONFIG_PROVIDED=
      mysql_warn "Multiple config files found in $CONFIG_DIR, using default config"
    elif [ "$number_of_files_found" -eq 1 ]; then
      local files_found
      files_found=$(ls "$CONFIG_DIR"/*."$FILE_TYPE")
      mysql_note "$files_found file is found"
      CONFIG_PROVIDED=$files_found
    else
      CONFIG_PROVIDED=
    fi
  fi
}

# docker_process_init_files Runs files found in /docker-entrypoint-initdb.d before the server is started.
# Taken from https://github.com/docker-library/mysql/blob/master/8.0/docker-entrypoint.sh
# Usage:
#   docker_process_init_files [file [file ...]]
#   e.g., docker_process_init_files /always-initdb.d/*
# Processes initializer files based on file extensions.
docker_process_init_files() {
  mysql_note "Running init script...s"
  local f sql

  for f; do
    case "$f" in
    *.sh)
      if [ -x "$f" ]; then
        mysql_note "$0: running $f"
        if ! "$f"; then
          mysql_error "Failed to execute init script '$f'"
        fi
      else
        mysql_note "$0: sourcing $f"
        if ! . "$f"; then
          mysql_error "Failed to source init script '$f'"
        fi
      fi
      ;;
    *.sql)
      mysql_note "$0: running $f"
      sql=$(cat "$f")
      exec_mysql "$sql" "Failed to execute $f: "
      ;;
    *.sql.bz2)
      mysql_note "$0: running $f"
      sql=$(bunzip2 -c "$f")
      exec_mysql "$sql" "Failed to execute $f: "
      ;;
    *.sql.gz)
      mysql_note "$0: running $f"
      sql=$(gunzip -c "$f")
      exec_mysql "$sql" "Failed to execute $f: "
      ;;
    *.sql.xz)
      mysql_note "$0: running $f"
      sql=$(xzcat "$f")
      exec_mysql "$sql" "Failed to execute $f: "
      ;;
    *.sql.zst)
      mysql_note "$0: running $f"
      sql=$(zstd -dc "$f")
      exec_mysql "$sql" "Failed to execute $f: "
      ;;
    *)
      mysql_warn "$0: ignoring $f"
      ;;
    esac
    echo
  done
}

# set_dolt_config_if_defined checks for a user-provided Dolt config file in $DOLT_CONFIG_DIR.
# If a single JSON config file is found, it copies it to $HOME/$DOLT_ROOT_PATH/config_global.json,
# overwriting the default config. Logs an error and exits if the copy fails.
set_dolt_config_if_defined() {
  get_config_file_path_if_exists "$DOLT_CONFIG_DIR" "json"
  if [ ! -z "$CONFIG_PROVIDED" ]; then
    if ! /bin/cp -rf "$CONFIG_PROVIDED" "$HOME/$DOLT_ROOT_PATH/config_global.json" 2>&1; then
      mysql_error "Failed to copy config file from '$CONFIG_PROVIDED' to '$HOME/$DOLT_ROOT_PATH/config_global.json'. Check file permissions and paths."
    fi
  fi
}

# create_database_from_env creates a database if the DATABASE environment variable is set.
# It retrieves the database name from environment variables (preferring DOLT_DATABASE over MYSQL_DATABASE)
# and attempts to create the database using exec_mysql.
create_database_from_env() {
  local database
  database=$(get_env_var "DATABASE")

  if [ -n "$database" ]; then
    mysql_note "Creating database '${database}'"
    exec_mysql "CREATE DATABASE IF NOT EXISTS \`$database\`;" "Failed to create database '$database': "
  fi
}

# create_user_from_env creates a new database user from environment variables.
# It prefers DOLT_USER/PASSWORD over MYSQL_USER/PASSWORD, and optionally grants access to a database.
# Requires both USER and PASSWORD to be set; if only the password is set, it logs a warning and does nothing.
# It does not allow creating a 'root' user via these environment variables.
create_user_from_env() {
  local user
  local password
  local database

  user=$(get_env_var "USER")
  password=$(get_env_var "PASSWORD")
  database=$(get_env_var "DATABASE")

  if [ "$user" = 'root' ]; then
    mysql_error <<-EOF
    $(get_env_var_name "USER")="root", $(get_env_var_name "USER") and $(get_env_var_name "PASSWORD") are for configuring the regular user and cannot be used for the root user.
EOF
  fi

  if [ -n "$user" ] && [ -z "$password" ]; then
    mysql_error "$(get_env_var_name "USER") specified, but missing $(get_env_var_name "PASSWORD"); user creation requires a password."
  elif [ -z "$user" ] && [ -n "$password" ]; then
    mysql_warn "$(get_env_var_name "PASSWORD") specified, but missing $(get_env_var_name "USER"); password will be ignored"
    return
  fi

  if [ -n "$user" ]; then
    local user_host
    user_host=$(get_env_var "USER_HOST")
    user_host="${user_host:-${DOLT_ROOT_HOST:-localhost}}"

    mysql_note "Creating user '${user}@${user_host}'..."
    exec_mysql "CREATE USER IF NOT EXISTS '$user'@'$user_host' IDENTIFIED BY '$password';" "Failed to create user '$user': "
    exec_mysql "GRANT USAGE ON *.* TO '$user'@'$user_host';" "Failed to grant server access to user '$user': "

    if [ -n "$database" ]; then
      mysql_note "Giving user '${user}@${user_host}' access to schema '${database}'..."
      exec_mysql "GRANT ALL ON \`$database\`.* TO '$user'@'$user_host';" "Failed to grant permissions to user '$user' on database '$database': "
    fi

    mysql_note "'${user}@${user_host}' user successfully created!"
  fi
}

# _main is the main entrypoint for the Dolt Docker container initialization.
_main() {
  check_for_dolt_binary

  local dolt_version
  dolt_version=$(dolt version | grep 'dolt version' | cut -f3 -d " ")
  mysql_note "Entrypoint script for Dolt Server $dolt_version starting..."

  declare -g CONFIG_PROVIDED

  # dolt config will be set if user provided a single json file in /etc/dolt/doltcfg.d directory.
  # It will overwrite config_global.json file in $HOME/.dolt
  set_dolt_config_if_defined

  # if there is a single yaml provided in /etc/dolt/servercfg.d directory,
  # it will be used to start the server with --config flag
  get_config_file_path_if_exists "$SERVER_CONFIG_DIR" "yaml"
  if [ ! -z "$CONFIG_PROVIDED" ]; then
    set -- "$@" --config="$CONFIG_PROVIDED"
  fi

  if [[ ! -f $INIT_COMPLETED ]]; then
    if ls /docker-entrypoint-initdb.d/* >/dev/null 2>&1; then
      docker_process_init_files /docker-entrypoint-initdb.d/*
    else
      mysql_warn "No files found in /docker-entrypoint-initdb.d/ to process"
    fi
    touch "$INIT_COMPLETED"
  fi

  create_database_from_env

  mysql_note "Starting Dolt server..."
  # Configure the root user first since the environment var initialization breaks
  # if certain queries are ran before the root user is ready.
  DOLT_ROOT_HOST="${DOLT_ROOT_HOST:-localhost}"
  mysql_note "Configuring user 'root@${DOLT_ROOT_HOST}'..."
  DOLT_SERVER_TIMEOUT="${DOLT_SERVER_TIMEOUT:-300}"
  local START_TIME
  START_TIME=$(date +%s)
  local SERVER_PID

  while true; do
    dolt sql-server --host=0.0.0.0 --port=3306 "$@" &
    SERVER_PID=$!

    sleep 2

    if kill -0 "$SERVER_PID" 2>/dev/null; then
      break
    else
      wait "$SERVER_PID" 2>/dev/null || true
      local NOW
      NOW=$(date +%s)
      local ELAPSED
      ELAPSED=$((NOW - START_TIME))

      if [ "$ELAPSED" -ge "$DOLT_SERVER_TIMEOUT" ]; then
        mysql_error "Dolt server failed to start within $DOLT_SERVER_TIMEOUT seconds"
      fi

      mysql_warn "Dolt server failed to start, retrying..."
    fi
  done

  exec_mysql "SELECT 1 FROM mysql.user WHERE User='root' LIMIT 1;" "The root user did not initialize: "

  # Ran in a subshell to avoid exiting the main script, capture output and use fallback if query fails
  local has_correct_host
  has_correct_host=$(exec_mysql \
    "SELECT User, Host FROM mysql.user WHERE User='root' AND Host='${DOLT_ROOT_HOST}' LIMIT 1;" \
    "Could not check root host: " 1 | grep -c "$DOLT_ROOT_HOST" || true)

  # docker-entrypoint-initdb.d scripts may conflict with environment variable initialization if they create users
  if [ "$has_correct_host" -eq 0 ]; then
    mysql_warn "Environment variables failed to initialize 'root@${DOLT_ROOT_HOST}'; docker-entrypoint-initdb.d scripts queries may have conflicted. Overriding root user..."
    exec_mysql "CREATE USER IF NOT EXISTS 'root'@'${DOLT_ROOT_HOST}' IDENTIFIED BY '${DOLT_ROOT_PASSWORD}';" "Could not create root user: " # override password
    exec_mysql "GRANT ALL PRIVILEGES ON *.* TO 'root'@'${DOLT_ROOT_HOST}' WITH GRANT OPTION;" "Could not set root privileges: "
  fi
  mysql_note "'root@${DOLT_ROOT_HOST}' user successfully configured!"

  create_user_from_env

  exec_mysql "SELECT User, Host FROM mysql.user;" "Could not list users: " 1

  mysql_note "Server initialization complete!"

  wait "$SERVER_PID"
}

_main "$@"
