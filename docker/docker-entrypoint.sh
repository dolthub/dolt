#!/bin/bash
set -eo pipefail

# mysql_log prints a timestamped (ISO 8601), color-coded structured log message.
# The message includes a log level and the message itself.
# If <MESSAGE> is omitted, it reads from stdin, allowing multi-line input.
#
# Arguments:
#   $1 - <LEVEL>   : Log level (e.g., Warn, Error, Debug)
#   $2 - <MESSAGE> : Message to log; if omitted, the function reads from stdin
#
# Usage:
#   mysql_log <LEVEL> [MESSAGE]
#   mysql_log Warn "Disk space low"
#   echo "Database connection lost" | mysql_log Error
#
# Output:
#   2025-10-16T12:34:56+00:00 [Warn] [Entrypoint] Disk space low
#   2025-10-16T12:35:01+00:00 [Error] [Entrypoint] Database connection lost
_mysql_log() {
  local level="$1"; shift

  local dt
  dt="$(date --rfc-3339=seconds)"

  local color_reset="\033[0m"
  local color=""
  case "$level" in
  Warn)  color="\033[1;33m" ;; # yellow
  Error) color="\033[1;31m" ;; # red
  Debug) color="\033[1;34m" ;; # blue
  esac

  local msg="$*"
  if [ "$#" -eq 0 ]; then
    msg="$(cat)"
  fi

  printf '%b%s [%s] [Entrypoint] %s%b\n' "$color" "$dt" "$level" "$msg" "$color_reset"
}


# _dbg logs a message of type 'Debug' using mysql_log.
_dbg() {
  _mysql_log Debug "$@"
}

# mysql_note logs a message of type 'Note' using mysql_log.
mysql_note() {
  _mysql_log Note "$@"
}

# mysql_warn logs a message of type 'Warning' using mysql_log and writes to stderr.
mysql_warn() {
  _mysql_log Warn "$@" >&2
}

# mysql_error logs a message of type 'ERROR' using mysql_log, writes to stderr, prints a container removal hint, and
# exits with status 1.
mysql_error() {
  _mysql_log Error "$@" >&2
  mysql_note "Remove this container with 'docker rm -f <container_name>' before retrying"
  exit 1
}

# exec_mysql executes a SQL query using Dolt, retrying until success or timeout. Ensures reliability during slow
# container or resource startup. On timeout, it prints the provided error prefix followed by filtered Dolt output.
# Errors are parsed to remove blank lines and extract only relevant error text. Use --show-result to display successful
# query results.
#
# Usage:
#   exec_mysql [--show-result] "<ERROR_MESSAGE>" "<QUERY>"
#   exec_mysql [--show-result] "<ERROR_MESSAGE>" < /docker-entrypoint-initdb.d/init.sql
#   cat /docker-entrypoint-initdb.d/init.sql | exec_mysql [--show-result] "<ERROR_MESSAGE>"
#
# Output:
#   Prints query output only if --show-result is specified.
exec_mysql() {
  local show_result=0
  if [ "$1" = "--show-result" ]; then
    show_result=1
    shift
  fi

  local error_message="$1"
  local query="${2:-}"
  local timeout="${DOLT_SERVER_TIMEOUT:-300}"
  local start_time now output status

  start_time=$(date +%s)

  while true; do
    if [ -n "$query" ]; then
      output=$(dolt sql -q "$query" 2>&1)
      status=$?
    else
      set +e # tmp disabled to initdb.d/ file err
      output=$(dolt sql < /dev/stdin 2>&1)
      status=$?
      set -e
    fi

    if [ "$status" -eq 0 ]; then
      [ "$show_result" -eq 1 ] && echo "$output" | grep -v "^$" || true
      return 0
    fi

    if echo "$output" | grep -qiE "Error [0-9]+ \([A-Z0-9]+\)"; then
      mysql_error "$error_message$(echo "$output" | grep -iE "Error|error")"
    fi

    if [ "$timeout" -ne 0 ]; then
      now=$(date +%s)
      if [ $((now - start_time)) -ge "$timeout" ]; then
        mysql_error "$error_message$(echo "$output" | grep -iE "Error|error" || true)"
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
SERVER_PID=-1

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
  local f
  echo
  for f; do
    case "$f" in
    *.sh)
      if [ -x "$f" ]; then
        mysql_note "$0: running $f"
        if ! "$f"; then
          mysql_error "Failed to execute $f: "
        fi
      else
        mysql_note "$0: sourcing $f"
        if ! . "$f"; then
          mysql_error "Failed to execute $f: "
        fi
      fi
      ;;
    *.sql)
      mysql_note "$0: running $f"
      exec_mysql --show-result "Failed to execute $f: " < "$f"
      ;;
    *.sql.bz2)
      mysql_note "$0: running $f"
      bunzip2 -c "$f" | exec_mysql --show-result "Failed to execute $f: "
      ;;
    *.sql.gz)
      mysql_note "$0: running $f"
      gunzip -c "$f" | exec_mysql --show-result "Failed to execute $f: "
      ;;
    *.sql.xz)
      mysql_note "$0: running $f"
      xzcat "$f" | exec_mysql --show-result "Failed to execute $f: "
      ;;
    *.sql.zst)
      mysql_note "$0: running $f"
      zstd -dc "$f" | exec_mysql --show-result "Failed to execute $f: "
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
    exec_mysql "Failed to create database '$database': " "CREATE DATABASE IF NOT EXISTS \`$database\`;"
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
    mysql_error "$(get_env_var_name "USER")="root", $(get_env_var_name "USER") and $(get_env_var_name "PASSWORD") are for configuring the regular user and cannot be used for the root user."
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

    mysql_note "Creating user '${user}@${user_host}'"
    exec_mysql "Failed to create user '$user': " "CREATE USER IF NOT EXISTS '$user'@'$user_host' IDENTIFIED BY '$password';"
    exec_mysql "Failed to grant server access to user '$user': " "GRANT USAGE ON *.* TO '$user'@'$user_host';"

    if [ -n "$database" ]; then
      exec_mysql "Failed to grant permissions to user '$user' on database '$database': " "GRANT ALL ON \`$database\`.* TO '$user'@'$user_host';"
    fi
  fi
}

# is_port_open checks if a TCP port is open on a given host.
# Arguments:
#   $1 - Host (IP or hostname)
#   $2 - Port number
# Returns:
#   0 if the port is open, non-zero otherwise.
is_port_open() {
  local host="$1"
  local port="$2"
  timeout 1 bash -c "cat < /dev/null > /dev/tcp/$host/$port" &>/dev/null
  return $?
}

# dolt_server_initializer starts the Dolt SQL server in the background and waits until it is ready to accept connections.
# It manages the server process, restarts it if necessary, and checks for readiness by probing the configured port.
# The function retries until the server is available or a timeout is reached, handling process management and logging.
# Arguments:
#   $@ - Additional arguments to pass to `dolt sql-server`
# Returns:
#   0 if the server starts successfully and is ready to accept connections; exits with error otherwise.
dolt_server_initializer() {
  local timeout="${DOLT_SERVER_TIMEOUT:-300}"
  local start_time
  start_time=$(date +%s)

  SERVER_PID=-1

  trap 'mysql_note "Caught Ctrl+C, shutting down Dolt server..."; [ $SERVER_PID -ne -1 ] && kill "$SERVER_PID"; exit 1' INT TERM

  while true; do
    if [ "$SERVER_PID" -eq -1 ] || ! kill -0 "$SERVER_PID" 2>/dev/null; then
      [ "$SERVER_PID" -ne -1 ] && wait "$SERVER_PID" 2>/dev/null || true
      SERVER_PID=-1
      dolt sql-server --host=0.0.0.0 --port=3306 "$@" 2>&1 &
      SERVER_PID=$!

    fi

    if is_port_open "0.0.0.0" 3306; then
      mysql_note "Dolt server started."
      return 0
    fi

    local now elapsed
    now=$(date +%s)
    elapsed=$((now - start_time))
    if [ "$elapsed" -ge "$timeout" ]; then
      kill "$SERVER_PID" 2>/dev/null || true
      wait "$SERVER_PID" 2>/dev/null || true
      SERVER_PID=-1
      mysql_error "Dolt server failed to start within $timeout seconds"
    fi

    sleep 1
  done
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
  # it will be used to start the server with --config flag.
  get_config_file_path_if_exists "$DOLT_CONFIG_DIR" "yaml"
  if [ -z "$CONFIG_PROVIDED" ]; then
    get_config_file_path_if_exists "$DOLT_CONFIG_DIR" "yml"
  fi
  if [ -z "$CONFIG_PROVIDED" ]; then
    get_config_file_path_if_exists "$SERVER_CONFIG_DIR" "yaml"
    if [ -z "$CONFIG_PROVIDED" ]; then
      get_config_file_path_if_exists "$SERVER_CONFIG_DIR" "yml"
    fi
  fi

  if [ -n "$CONFIG_PROVIDED" ]; then
    set -- "$@" --config="$CONFIG_PROVIDED"
  fi

  mysql_note "Starting Dolt server"
  # Attempt to configure the root user directly through the sql-server using built-in environment variable support
  # The user creation queries with `dolt sql` can interfere with this process so we run them after the server is started
  DOLT_ROOT_HOST="${DOLT_ROOT_HOST:-localhost}"

  # `dolt sql` can hold locks that prevent the server from starting during system hangs without this func
  dolt_server_initializer "$@"
  # Ran in a subshell to avoid exiting the main script, and so, we can use fallback below
  local has_correct_host
  has_correct_host=$(exec_mysql --show-result "Could not check root host: " \
    "SELECT User, Host FROM mysql.user WHERE User='root' AND Host='${DOLT_ROOT_HOST}' LIMIT 1;" | \
    grep -c "$DOLT_ROOT_HOST" || true)

  # args or system hangs may conflict with sql-server root env vars support
  if [ "$has_correct_host" -eq 0 ]; then
    mysql_warn "Environment variables failed to initialize 'root@${DOLT_ROOT_HOST}'; docker-entrypoint-initdb.d scripts queries may have conflicted. Overriding root user..."
    exec_mysql "Could not create root user: " "CREATE USER IF NOT EXISTS 'root'@'${DOLT_ROOT_HOST}' IDENTIFIED BY '${DOLT_ROOT_PASSWORD}';" # override password
    exec_mysql "Could not set root privileges: " "GRANT ALL PRIVILEGES ON *.* TO 'root'@'${DOLT_ROOT_HOST}' WITH GRANT OPTION;"
  fi

  create_database_from_env

  create_user_from_env

  exec_mysql --show-result "Could not list users: " "SELECT User, Host FROM mysql.user;"

  if [[ ! -f $INIT_COMPLETED ]]; then
    if ls /docker-entrypoint-initdb.d/* >/dev/null 2>&1; then
      docker_process_init_files /docker-entrypoint-initdb.d/*
    else
      mysql_warn "No files found in /docker-entrypoint-initdb.d/ to process"
    fi
    touch "$INIT_COMPLETED"
  fi

  mysql_note "Dolt init process done. Ready for connections."
  wait "$SERVER_PID"
}

_main "$@"
