#!/bin/bash
set -eo pipefail

# logging functions
mysql_log() {
	local type="$1"; shift
	# accept argument string or stdin
	local text="$*"; if [ "$#" -eq 0 ]; then text="$(cat)"; fi
	local dt; dt="$(date --rfc-3339=seconds)"
	printf '%s [%s] [Entrypoint]: %s\n' "$dt" "$type" "$text"
}
mysql_note() {
	mysql_log Note "$@"
}
mysql_warn() {
	mysql_log Warn "$@" >&2
}
mysql_error() {
	mysql_log ERROR "$@" >&2
	exit 1
}
docker_process_sql() {
  dolt sql
}

CONTAINER_DATA_DIR="/var/lib/dolt"
INIT_COMPLETED="$CONTAINER_DATA_DIR/.init_completed"
DOLT_CONFIG_DIR="/etc/dolt/doltcfg.d"
SERVER_CONFIG_DIR="/etc/dolt/servercfg.d"
DOLT_ROOT_PATH="/.dolt"

check_for_dolt() {
    local dolt_bin=$(which dolt)
    if [ ! -x "$dolt_bin" ]; then
        mysql_error "dolt binary executable not found"
    fi
}

get_env_var() {
    local var_name="$1"
    local mysql_var="MYSQL_${var_name}"
    local dolt_var="DOLT_${var_name}"

    if [ -n "${!mysql_var}" ]; then
        echo "${!mysql_var}"
    elif [ -n "${!dolt_var}" ]; then
        echo "${!dolt_var}"
    else
        echo ""
    fi
}

get_env_var_name() {
    local var_name="$1"
    local mysql_var="MYSQL_${var_name}"
    local dolt_var="DOLT_${var_name}"

    if [ -n "${!mysql_var}" ]; then
        echo "MYSQL_${var_name}"
    elif [ -n "${!dolt_var}" ]; then
        echo "DOLT_${var_name}"
    else
        echo "MYSQL_${var_name}/DOLT_${var_name}"
    fi
}

# arg $1 is the directory to search in
# arg $2 is the type file to search for
get_config_file_path_if_exists() {
    CONFIG_PROVIDED=
    local CONFIG_DIR=$1
    local FILE_TYPE=$2
    if [ -d "$CONFIG_DIR" ]; then
        mysql_note "Checking for config provided in $CONFIG_DIR"
        local number_of_files_found=$(find "$CONFIG_DIR" -type f -name "*.$FILE_TYPE" | wc -l)
        if [ "$number_of_files_found" -gt 1 ]; then
            CONFIG_PROVIDED=
            mysql_warn "multiple config file found in $CONFIG_DIR, using default config"
        elif [ "$number_of_files_found" -eq 1 ]; then
            local files_found=$(ls "$CONFIG_DIR"/*"$FILE_TYPE")
            mysql_note "$files_found file is found"
            CONFIG_PROVIDED=$files_found
        else
            CONFIG_PROVIDED=
        fi
    fi
}

# taken from https://github.com/docker-library/mysql/blob/master/8.0/docker-entrypoint.sh
# this function will run files found in /docker-entrypoint-initdb.d directory BEFORE server is started
# usage: docker_process_init_files [file [file [...]]]
#    ie: docker_process_init_files /always-initdb.d/*
# process initializer files, based on file extensions
docker_process_init_files() {
	mysql_note "Running init scripts"
	local f
	for f; do
		case "$f" in
			*.sh)
				# https://github.com/docker-library/postgres/issues/450#issuecomment-393167936
				# https://github.com/docker-library/postgres/pull/452
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
			*.sql)     mysql_note "$0: running $f"; docker_process_sql < "$f"; echo ;;
			*.sql.bz2) mysql_note "$0: running $f"; bunzip2 -c "$f" | docker_process_sql; echo ;;
			*.sql.gz)  mysql_note "$0: running $f"; gunzip -c "$f" | docker_process_sql; echo ;;
			*.sql.xz)  mysql_note "$0: running $f"; xzcat "$f" | docker_process_sql; echo ;;
			*.sql.zst) mysql_note "$0: running $f"; zstd -dc "$f" | docker_process_sql; echo ;;
			*)         mysql_warn "$0: ignoring $f" ;;
		esac
		echo
	done
}

# if there is config file provided through /etc/dolt/doltcfg.d,
# we overwrite $HOME/.dolt/config_global.json file with this file.
set_dolt_config_if_defined() {
    get_config_file_path_if_exists "$DOLT_CONFIG_DIR" "json"
    if [ ! -z "$CONFIG_PROVIDED" ]; then
        if ! /bin/cp -rf "$CONFIG_PROVIDED" "$HOME/$DOLT_ROOT_PATH/config_global.json" 2>&1; then
            mysql_error "Failed to copy config file from '$CONFIG_PROVIDED' to '$HOME/$DOLT_ROOT_PATH/config_global.json'. Check file permissions and paths."
        fi
    fi
}

create_default_database_from_env() {
    local user
    local password
    local database

    database=$(get_env_var "DATABASE")
    user=$(get_env_var "USER")
    password=$(get_env_var "PASSWORD")

    if [ -n "$database" ]; then
        mysql_note "Creating database '${database}'"
        local db_output
        if ! db_output=$(dolt sql -q "CREATE DATABASE IF NOT EXISTS \`$database\`;" 2>&1); then
            mysql_error "Failed to create database '$database'. Error: $db_output"
        fi
    fi

    if [ "$user" = 'root' ]; then
        # TODO: add ALLOW_EMPTY_PASSWORD and RANDOM_ROOT_PASSWORD support
mysql_error <<-EOF
    $(get_env_var_name "USER")="root", $(get_env_var_name "USER") and $(get_env_var_name "PASSWORD") are for configuring a regular user and cannot be used for the root user
        Remove $(get_env_var_name "USER")="root" and use the following to control the root user password:
        - DOLT_ROOT_PASSWORD
EOF
    fi

    if [ -n "$user" ] && [ -z "$password" ]; then
        mysql_error "$(get_env_var_name "USER") specified, but missing $(get_env_var_name "PASSWORD"); user creation requires a password"
    elif [ -z "$user" ] && [ -n "$password" ]; then
        mysql_warn "$(get_env_var_name "PASSWORD") specified, but missing $(get_env_var_name "USER"); password will be ignored"
        return
    fi

    if [ -n "$user" ]; then
        # Use the same host as the root user for consistency
        local user_host="${DOLT_ROOT_HOST:-localhost}"
        mysql_note "Creating user '${user}'"
        if ! dolt sql -q "CREATE USER IF NOT EXISTS '$user'@'$user_host' IDENTIFIED BY '$password';" >/dev/null 2>&1; then
            mysql_error "Failed to create user '$user'. Check if user already exists or if there are permission issues."
        fi

        # Grant basic server access
        mysql_note "Granting server access to user '${user}'"
        if ! dolt sql -q "GRANT USAGE ON *.* TO '$user'@'$user_host';" >/dev/null 2>&1; then
            mysql_error "Failed to grant server access to user '$user'"
        fi

        if [ -n "$database" ]; then
            mysql_note "Giving user '${user}' access to schema '${database}'"
            if ! dolt sql -q "GRANT ALL ON \`$database\`.* TO '$user'@'$user_host';" >/dev/null 2>&1; then
                mysql_error "Failed to grant permissions to user '$user' on database '$database'"
            fi
        fi
    fi
}

_main() {
    # check for dolt binary executable
    check_for_dolt

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

    # TODO: add support for MYSQL_ROOT_HOST and MYSQL_ROOT_PASSWORD
    # Note: User creation will happen after server starts to avoid conflicts with default users

    if [[ ! -f $INIT_COMPLETED ]]; then
        # run any file provided in /docker-entrypoint-initdb.d directory before the server starts
        if ls /docker-entrypoint-initdb.d/* >/dev/null 2>&1; then
            docker_process_init_files /docker-entrypoint-initdb.d/*
        fi
        touch $INIT_COMPLETED
    fi

    # Start server in background for user setup
    mysql_note "Starting Dolt server in background..."
    dolt sql-server --host=0.0.0.0 --port=3306 "$@" > /tmp/server.log 2>&1 &
    local server_pid=$!
    
    # Wait for server to be ready
    local max_attempts=30
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if dolt sql -q "SELECT 1;" >/dev/null 2>&1; then
            mysql_note "Server initialization complete!"
            break
        fi
        sleep 1
        attempt=$((attempt + 1))
    done
    
    if [ $attempt -eq $max_attempts ]; then
        mysql_error "Server failed to start within 30 seconds"
    fi
    
    # Create root user with the specified host (defaults to localhost if not specified)
    local root_host="${DOLT_ROOT_HOST:-localhost}"
    mysql_note "Ensuring root@${root_host} superuser exists with password"
    
    # Ensure root user exists with correct password and permissions
    mysql_note "Configuring root@${root_host} user"
    dolt sql -q "CREATE USER IF NOT EXISTS 'root'@'${root_host}' IDENTIFIED BY '${DOLT_ROOT_PASSWORD}'; ALTER USER 'root'@'${root_host}' IDENTIFIED BY '${DOLT_ROOT_PASSWORD}'; GRANT ALL ON *.* TO 'root'@'${root_host}' WITH GRANT OPTION;" >/dev/null 2>&1 || mysql_error "Failed to configure root@${root_host} user. Check logs for details."
    
    # If DOLT_DATABASE or MYSQL_DATABASE has been specified, create the database if it does not exist
    create_default_database_from_env
    
    # Show what users exist for debugging
    mysql_note "Current users in the system:"
    dolt sql -q "SELECT User, Host FROM mysql.user;" 2>&1 | grep -v "^$" || mysql_warn "Could not list users"
    
    # This contains the server "ready for connections" message which tests use to detect server is ready
    # We only show the logs at this stage since the users are available now for testing
    mysql_note "Reattaching to server process..."
    cat /tmp/server.log

    # Kill the background process and restart in foreground to show live output
    kill $server_pid 2>/dev/null || true
    wait $server_pid 2>/dev/null || true
    
    # Start server in foreground to show live output
    exec dolt sql-server --host=0.0.0.0 --port=3306 "$@"
}

_main "$@"
