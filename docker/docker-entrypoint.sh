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

# arg $1 is the directory to search in
# arg $2 is the type file to search for
get_config_file_path_if_exists() {
    CONFIG_PROVIDED=
    CONFIG_DIR=$1
    FILE_TYPE=$2
    if [ -d "$CONFIG_DIR" ]; then
        mysql_note "Checking for config provided in $CONFIG_DIR"
        number_of_files_found=( `find $CONFIG_DIR -type f -name "*.$FILE_TYPE" | wc -l` )
        if [ $number_of_files_found -gt 1 ]; then
            CONFIG_PROVIDED=
            mysql_warn "multiple config file found in $CONFIG_DIR, using default config"
        elif [ $number_of_files_found -eq 1 ]; then
            files_found=( `ls $CONFIG_DIR/*$FILE_TYPE` )
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
					"$f"
				else
					mysql_note "$0: sourcing $f"
					. "$f"
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
    if [ ! -z $CONFIG_PROVIDED ]; then
        /bin/cp -rf $CONFIG_PROVIDED $HOME/$DOLT_ROOT_PATH/config_global.json
    fi
}

create_default_database_from_env() {
    local database=""

    if [ -n "$DOLT_DATABASE" ]; then
        database="$DOLT_DATABASE"
    elif [ -n "$MYSQL_DATABASE" ]; then
        database="$MYSQL_DATABASE"
    fi

    if [ -n "$database" ]; then
        dolt sql -q "CREATE DATABASE IF NOT EXISTS $database;"
    fi
}

_main() {
    # check for dolt binary executable
    check_for_dolt

    local dolt_version=$(dolt version | grep 'dolt version' | cut -f3 -d " ")
    mysql_note "Entrypoint script for Dolt Server $dolt_version starting."

    declare -g CONFIG_PROVIDED

    # dolt config will be set if user provided a single json file in /etc/dolt/doltcfg.d directory.
    # It will overwrite config_global.json file in $HOME/.dolt
    set_dolt_config_if_defined

    # if there is a single yaml provided in /etc/dolt/servercfg.d directory,
    # it will be used to start the server with --config flag
    get_config_file_path_if_exists "$SERVER_CONFIG_DIR" "yaml"
    if [ ! -z $CONFIG_PROVIDED ]; then
        set -- "$@" --config=$CONFIG_PROVIDED
    fi

    # If DOLT_ROOT_HOST has been specified – create a root user for that host with the specified password
    if [ -n "$DOLT_ROOT_HOST" ] && [ "$DOLT_ROOT_HOST" != 'localhost' ]; then
       echo "Ensuring root@${DOLT_ROOT_HOST} superuser exists (DOLT_ROOT_HOST was specified)"
       dolt sql -q "CREATE USER IF NOT EXISTS 'root'@'${DOLT_ROOT_HOST}' IDENTIFIED BY '${DOLT_ROOT_PASSWORD}';
                    ALTER USER 'root'@'${DOLT_ROOT_HOST}' IDENTIFIED BY '${DOLT_ROOT_PASSWORD}';
                    GRANT ALL ON *.* TO 'root'@'${DOLT_ROOT_HOST}' WITH GRANT OPTION;"
    fi

    # Ensure the root@localhost user exists, with the requested password
    echo "Ensuring root@localhost user exists"
    dolt sql -q "CREATE USER IF NOT EXISTS 'root'@'localhost' IDENTIFIED BY '${DOLT_ROOT_PASSWORD}';
                 ALTER USER 'root'@'localhost' IDENTIFIED BY '${DOLT_ROOT_PASSWORD}';
                 GRANT ALL ON *.* TO 'root'@'localhost' WITH GRANT OPTION;"

    # If DOLT_DATABASE or MYSQL_DATABASE has been specified, create the database if it does not exist
    create_default_database_from_env

    if [[ ! -f $INIT_COMPLETED ]]; then
        # run any file provided in /docker-entrypoint-initdb.d directory before the server starts
        docker_process_init_files /docker-entrypoint-initdb.d/*
        touch $INIT_COMPLETED
    fi

    # switch this process over to executing dolt sql-server
    exec dolt sql-server --host=0.0.0.0 --port=3306 "$@"
}

_main "$@"
