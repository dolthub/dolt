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

CONTAINER_DATA_DIR="/var/lib/dolt"
DOLT_CONFIG_DIR="/etc/dolt/doltcfg.d"
SERVER_CONFIG_DIR="/etc/dolt/servercfg.d"
DOLT_ROOT_PATH="/.dolt"

# create all dirs in path
_create_dir() {
    local path="$1"
    mkdir -p "$path"
}

check_for_dolt() {
    echo "Verifying dolt executable..."
    local dolt_bin=$(which dolt)
    if [ ! -x "$dolt_bin" ]; then
        mysql_error "dolt binary executable not found"
    fi
}

# To make /var/lib/dolt/ a fixed directory to start the server in and store the data in,
# set data_dir to null as default in yaml file
check_for_data_dir_definition() {
    dd_found=(`grep $1 -e "data_dir" | wc -l`)
    if [ $dd_found -gt 0 ]; then
        mysql_warn "Data directory cannot be defined in docker container. It is set to '/var/lib/dolt' directory and can be mounted with local directory on host."
        sed -i -e 's/data_dir.*/data_dir: null/' $1
        rm "$1-e"
    else
        echo "data_dir: null" > /default_config.yaml
    fi
}

# check arguments for an option that would cause mysqld to stop
# return true if there is one
_mysql_want_help() {
	local arg
	for arg; do
		case "$arg" in
			-'?'|-h|--help)
				return 0
				;;
		esac
	done
	return 1
}

# arg $1 is the directory to search in
# arg $2 is the type file to search for
get_config_file_path_if_exists() {
    CONFIG_PROVIDED=
    CONFIG_DIR=$1
    FILE_TYPE=$2
    if [ -d "$CONFIG_DIR" ]; then
        echo "Checking for config provided in $CONFIG_DIR"
        files_found=( `ls $CONFIG_DIR/*$FILE_TYPE` )
        number_of_files_found=( `find .$CONFIG_DIR -type f -name "*.$FILE_TYPE" | wc -l` )
        if [ $number_of_files_found -gt 1 ]; then
            CONFIG_PROVIDED=
            mysql_warn "multiple config file found in $CONFIG_DIR, using default config"
        elif [ $number_of_files_found -eq 1 ]; then
            echo "$files_found file is found"
            CONFIG_PROVIDED=$files_found
        else
            CONFIG_PROVIDED=
        fi
    fi
}

# taken from https://github.com/docker-library/mysql/blob/master/8.0/docker-entrypoint.sh
# this function will run files found in /docker-entrypoint-initdb.d directory AFTER server is started
# usage: docker_process_init_files [file [file [...]]]
#    ie: docker_process_init_files /always-initdb.d/*
# process initializer files, based on file extensions
docker_process_init_files() {
	echo
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

start_server() {
    # start the server in fixed data directory at /var/lib/dolt
    cd $CONTAINER_DATA_DIR
    "$@" $1
}

# if there is config file provided through /etc/dolt/doltcfg.d,
# we overwrite $HOME/.dolt/config_global.json file with this file.
set_dolt_config_if_defined() {
    get_config_file_path_if_exists "$DOLT_CONFIG_DIR" "json"
    if [ ! -z $CONFIG_PROVIDED ]; then
        /bin/cp -rf $CONFIG_PROVIDED $HOME/$DOLT_ROOT_PATH/config_global.json
    fi
}

# if there is config yaml file provided through /etc/dolt/servercfg.d
# we use it as configuration to start the server with; otherwise,
# the server will start with default configuration and what's defined
# in the commandline argument
get_server_config_if_defined() {
    get_config_file_path_if_exists "$SERVER_CONFIG_DIR" "yaml"
    if [ ! -z $CONFIG_PROVIDED ]; then
        check_for_data_dir_definition $CONFIG_PROVIDED
        echo "--config=$CONFIG_PROVIDED"
    else
        echo "--config=/default_config.yaml"
    fi
}

_main() {
    # check for dolt binary executable
    check_for_dolt

    if [ "${1:0:1}" = '-' ]; then
        # if there is any command line argument defined we use
        # them with default command `dolt sql-server --host=0.0.0.0 --port=3306`
        # why we use fixed host=0.0.0.0 and port=3306 in README
        commandline_arg=( `echo $@ | sed 's/h//'` )
        echo "THIS IS COMMANDLINE ARG : $@"

        set -- dolt sql-server --host=0.0.0.0 --port=3306 "$@"
    fi

    if [ "$1" = 'dolt' ] && [ "$2" = 'sql-server' ] && ! _mysql_want_help "$@"; then
        local dolt_version=$(dolt version | grep 'dolt version' | cut -f3 -d " ")
        mysql_note "Entrypoint script for Dolt Server $dolt_version starting."

        declare -g CONFIG_PROVIDED

        # dolt config will be set if user provided a single json file in /etc/dolt/doltcfg.d directory.
        # It will overwrite config_global.json file in $HOME/.dolt
        set_dolt_config_if_defined

        # if there is a single yaml provided in /etc/dolt/servercfg.d directory,
        # it will be used to start the server with --config flag
        start_server set_server_config_if_defined

        # run any file provided in /docker-entrypoint-initdb.d directory after the server starts
        docker_process_init_files /docker-entrypoint-initdb.d/*

        mysql_note "Dolt Server $dolt_version is started."
    fi
    exec "$@"
}

_main "$@"
