SERVER_REQS_INSTALLED="FALSE"
SERVER_PID=""
DEFAULT_DB=""

set_server_reqs_installed() {
    SERVER_REQS_INSTALLED=$(python3 -c "
requirements_installed = True
try:
    import mysql.connector
except:
    requirements_installed = False

print(str(requirements_installed).upper())
")
}

wait_for_connection() {
    PYTEST_DIR="$BATS_TEST_DIRNAME/helper"
    python3 -c "
import os
import sys

args = sys.argv[sys.argv.index('--') + 1:]
working_dir, database, port_str, timeout_ms = args
os.chdir(working_dir)

from pytest import wait_for_connection
wait_for_connection(port=int(port_str), timeout_ms=int(timeout_ms), database=database, user='dolt')
" -- "$PYTEST_DIR" "$DEFAULT_DB" "$1" "$2"
}

start_sql_server() {
    DEFAULT_DB="$1"
    PORT=$( definePORT )
    dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt --socket "dolt.$PORT.sock" &
    SERVER_PID=$!
    wait_for_connection $PORT 5000
}

# like start_sql_server, but the second argument is a string with all
# arguments to dolt-sql-server (excluding --port, which is defined in
# this func)
start_sql_server_with_args() {
    DEFAULT_DB=""
    PORT=$( definePORT )
    dolt sql-server "$@" --port=$PORT --socket "dolt.$PORT.sock" &
    SERVER_PID=$!
    wait_for_connection $PORT 5000
}

start_sql_server_with_config() {
    DEFAULT_DB="$1"
    PORT=$( definePORT )
    echo "
log_level: debug

user:
  name: dolt

listener:
  host: 0.0.0.0
  port: $PORT
  max_connections: 10

behavior:
  autocommit: false
" > .cliconfig.yaml
    cat "$2" >> .cliconfig.yaml
    dolt sql-server --config .cliconfig.yaml --socket "dolt.$PORT.sock" &
    SERVER_PID=$!
    wait_for_connection $PORT 5000
}

start_sql_multi_user_server() {
    DEFAULT_DB="$1"
    PORT=$( definePORT )
    echo "
log_level: debug

user:
  name: dolt

listener:
  host: 0.0.0.0
  port: $PORT
  max_connections: 10

behavior:
  autocommit: false
" > .cliconfig.yaml
    dolt sql-server --config .cliconfig.yaml --socket "dolt.$PORT.sock" &
    SERVER_PID=$!
    wait_for_connection $PORT 5000
}

start_multi_db_server() {
    DEFAULT_DB="$1"
    PORT=$( definePORT )
    dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt --data-dir ./ --socket "dolt.$PORT.sock" &
    SERVER_PID=$!
    wait_for_connection $PORT 5000
}

# stop_sql_server stops the SQL server. For cases where it's important
# to wait for the process to exit after the kill signal (e.g. waiting
# for an async replication push), pass 1.
# kill the process if it's still running
stop_sql_server() {
    # Clean up any mysql.sock file in the default, global location
    if [ -f "/tmp/mysql.sock" ]; then rm -f /tmp/mysql.sock; fi
    if [ -f "/tmp/dolt.$PORT.sock" ]; then rm -f /tmp/dolt.$PORT.sock; fi

    wait=$1
    if [ ! -z "$SERVER_PID" ]; then
        # ignore failures of kill command in the case the server is already dead
        run kill $SERVER_PID
        if [ $wait ]; then
            while ps -p $SERVER_PID > /dev/null; do
                sleep .1;
            done
        fi;
    fi
    SERVER_PID=
}

definePORT() {
  getPORT=""
  for i in {0..9}
  do
    let getPORT="($$ + $i) % (65536-1024) + 1024"
    portinuse=$(lsof -i -P -n | grep LISTEN | grep $getPORT | wc -l)
    if [ $portinuse -eq 0 ]; then
      echo "$getPORT"
      break
    fi
  done
}
