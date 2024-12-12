load helper/windows-compat
SERVER_REQS_INSTALLED="FALSE"
SERVER_PID=""
DEFAULT_DB=""

# wait_for_connection(<PORT>, <TIMEOUT IN MS>) attempts to connect to the sql-server at the specified
# port on localhost, using the $SQL_USER (or 'dolt' if unspecified) as the user name, and trying once
# per second until the millisecond timeout is reached. If a connection is successfully established,
# this function returns 0. If a connection was not able to be established within the timeout period,
# this function returns 1.
wait_for_connection() {
  port=$1
  timeout=$2

  if [ -n "$AWS_LAMBDA_FUNCTION_NAME" ]; then
      timeout=$((timeout * 2))
      echo "Running in AWS Lambda; increasing timeout to: $timeout"
  fi

  user=${SQL_USER:-dolt}
  end_time=$((SECONDS+($timeout/1000)))

  while [ $SECONDS -lt $end_time ]; do
    run dolt -u $user -p "$DOLT_REMOTE_PASSWORD" --host localhost --no-tls --port $port --use-db "$DEFAULT_DB" sql -q "SELECT 1;"
    if [ $status -eq 0 ]; then
      echo "Connected successfully!"
      return 0
    fi
    sleep 1
  done

  echo "Failed to connect to database $DEFAULT_DB on port $port within $timeout ms."
  return 1
}

start_sql_server() {
    DEFAULT_DB="$1"
    logFile="$2"
    PORT=$( definePORT )
    if [[ $logFile ]]
    then
        if [ "$IS_WINDOWS" == true ]; then
          dolt sql-server --host 0.0.0.0 --port=$PORT --user "${SQL_USER:-dolt}" > $logFile 2>&1 &
        else
          dolt sql-server --host 0.0.0.0 --port=$PORT --user "${SQL_USER:-dolt}" --socket "dolt.$PORT.sock" > $logFile 2>&1 &
        fi
    else
        if [ "$IS_WINDOWS" == true ]; then
          dolt sql-server --host 0.0.0.0 --port=$PORT --user "${SQL_USER:-dolt}" &
        else
          dolt sql-server --host 0.0.0.0 --port=$PORT --user "${SQL_USER:-dolt}" --socket "dolt.$PORT.sock" &
        fi
    fi
    echo db:$DEFAULT_DB logFile:$logFile PORT:$PORT CWD:$PWD
    SERVER_PID=$!
    wait_for_connection $PORT 8500
}

# like start_sql_server, but the second argument is a string with all
# arguments to dolt-sql-server (excluding --port, which is defined in
# this func)
start_sql_server_with_args() {
    PORT=$( definePORT )
    start_sql_server_with_args_no_port "$@" --port=$PORT
}

# behaves like start_sql_server_with_args, but doesn't define --port.
# caller must set variable PORT to proper value before calling.
start_sql_server_with_args_no_port() {
    DEFAULT_DB=""
    if [ "$IS_WINDOWS" == true ]; then
      dolt sql-server "$@" &
    else
      dolt sql-server "$@" --socket "dolt.$PORT.sock" &
    fi
    SERVER_PID=$!
    wait_for_connection $PORT 8500
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
    if [ "$IS_WINDOWS" == true ]; then
      dolt sql-server --config .cliconfig.yaml &
    else
      dolt sql-server --config .cliconfig.yaml --socket "dolt.$PORT.sock" &
    fi
    SERVER_PID=$!
    wait_for_connection $PORT 8500
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
    if [ "$IS_WINDOWS" == true ]; then
      dolt sql-server --config .cliconfig.yaml &
    else
      dolt sql-server --config .cliconfig.yaml --socket "dolt.$PORT.sock" &
    fi
    SERVER_PID=$!
    wait_for_connection $PORT 8500
}

start_multi_db_server() {
    DEFAULT_DB="$1"
    PORT=$( definePORT )
    if [ "$IS_WINDOWS" == true ]; then
      dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt --data-dir ./ &
    else
      dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt --data-dir ./ --socket "dolt.$PORT.sock" &
    fi
    SERVER_PID=$!
    wait_for_connection $PORT 8500
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
  for i in {0..99}
  do
    port=$((RANDOM % 4096 + 2048))
    # nc (netcat) returns 0 when it _can_ connect to a port (therefore in use), 1 otherwise.
    run nc -z localhost $port
    if [ "$status" -eq 1 ]; then
      echo $port
      break
    fi
  done
}
