SERVER_REQS_INSTALLED="FALSE"
SERVER_PID=""
DEFAULT_DB=""

PYTHON_QUERY_SCRIPT="
import os
import sys

args = sys.argv[sys.argv.index('--') + 1:]
query_results = None
expected_exception = None

working_dir, database, port_str, auto_commit, username, password, query_strs = args[0:7]
if len(args) > 7:
   query_results = args[7]
if len(args) > 8:
   expected_exception = args[8]

print('User: ' + username)
print('Password: ' + password)
print('Query Strings: ' + query_strs)
print('Working Dir: ' + working_dir)
print('Database: ' + database)
print('Port: ' + port_str)
print('Autocommit: ' + auto_commit)
print('Expected Results: ' + str(query_results))
print('Expected Exception: ' + str(expected_exception))

os.chdir(working_dir)

if auto_commit == '1':
    auto_commit = True
else:
    auto_commit = False

from pytest import DoltConnection, csv_to_row_maps

if not database:
    dc = DoltConnection(port=int(port_str), database=None, user=username, password=password, auto_commit=auto_commit)
else:
    dc = DoltConnection(port=int(port_str), database=database, user=username, password=password, auto_commit=auto_commit)

try: 
    dc.connect()
except BaseException as e:
    print('caught exception', str(e))
    if expected_exception is not None and len(expected_exception) > 0:
        if expected_exception not in str(e):
            print('expected exception: ', expected_exception, '\n  got: ', str(e))
            sys.exit(1)
        else:
            sys.exit(0)


queries = query_strs.split(';')
expected = [None]*len(queries)

if query_results is not None:
    expected = query_results.split(';')
    if len(expected) < len(queries):
        expected.extend(['']*(len(queries)-len(expected)))

for i in range(len(queries)):
    query_str = queries[i].strip()
    print('executing:', query_str)

    actual_rows, num_rows = None, None
    try:
        actual_rows, num_rows = dc.query(query_str, False)
    except BaseException as e:
        print('caught exception', str(e))
        if expected_exception is not None and len(expected_exception) > 0:
            if expected_exception not in str(e):
                print('expected exception: ', expected_exception, '\n  got: ', str(e))
                sys.exit(1)
        else:
            sys.exit(0)

    if expected[i] is not None and expected[i] != '':
        print('Raw Expected: ', expected[i])
        expected_rows = csv_to_row_maps(expected[i])
        if expected_rows != actual_rows:
            print('expected:', expected_rows, '\n  actual:', actual_rows)
            sys.exit(1)
"

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
    dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt &
    SERVER_PID=$!
    wait_for_connection $PORT 5000
}

# like start_sql_server, but the second argument is a string with all
# arguments to dolt-sql-server (excluding --port, which is defined in
# this func)
start_sql_server_with_args() {
    DEFAULT_DB=""
    PORT=$( definePORT )
    dolt sql-server "$@" --port=$PORT &
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
    dolt sql-server --config .cliconfig.yaml &
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
    dolt sql-server --config .cliconfig.yaml &
    SERVER_PID=$!
    wait_for_connection $PORT 5000
}

start_multi_db_server() {
    DEFAULT_DB="$1"
    PORT=$( definePORT )
    dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt --data-dir ./ &
    SERVER_PID=$!
    wait_for_connection $PORT 5000
}

# stop_sql_server stops the SQL server. For cases where it's important
# to wait for the process to exit after the kill signal (e.g. waiting
# for an async replication push), pass 1.
# kill the process if it's still running
stop_sql_server() {
    wait=$1
    if [ ! -z "$SERVER_PID" ]; then
      serverpidinuse=$(lsof -i -P -n | grep LISTEN | grep $SERVER_PID | wc -l)
      if [ $serverpidinuse -gt 0 ]; then
        kill $SERVER_PID
        if [ $wait ]; then
            while ps -p $SERVER_PID > /dev/null; do
                sleep .1;
            done
        fi;
      fi
    fi
    SERVER_PID=
}

# server_query connects to a running mysql server,
# executes a query (or list of queries separated by a `;`),
# and compares the results against what is expected.
#
# EXAMPLE: server_query db1 1 dolt "" "select * from test" "c1\n0"
# 
# If executing multiple queries, separate the expected CSV values with a `;`.
#
# EXAMPLE: server_query "" 1 dolt "" "use db1; select * from test" ";c1\n0"
#
# If you expect an exception, leave query results blank and add an additional
# value of 1 to the end of the call. This could be improved to actually send
# up the exception string to be checked but I could not figure out how to do
# that. When calling with bats use `run` and then check the $output if you
# want to inspect the exception string.
#
# EXAMPLE: run server_query "" 1 dolt "" "garbage" "" 1
#          [[ "$output" =~ "error" ]] || false
#
# In the event that the results do not match expectations,
# the python process will exit with an exit code of 1
#
#  * param1: The database name for the connection string.
#            Leave empy for no database.
#  * param2: 1 for autocommit = true, 0 for autocommit = false
#  * param3: User
#  * param4: Password
#  * param5: Query string or query strings separated by `;`
#  * param6: A csv representing the expected result set.
#            If a query is not expected to have a result set "" should
#            be passed. Seprate multiple result sets with `;`
#  * param7: Expected exception value of 1. Mutually exclusive with param6.
#
server_query() {
    PORT=$( definePORT )
    server_query_with_port "$PORT" "$@"
}

# See server_query, but first parameter is the port sql-server is running on,
# every other parameter is positionally one later.
server_query_with_port() {
    let PORT="$1"
    shift
    PYTEST_DIR="$BATS_TEST_DIRNAME/helper"
    echo Executing server_query
    python3 -u -c "$PYTHON_QUERY_SCRIPT" -- "$PYTEST_DIR" "$1" "$PORT" "$2" "$3" "$4" "$5" "$6" "$7"
}

definePORT() {
  getPORT=""
  for i in {0..9}
  do
    let getPORT="($$ + $i) % (65536-1024) + 1024"
    portinuse=$(lsof -i -P -n | grep LISTEN | grep $attemptedPORT | wc -l)
    if [ $portinuse -eq 0 ]; then
      echo "$getPORT"
      break
    fi
  done
}
