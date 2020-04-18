SERVER_REQS_INSTALLED="FALSE"
SERVER_PID=""
DEFAULT_DB=""
PYTHON_QUERY_SCRIPT="
import os
import sys

args = sys.argv[sys.argv.index('--') + 1:]
query_results = None

if len(args) == 6:
    working_dir, database, port_str, auto_commit, query_strs, query_results = args
else:
    working_dir, database, port_str, auto_commit, query_strs = args

print('Query Strings: ' + query_strs)
print('Workind Dir: ' + working_dir)
print('Database: ' + database)
print('Port: ' + port_str)
print('Autocommit: ' + auto_commit)
print('Expected Results: ' + str(query_results))

os.chdir(working_dir)

if auto_commit == '1':
    auto_commit = True
else:
    auto_commit = False

from pytest import DoltConnection, csv_to_row_maps

dc = DoltConnection(port=int(port_str), database=database, user='dolt', auto_commit=auto_commit)
dc.connect()

queries = query_strs.split(';')
expected = [None]*len(queries)

if query_results is not None:
    expected = query_results.split(';')
    if len(expected) < len(queries):
        expected.extend(['']*(len(queries)-len(expected)))

for i in range(len(queries)):
    query_str = queries[i].strip()
    print('executing:', query_str)
    actual_rows = dc.query(query_str)

    if expected[i] is not None:
        expected_rows = csv_to_row_maps(expected[i])
        print('expected:', expected_rows, '\n  actual:', actual_rows)

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
    let PORT="$$ % (65536-1024) + 1024"
    dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt &
    SERVER_PID=$!
    wait_for_connection $PORT 5000
}


start_multi_db_server() {
    DEFAULT_DB="$1"
    let PORT="$$ % (65536-1024) + 1024"
    dolt sql-server --host 0.0.0.0 --port=$PORT --user dolt --multi-db-dir ./ &
    SERVER_PID=$!
    wait_for_connection $PORT 5000
}

stop_sql_server() {
    kill $SERVER_PID
}

# server_query connects to a running mysql server, executes a query and compares the results against what is expected.
# In the event that the results do not match expectations, the python process will exit with an exit code of 1
#  * param1 is 1 for autocommit = true, 0 for autocommit = false
#  * param2 is the query_str
#  * param3 is a csv representing the expected result set.  If a query is not expected to have a result set "" should
#      be passed.
server_query() {
    let PORT="$$ % (65536-1024) + 1024"
    PYTEST_DIR="$BATS_TEST_DIRNAME/helper"
    echo Executing server_query
    python3 -c "$PYTHON_QUERY_SCRIPT" -- "$PYTEST_DIR" "$DEFAULT_DB" "$PORT" "$1" "$2" "$3"
}

# server_query connects to a running mysql server, executes a query and compares the results against what is expected.
# In the event that the results do not match expectations, the python process will exit with an exit code of 1
#  * param1 is 1 for autocommit = true, 0 for autocommit = false
#  * param2 is the query_str
multi_query() {
    let PORT="$$ % (65536-1024) + 1024"
    PYTEST_DIR="$BATS_TEST_DIRNAME/helper"
    echo Executing multi_query
    python3 -c "$PYTHON_QUERY_SCRIPT" -- "$PYTEST_DIR" "$DEFAULT_DB" "$PORT" "$1" "$2"
}

# update_query runs an update query and should be called with 2 parameters
#   * param1 is 1 for autocommit = true, 0 for autocommit = false
#   * param2 is the query string
update_query() {
    server_query $1 "$2" ""
}

# insert_query runs an insert query and should be called with 2 parameters
#   * param1 is 1 for autocommit = true, 0 for autocommit = false
#   * param2 is the query string
insert_query() {
    server_query $1 "$2" ""
}
