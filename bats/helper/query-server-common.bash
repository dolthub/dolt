
wait_for_connection() {
    PYTEST_DIR="$BATS_TEST_DIRNAME/helper"
    python3 -c "
import os
import sys

args = sys.argv[sys.argv.index('--') + 1:]
working_dir, port_str, timeout_ms = args
os.chdir(working_dir)

from pytest import wait_for_connection
wait_for_connection(port=int(port_str), timeout_ms=int(timeout_ms))
" -- $PYTEST_DIR $1 $2
}

start_sql_server() {
    let PORT="$$ % (65536-1024) + 1024"
    dolt sql-server --port=$PORT &
    wait_for_connection $PORT 5000
}

stop_sql_server() {
    let PORT="$$ % (65536-1024) + 1024"
    pkill -f "dolt sql-server --port=$PORT"
}

# server_query connects to a running mysql server, executes a query and compares the results against what is expected.
# In the event that the results do not match expectations, the python process will exit with an exit code of 1
#  * param1 is the query_str
#  * param2 is a csv representing the expected result set.  If a query is not expected to have a result set "" should
#      be passed.
server_query() {
    let PORT="$$ % (65536-1024) + 1024"
    PYTEST_DIR=`batshelper`
    python3 -c "
import os
import sys

args = sys.argv[sys.argv.index('--') + 1:]
print(args)
working_dir, port_str, query_str, query_results = args
os.chdir(working_dir)

from pytest import DoltConnection, csv_to_row_maps

expected_rows = csv_to_row_maps(query_results)

dc = DoltConnection(port=int(port_str))
dc.connect()

print('executing:', query_str)
actual_rows = dc.query(query_str)

print('expected:', expected_rows, '\n  actual:', actual_rows)

if expected_rows != actual_rows:
    print('expected:', expected_rows, '\n  actual:', actual_rows)
    sys.exit(1)
" -- $PYTEST_DIR $PORT "$1" "$2"
}

# update_query runs an update query and should be called with 2 parameters
#   * param1 is the query string
#   * param2 is the expected number of rows affected
update_query() {
    server_query "$1" "matched,updated\n$2,$2"
}

# insert_query runs an insert query and should be called with 2 parameters
#   * param1 is the query string
#   * param2 is the expected number of rows inserted
insert_query() {
    server_query "$1" "updated\n$2"
}
