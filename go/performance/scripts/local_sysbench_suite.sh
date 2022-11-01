#!/bin/bash
set -e
set -o pipefail

WORKING_DIR=`mktemp -d`
PPROF=0

# parse options
# superuser.com/questions/186272/
while test $# -gt 0
do
    case "$1" in

        # benchmark with new NomsBinFmt
        --new-nbf) export DOLT_DEFAULT_BIN_FORMAT="__DOLT_1__"
            ;;

        --new-new) export DOLT_DEFAULT_BIN_FORMAT="__DOLT_1__" &&
            export ENABLE_ROW_ITER_2=true
            ;;

        --no-exchange) export SINGLE_THREAD_FEATURE_FLAG=true
            ;;

        # benchmark with pprof profiling
        --pprof) PPROF=1
            ;;

        # run dolt single threaded
        --single) export GOMAXPROCS=1
            ;;

        --row2) export ENABLE_ROW_ITER_2=true
            ;;

        # specify sysbench benchmark
        *) SYSBENCH_TEST="$1"
            ;;

    esac
    shift
done

wait_for_connection() {
    PYTEST_DIR="$WORKING_DIR"
    python3 -c "
import os
import sys

args = sys.argv[sys.argv.index('--') + 1:]
working_dir, database, port_str, timeout_ms = args
os.chdir(working_dir)

from pytest import wait_for_connection
wait_for_connection(port=int(port_str), timeout_ms=int(timeout_ms), database=database, user='user')
" -- "$PYTEST_DIR" "$DEFAULT_DB" "$1" "$2"
}

if [ ! -d "./sysbench-lua-scripts" ]; then
  git clone https://github.com/dolthub/sysbench-lua-scripts.git
fi

# collect custom sysbench scripts
cp ./sysbench-lua-scripts/*.lua "$WORKING_DIR"
cd "$WORKING_DIR"


# make a sql-server config file
cat <<YAML > dolt-config.yaml
log_level: "info"

behavior:
  read_only: false
  autocommit: true

user:
  name: "user"
  password: "pass"

listener:
  host: "0.0.0.0"
  port: $PORT
  max_connections: 128
  read_timeout_millis: 28800000
  write_timeout_millis: 28800000

databases:
  - name: "sbtest"
    path: "."
YAML

# start a server
let PORT1="$$ % (65536-1024) + 1024"
let PORT2="$$ % (65536-1025) + 1025"

dolt init
dolt sql-server --port=$PORT1 --config="dolt-config.yaml" 2> prepare1.log &
SERVER_PID=$!
#wait_for_connection $PORT 5000
sleep 1

# stop it if it crashes
cleanup() {
  kill -15 "$SERVER_PID"
}
trap cleanup EXIT


# loop through and run all of the tests
# for each test, grep for the median latency, print in summary file with test name and dolt version
readTests=('oltp_read_only', 'oltp_point_select', 'select_random_points', 'select_random_ranges', 'covering_index_scan', 'index_scan', 'table_scan', 'groupby_scan', 'index_join_scan')
writeTests=('oltp_read_write', 'oltp_update_index', 'oltp_update_non_index', 'oltp_insert', 'bulk_insert', 'oltp_write_only', 'oltp_delete_insert')

END=${#readTests[@]}
echo $END
for ((i=0;i<=END-1;i++)); do
    echo "test: ${readTests[$i]}"
    #sysbench \
      #--db-driver="mysql" \
      #--mysql-host="0.0.0.0" \
      #--mysql-port="$PORT" \
      #--mysql-user="user" \
      #--mysql-password="pass" \
      #"$SYSBENCH_TEST" prepare

    #sysbench \
      #--db-driver="mysql" \
      #--mysql-host="0.0.0.0" \
      #--mysql-port="$PORT" \
      #--mysql-user="user" \
      #--mysql-password="pass" \
      #--db-ps-mode=disable \
      #"$SYSBENCH_TEST" run

done

## restart server to isolate bench run
#kill -15 "$SERVER_PID"

## maybe run with pprof
#if [ "$PPROF" -eq 1 ]; then
  #dolt --prof cpu sql-server --config="dolt-config.yaml" 2> run.log &
#else
  #dolt sql-server --config="dolt-config.yaml" 2> run.log &
#fi
#SERVER_PID="$!"
#sleep 1


# run benchmark
#echo "benchmark $SYSBENCH_TEST starting at $WORKING_DIR"

unset DOLT_DEFAULT_BIN_FORMAT
unset ENABLE_ROW_ITER_2
unset SINGLE_THREAD_FEATURE_FLAG
unset GOMAXPROCS

echo "benchmark $SYSBENCH_TEST complete at $WORKING_DIR"
echo "DOLT_FORMAT_FEATURE_FLAG='$DOLT_FORMAT_FEATURE_FLAG'"
echo ""
