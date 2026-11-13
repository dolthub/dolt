#!/bin/bash

HOST="127.0.0.1"
PORT=3306
DBNAME="sbtest"
USER="root"
PASS=""

TIME=20
TABLE_SIZE=10000
SYSBENCH_TEST="default"

# parse options
while test $# -gt 0
do
  case "$1" in
    # specify table size
    --table-size)
      TABLE_SIZE="$2"
      shift
      ;;

    # specify sysbench benchmark
    *) SYSBENCH_TEST="$1"
      shift
      ;;
    esac
done

if [ ! -d "./sysbench-custom" ]; then
  echo "directory './sysbench-custom' is missing"
  exit
fi
cd "./sysbench-custom"

sysbench \
  --db-driver="mysql" \
  --mysql-host="$HOST" \
  --mysql-port="$PORT" \
  --mysql-user="$USER" \
  --mysql-password="$PASS" \
  --db-ps-mode=disable \
  --rand-type=uniform \
  --time="$TIME" \
  --report_interval=10 \
  --table-size="$TABLE_SIZE" \
  --percentile=95 \
  --rand-type=uniform \
  --rand-seed=1 \
  "$SYSBENCH_TEST" cleanup

sysbench \
  --db-driver="mysql" \
  --mysql-host="$HOST" \
  --mysql-port="$PORT" \
  --mysql-user="$USER" \
  --mysql-password="$PASS" \
  --db-ps-mode=disable \
  --rand-type=uniform \
  --time="$TIME" \
  --report_interval=10 \
  --table-size="$TABLE_SIZE" \
  --percentile=95 \
  --rand-type=uniform \
  --rand-seed=1 \
  "$SYSBENCH_TEST" prepare

# run benchmark
echo "benchmark $SYSBENCH_TEST starting at $WORKING_DIR"

sysbench \
  --db-driver="mysql" \
  --mysql-host="$HOST" \
  --mysql-port="$PORT" \
  --mysql-user="$USER" \
  --mysql-password="$PASS" \
  --db-ps-mode=disable \
  --rand-type=uniform \
  --time="$TIME" \
  --report_interval=10 \
  --table-size="$TABLE_SIZE" \
  --percentile=95 \
  --rand-type=uniform \
  --rand-seed=1 \
  "$SYSBENCH_TEST" run

