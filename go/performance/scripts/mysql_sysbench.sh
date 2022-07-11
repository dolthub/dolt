#!/bin/bash
set -e
set -o pipefail


HOST="127.0.0.1"
PORT=3316
DBNAME="tpcc_test"
USER="root"
PASS="toor"

SYSBENCH_TEST="oltp_point_select"

# parse options
# superuser.com/questions/186272/
while test $# -gt 0
do
    case "$1" in

        # specify sysbench benchmark
        *) SYSBENCH_TEST="$1"
            ;;

    esac
    shift
done

if [ ! -d "./sysbench-lua-scripts" ]; then
  git clone https://github.com/dolthub/sysbench-lua-scripts.git
fi

pushd sysbench-lua-scripts


sysbench \
  --mysql-host="$HOST" \
  --mysql-user="$USER" \
  --mysql-password="$PASS" \
  --mysql-port="$PORT" \
  --db-ps-mode=disable \
  "$SYSBENCH_TEST" cleanup

sysbench \
  --mysql-host="$HOST" \
  --mysql-user="$USER" \
  --mysql-password="$PASS" \
  --mysql-port="$PORT" \
  "$SYSBENCH_TEST" prepare

# run benchmark
echo "benchmark $SYSBENCH_TEST starting at $WORKING_DIR"

sysbench \
  --mysql-host="$HOST" \
  --mysql-user="$USER" \
  --mysql-password="$PASS" \
  --mysql-port="$PORT" \
  --db-ps-mode=disable \
  --rand-type="uniform" \
  "$SYSBENCH_TEST" run

popd
