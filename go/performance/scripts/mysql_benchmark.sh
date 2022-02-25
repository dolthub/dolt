#!/bin/bash
set -e
set -o pipefail

SYSBENCH_TEST="oltp_point_select"
WORKING_DIR=`mktemp -d`
PPROF=0
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

sleep 1
sysbench \
  --mysql-host="127.0.0.1" \
  --mysql-user="root" \
  --mysql-password="toor" \
  --mysql-port=3316 \
  "$SYSBENCH_TEST" prepare

# run benchmark
echo "benchmark $SYSBENCH_TEST starting at $WORKING_DIR"

sysbench \
  --mysql-host="127.0.0.1" \
  --mysql-user="root" \
  --mysql-password="toor" \
  --mysql-port=3316 \
  --db-ps-mode=disable \
  "$SYSBENCH_TEST" run

popd
