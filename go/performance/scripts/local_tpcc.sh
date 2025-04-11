#!/bin/bash
set -e
set -o pipefail

WORKING_DIR=`mktemp -d`
PPROF=0
PORT=3366

# parse options
# superuser.com/questions/186272/
while test $# -gt 0
do
    case "$1" in

        # benchmark with new NomsBinFmt
        --new-nbf) export DOLT_DEFAULT_BIN_FORMAT="__DOLT__"
            ;;

        --new-new) export DOLT_DEFAULT_BIN_FORMAT="__DOLT__" &&
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

    esac
    shift
done

if [ ! -d "./sysbench-tpcc" ]; then
  git clone https://github.com/Percona-Lab/sysbench-tpcc.git
fi

# collect custom sysbench scripts
cp ./sysbench-tpcc/*.lua "$WORKING_DIR"
cd "$WORKING_DIR"

# make a sql-server config file
cat <<YAML > dolt-config.yaml
log_level: "debug"

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
dolt init
dolt sql-server --config="dolt-config.yaml" 2> prepare.log &
SERVER_PID="$!"

# stop it if it crashes
cleanup() {
  kill -15 "$SERVER_PID"
}
trap cleanup EXIT

# setup benchmark
echo "benchmark TPC-C bootstrapping at $WORKING_DIR"

sleep 1

./tpcc.lua --db-driver="mysql" --mysql-db="sbtest" --mysql-host="0.0.0.0" --mysql-port="$PORT" --mysql-user="user" --mysql-password="pass" --time=10 --report_interval=1 --threads=2 --tables=1 --scale=1 --trx_level="RR" prepare

# restart server to isolate bench run
kill -15 "$SERVER_PID"

# maybe run with pprof
if [ "$PPROF" -eq 1 ]; then
  dolt --prof cpu sql-server --config="dolt-config.yaml" 2> run.log &
else
  dolt sql-server --config="dolt-config.yaml" 2> run.log &
fi
SERVER_PID="$!"
sleep 1

# run benchmark
echo "benchmark $SYSBENCH_TEST starting at $WORKING_DIR"

./tpcc.lua --db-driver="mysql" --mysql-db="sbtest" --mysql-host="0.0.0.0" --mysql-port="$PORT" --mysql-user="user" --mysql-password="pass" --time=10 --report_interval=1 --threads=2 --tables=1 --scale=1 --trx_level="RR" run

echo "benchmark TPC-C complete at $WORKING_DIR"
echo "DOLT_DEFAULT_BIN_FORMAT='$DOLT_DEFAULT_BIN_FORMAT'"
echo ""

unset DOLT_DEFAULT_BIN_FORMAT
unset ENABLE_ROW_ITER_2
unset SINGLE_THREAD_FEATURE_FLAG
unset GOMAXPROCS
