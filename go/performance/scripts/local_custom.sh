#!/bin/bash

WORKING_DIR=`mktemp -d`
mkdir $WORKING_DIR/sbtest
WORKING_DIR=$WORKING_DIR/sbtest

PORT=3366
USER="root"
PASS=""

PPROF=0
TIME=20
TABLE_SIZE=10000
SYSBENCH_TEST="default"

# parse options
while test $# -gt 0
do
  case "$1" in
    # benchmark with pprof profiling
    --pprof)
      PPROF=1
      shift
      ;;

    # specify benchmark duration
    --time)
      TIME="$2"
      shift
      ;;

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


# collect custom sysbench scripts
if [ ! -d "./sysbench-custom" ]; then
  echo "directory './sysbench-custom' is missing"
  exit
fi
cp ./sysbench-custom/*.lua "$WORKING_DIR"

# grab testing SSL pems
cp ../../libraries/doltcore/servercfg/testdata/chain* "$WORKING_DIR"

cd "$WORKING_DIR"

# make a sql-server config file if it doesn't already exist
cat <<YAML > dolt-config.yaml
log_level: info

listener:
  host: 127.0.0.1
  port: 3366
  tls_key: "./chain_key.pem"
  tls_cert: "./chain_cert.pem"
  require_secure_transport: true

system_variables: {
  sql_mode: ""
}

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
echo "benchmark $SYSBENCH_TEST starting at $WORKING_DIR"

sleep 1
sysbench \
  --db-driver="mysql" \
  --mysql-db="sbtest" \
  --mysql-host="127.0.0.1" \
  --mysql-port="$PORT" \
  --mysql-user="$USER" \
  --mysql-password="$PASS" \
  --db-ps-mode=disable \
  --time="$TIME" \
  --report_interval=10 \
  --table-size="$TABLE_SIZE" \
  --percentile=95 \
  --rand-type=uniform \
  --rand-seed=1 \
  "$SYSBENCH_TEST" prepare

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

sysbench \
  --db-driver="mysql" \
  --mysql-db="sbtest" \
  --mysql-host="127.0.0.1" \
  --mysql-port="$PORT" \
  --mysql-user="$USER" \
  --mysql-password="$PASS" \
  --db-ps-mode=disable \
  --time="$TIME" \
  --report_interval=10 \
  --table-size="$TABLE_SIZE" \
  --percentile=95 \
  --rand-type=uniform \
  --rand-seed=1 \
  "$SYSBENCH_TEST" run

echo "benchmark $SYSBENCH_TEST complete at $WORKING_DIR"
if [ "$PPROF" -eq 1 ]; then
  # parse run.log to output the profile location
  head -n1 "$WORKING_DIR/run.log" | cut -d ":" -f 4
fi
echo ""
