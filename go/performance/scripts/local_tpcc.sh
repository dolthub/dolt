#!/bin/bash
set -e
set -o pipefail

WORKING_DIR=`mktemp -d`
mkdir $WORKING_DIR/sbtest
WORKING_DIR=$WORKING_DIR/sbtest
PPROF=0
PORT=3366

USER="root"
PASS=""

# parse options
# superuser.com/questions/186272/
while test $# -gt 0
do
    case "$1" in

        # benchmark with new NomsBinFmt
        --new-nbf) export DOLT_DEFAULT_BIN_FORMAT="__DOLT__"
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

# grab testing SSL pems
cp ../../libraries/doltcore/servercfg/testdata/chain* "$WORKING_DIR"

cd "$WORKING_DIR"

# make a sql-server config file
cat <<YAML > dolt-config.yaml
log_level: info

listener:
  host: 127.0.0.1
  port: $PORT
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
echo "benchmark TPC-C bootstrapping at $WORKING_DIR"

sleep 1

./tpcc.lua \
  --db-driver="mysql" \
  --mysql-db="sbtest" \
  --mysql-host="127.0.0.1" \
  --mysql-port="$PORT" \
  --mysql-user="$USER" \
  --mysql-password="$PASS" \
  --time=120 \
  --report_interval=1 \
  --threads=2 \
  --tables=1 \
  --scale=1 \
  --trx_level="RR" prepare

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

./tpcc.lua \
  --db-driver="mysql" \
  --mysql-db="sbtest" \
  --mysql-host="127.0.0.1" \
  --mysql-port="$PORT" \
  --mysql-user="$USER" \
  --mysql-password="$PASS" \
  --time=120 \
  --report_interval=1 \
  --threads=2 \
  --tables=1 \
  --scale=1 \
  --trx_level="RR" run

unset DOLT_DEFAULT_BIN_FORMAT
unset SINGLE_THREAD_FEATURE_FLAG
unset GOMAXPROCS

echo "benchmark $SYSBENCH_TEST complete at $WORKING_DIR"
if [ "$PPROF" -eq 1 ]; then
  # parse run.log to output the profile location
  head -n1 "$WORKING_DIR/run.log" | cut -d ":" -f 4
fi
echo ""
