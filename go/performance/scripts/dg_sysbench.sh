#!/bin/bash
set -e
set -o pipefail

SYSBENCH_TEST="oltp_insert_only"
WORKING_DIR=`mktemp -d`
PPROF=0
PORT=5433

# parse options
# superuser.com/questions/186272/
while test $# -gt 0
do
    case "$1" in

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

        --row2) export ENABLE_ROW_ITER_2=true
            ;;

        --journal) export DOLT_ENABLE_CHUNK_JOURNAL=true
            ;;

        # specify sysbench benchmark
        *) SYSBENCH_TEST="$1"
            ;;

    esac
    shift
done

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

user:
  name: "user"
  password: "pass"

listener:
  host: "0.0.0.0"
  port: $PORT
  read_timeout_millis: 28800000
  write_timeout_millis: 28800000

data_dir: .
YAML

# start a server
mkdir sbtest
cd sbtest
doltgres -config="../dolt-config.yaml" 2> prepare.log &
SERVER_PID="$!"

set -x

sleep 1

ps aux | grep "doltgres"
lsof -iTCP -sTCP:LISTEN
echo $SERVER_PID
psql --port $PORT --host=0.0.0.0 --db=doltgres -c "create database sbtest"


# stop it if it crashes
cleanup() {
  kill -15 "$SERVER_PID"
}
trap cleanup EXIT

# setup benchmark
echo "benchmark $SYSBENCH_TEST bootstrapping at $WORKING_DIR"


sysbench \
  --db-driver="pgsql" \
  --pgsql-host="0.0.0.0" \
  --pgsql-port="$PORT" \
  --pgsql-user="user" \
  --pgsql-password="pass" \
  "$SYSBENCH_TEST" prepare

# restart server to isolate bench run
kill -15 "$SERVER_PID"

# maybe run with pprof
if [ "$PPROF" -eq 1 ]; then
  doltgres --prof cpu -config="../dolt-config.yaml" 2> run.log &
else
  doltgres -config="../dolt-config.yaml" 2> run.log &
fi
SERVER_PID="$!"
sleep 1


# run benchmark
echo "benchmark $SYSBENCH_TEST starting at $WORKING_DIR"

sysbench \
  --db-driver="pgsql" \
  --pgsql-host="0.0.0.0" \
  --pgsql-port="$PORT" \
  --pgsql-user="user" \
  --pgsql-password="pass" \
  --db-ps-mode=disable \
  --time=30 \
  --db-ps-mode=disable \
  "$SYSBENCH_TEST" run

unset DOLT_ENABLE_CHUNK_JOURNAL
unset DOLT_DEFAULT_BIN_FORMAT
unset ENABLE_ROW_ITER_2
unset SINGLE_THREAD_FEATURE_FLAG
unset GOMAXPROCS

echo "benchmark $SYSBENCH_TEST complete at $WORKING_DIR"
if [ "$PPROF" -eq 1 ]; then
  # parse run.log to output the profile location
  head -n1 "$WORKING_DIR/run.log" | cut -d ":" -f 4
fi
echo ""
