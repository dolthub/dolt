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

        # benchmark with new NomsBinFmt
        --new-nbf) export DOLT_FORMAT_FEATURE_FLAG=true
            ;;

        # benchmark with pprof profiling
        --pprof) PPROF=1
            ;;

        # run dolt single threaded
        --single) export GOMAXPROCS=1
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
  autocommit: true

user:
  name: "user"
  password: "pass"

listener:
  host: "0.0.0.0"
  port: 3306
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
echo "benchmark $SYSBENCH_TEST bootstrapping at $WORKING_DIR"

sleep 1
sysbench \
  --mysql-host="0.0.0.0" \
  --mysql-user="user" \
  --mysql-password="pass" \
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
  --mysql-host="0.0.0.0" \
  --mysql-user="user" \
  --mysql-password="pass" \
  "$SYSBENCH_TEST" run

echo "benchmark $SYSBENCH_TEST complete at $WORKING_DIR"
echo "DOLT_FORMAT_FEATURE_FLAG='$DOLT_FORMAT_FEATURE_FLAG'"
echo ""
