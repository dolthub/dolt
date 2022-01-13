#!/bin/bash
set -e
set -o pipefail

#SYSBENCH_TEST="oltp_point_select"
SYSBENCH_TEST="table_scan"

TMP_DIR=`mktemp -d`
cp ./lua/* "$TMP_DIR"
cd "$TMP_DIR"

echo " "
echo "running benchmark $SYSBENCH_TEST in $TMP_DIR"
echo " "

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

dolt init
dolt sql-server --config="dolt-config.yaml" &
SERVER_PID="$!"

cleanup() {
  kill -15 "$SERVER_PID"
}
trap cleanup EXIT

sleep 1
sysbench \
  --mysql-host="0.0.0.0" \
  --mysql-user="user" \
  --mysql-password="pass" \
  "$SYSBENCH_TEST" prepare

# restart server to isolate bench run
kill -15 "$SERVER_PID"
dolt sql-server --config="dolt-config.yaml" &
SERVER_PID="$!"

sleep 1
sysbench \
  --mysql-host="0.0.0.0" \
  --mysql-user="user" \
  --mysql-password="pass" \
  "$SYSBENCH_TEST" run
