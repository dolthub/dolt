#!/bin/bash
set -e
set -o pipefail

HOST="127.0.0.1"
PORT=3316
DBNAME="tpcc_test"
USER="root"
PASS="toor"

if [ ! -d "./sysbench-tpcc" ]; then
  git clone https://github.com/Percona-Lab/sysbench-tpcc.git
fi

pushd sysbench-tpcc

./tpcc.lua \
  --db-driver="mysql" \
  --mysql-db="$DBNAME" \
  --mysql-host="$HOST" \
  --mysql-port="$PORT" \
  --mysql-user="$USER" \
  --mysql-password="$PASS" \
  --time=10 \
  --report_interval=1 \
  --threads=2 \
  --tables=1 \
  --scale=1 \
  --trx_level="RR" \
  cleanup


./tpcc.lua \
  --db-driver="mysql" \
  --mysql-db="$DBNAME" \
  --mysql-host="$HOST" \
  --mysql-port="$PORT" \
  --mysql-user="$USER" \
  --mysql-password="$PASS" \
  --time=10 \
  --report_interval=1 \
  --threads=2 \
  --tables=1 \
  --scale=1 \
  --trx_level="RR" \
  prepare

./tpcc.lua \
  --db-driver="mysql" \
  --mysql-db="$DBNAME" \
  --mysql-host="$HOST" \
  --mysql-port="$PORT" \
  --mysql-user="$USER" \
  --mysql-password="$PASS" \
  --time=10 \
  --report_interval=1 \
  --threads=2 \
  --tables=1 \
  --scale=1 \
  --trx_level="RR" \
  run


echo "benchmark TPC-C complete at $WORKING_DIR"
