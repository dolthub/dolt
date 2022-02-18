#!/bin/sh
dolt version
# Github Actions ignores the WORKDIR?
cd ./go/performance/utils/sysbench_runner/cmd
DEBUG=1 go run . --config=/config.json

status_code=$?

if [ $status_code -ne 0 ]
then
  exit 1
fi

cd /dolt/go/performance/utils/tpcc_runner/cmd
DEBUG=1 go run . --config=/tpcc-config.json
