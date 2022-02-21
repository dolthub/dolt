#!/bin/bash

set -e
set -o pipefail

dolt version
# Github Actions ignores the WORKDIR?
cd ./go/performance/utils/sysbench_runner/cmd
DEBUG=1 go run . --config=/config.json

cd /dolt/go/performance/utils/tpcc_runner/cmd
DEBUG=1 go run . --config=/tpcc-config.json
