#!/bin/sh
dolt version
# Github Actions ignores the WORKDIR?
cd ./go/performance/utils/sysbench_runner/cmd
DEBUG=1 go run . --config=/config.json
