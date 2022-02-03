#!/bin/sh
dolt version
ls -a .
# Github Actions ignores the WORKDIR?
dolt config --global --add user.email "you@example.com"
dolt config --global --add user.name "Your Name"
#cd ./go/performance/utils/tpcc_runner/cmd
DEBUG=1 go run . --config=/tpcc-config.json
