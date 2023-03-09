#!/bin/sh
source ../helpers.bash

echo "Running $1 tests"
setup_dolt_repo
cd ..
node $1 $USER $PORT $REPO_NAME
teardown_dolt_repo