#!/bin/bash

set -eo pipefail

script_dir=$(dirname "$0")
cd "$script_dir"

exec go run . $BRANCH_NAME $CHANGE_TARGET
