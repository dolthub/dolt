#!/bin/bash

set -eo pipefail

script_dir=$(dirname "$0")
cd "$script_dir"

exec go run . $GIT_COMMIT $CHANGE_TARGET
