#!/bin/bash

set -eo pipefail

script_dir=$(dirname "$0")

exec go run . $GIT_COMMIT $CHANGE_TARGET
