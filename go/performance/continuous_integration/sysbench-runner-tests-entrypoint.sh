#!/bin/bash

set -e
set -o pipefail

dolt version
# Github Actions ignores the WORKDIR?
cd ./go/performance/utils/benchmark_runner
DEBUG=true RUN_BENCHMARK_RUNNER_TESTS=true go test -timeout 15m ./...
