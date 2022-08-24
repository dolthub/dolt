#!/bin/bash

set -eo pipefail

script_dir=$(dirname "$0")
cd $script_dir/..

( go list -deps -json ./cmd/dolt/. && \
  GOOS=windows go list -deps -json ./cmd/dolt/. ) \
  | go run ./utils/3pdeps/. -verify ./Godeps/LICENSES
