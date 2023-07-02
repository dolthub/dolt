#!/bin/bash

set -eo pipefail

script_dir=$(dirname "$0")
cd $script_dir/..

( go list -deps -json -tags +arm64,+amd64,+windows,+linux,+darwin,+386 ./cmd/dolt/. ) \
  | go run ./utils/3pdeps/. -verify ./Godeps/LICENSES
