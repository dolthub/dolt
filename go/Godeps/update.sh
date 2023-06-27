#!/bin/bash

set -eo pipefail

script_dir=$(dirname "$0")
cd $script_dir/..

( go list -deps -json -tags +amd64,+arm64,+linux,+windows,+darwin,+386 ./cmd/dolt/. ) \
  | go run ./utils/3pdeps/. > ./Godeps/LICENSES
