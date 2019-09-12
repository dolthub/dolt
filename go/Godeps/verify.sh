#!/bin/bash

set -eo pipefail

script_dir=$(dirname "$0")
cd $script_dir/..

( go list -deps -json ./cmd/dolt/. ./cmd/git-dolt/. ./cmd/git-dolt-smudge/. && \
  GOOS=windows go list -deps -json ./cmd/dolt/. ./cmd/git-dolt/. ./cmd/git-dolt-smudge/.) \
  | go run ./utils/3pdeps/. -verify ./Godeps/LICENSES
