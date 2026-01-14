#!/bin/bash

set -eo pipefail

script_dir=$(dirname "$0")
cd $script_dir/..

( GOOS=linux GOARCH=amd64 go list -deps -json ./cmd/dolt/. &&
  GOOS=linux GOARCH=386 go list -deps -json ./cmd/dolt/. &&
  GOOS=linux GOARCH=arm64 go list -deps -json ./cmd/dolt/. &&
  GOOS=windows GOARCH=amd64 go list -deps -json ./cmd/dolt/. &&
  GOOS=windows GOARCH=arm64 go list -deps -json ./cmd/dolt/. &&
  GOOS=darwin GOARCH=amd64 go list -deps -json ./cmd/dolt/. &&
  GOOS=darwin GOARCH=arm64 go list -deps -json ./cmd/dolt/.
) \
  | go run ./utils/3pdeps/. -verify ./Godeps/LICENSES
