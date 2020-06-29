#!/bin/bash

set -eo pipefail

script_dir=$(dirname "$0")
cd $script_dir/../..

target="master"
if [[ $# -eq 1 ]]; then
    target="$1"
fi

# Keep this in sync with .github/workflows/ci-check-repo.yaml contents that
# are easy to evaluate locally and might commonly fail.

GOFLAGS="-mod=readonly" go build ./...
./utils/repofmt/check_fmt.sh
./Godeps/verify.sh
go run ./utils/checkcommitters -dir "$target"
go vet -mod=readonly ./...
go run -mod=readonly ./utils/copyrightshdrs/
