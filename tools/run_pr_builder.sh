#!/bin/bash

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

# This script runs on the Noms PR Builder (http://jenkins3.noms.io/job/NomsPRBuilder).

set -eux

export GOPATH=${WORKSPACE}
NOMS_DIR=${WORKSPACE}/src/github.com/attic-labs/noms

go version
node --version

# go list is expensive, only do it once.
GO_LIST="$(go list ./... | grep -v /vendor/ | grep -v /samples/js/)"
go build ${GO_LIST}

# go test plus build coverage data for upload codecov.io
rm -rf coverage.txt
touch coverage.txt
for d in ${GO_LIST}; do
    go test -coverprofile=profile.out -covermode=atomic $d
    if [ -f profile.out ]; then
        cat profile.out >> coverage.txt
        rm profile.out
    fi
done

echo "== JS tests"
pushd ${NOMS_DIR}
python tools/run-all-js-tests.py
popd
echo "== JS tests done"

# The integration test only works after the node tests because the node tests sets up samples/js/node_modules
echo "== Integration tests"
pushd ${NOMS_DIR}/samples/js
go test -v ./...
popd
echo "== Integration tests done"

echo "== Python tests"
python -m unittest discover -p "*_test.py" -s $GOPATH/src/github.com/attic-labs/noms/tools
echo "== Python tests done"

echo "== Codecov upload"
bash <(curl -s https://codecov.io/bash) -t ${COVERALLS_TOKEN}
echo "== Codecov upload done"
