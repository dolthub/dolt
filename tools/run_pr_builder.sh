#!/bin/bash

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

# This script runs on the Noms PR Builder (http://jenkins3.noms.io/job/NomsPRBuilder).

set -eux

export GOPATH=${WORKSPACE}
NOMS_DIR=${WORKSPACE}/src/github.com/attic-labs/noms

go version

# go list is expensive, only do it once.
GO_LIST="$(go list ./... | grep -v /vendor/ | grep -v /samples/js/)"
go build ${GO_LIST}
go test ${GO_LIST}

python -m unittest discover -p "*_test.py" -s $GOPATH/src/github.com/attic-labs/noms/tools
