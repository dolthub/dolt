#!/bin/bash

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

# jenkins-build.sh [GOOS] [GOARCH]
# GOOS - darwin, linux
# GOARCH - amd64, 386
# see https://golang.org/doc/install/source#environment for more options
# This script assumes go, git, tar are in your $PATH already
# If successful, a tar.gz file named $GOOS-$GOARCH.tar.gz is generated.

set -euo pipefail

# default to building for Mac OS X
GOOS=${1:-darwin}
GOARCH=${2:-amd64}

NOMS_GIT_REV=`git describe --always`
NOMS_GIT_REPO="github.com/attic-labs/noms"

BINARIES_TO_BUILD=( \
	"cmd/noms" \
	"cmd/noms-ui" \
	"samples/go/blob-get" \
	"samples/go/counter" \
	"samples/go/csv/csv-import" \
	"samples/go/csv/csv-export" \
	"samples/go/hr" \
	"samples/go/json-import" \
	"samples/go/nomsfs" \
	"samples/go/url-fetch" \
	"samples/go/xml-import"
)

goTest () {
	echo Running tests...
	go test ${NOMS_GIT_REPO}/cmd/... ${NOMS_GIT_REPO}/samples/go/... ${NOMS_GIT_REPO}/go/...
}

# execute `go build` with arg for package ($1), inserting the git sha1 into the binary
goBuild () {
	echo Building ${NOMS_GIT_REPO}/$1...
    GOOS=${GOOS} GOARCH=${GOARCH} go build \
  		-ldflags "-X github.com/attic-labs/noms/go/constants.NomsGitSHA=${NOMS_GIT_REV}" \
  		${NOMS_GIT_REPO}/$1
}

echoBuildEnv () {
	date
	uname -a
	go version
	echo Building from $NOMS_GIT_REV for $GOOS/$GOARCH...
}

# remove previous build if present
rm -rf $GOOS-$GOARCH.tar.gz

echobuildenv

# run tests, only build binaries if test succeeds
gotest

# perform the actual builds
binaries=""
for bin in "${BINARIES_TO_BUILD[@]}"
do
	gobuild $bin
	binaries+=" "
	binaries+=`basename $bin`
done

# bundle the built files
tar czvf $GOOS-$GOARCH.tar.gz ${binaries}
rm ${binaries}
