#!/bin/bash

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

# jenkins-build.sh [GOOS-GOARCH] [GOOS-GOARCH] ...
# e.g. darwin-amd64, linux-amd64 darwin-386, etc.
# see https://golang.org/doc/install/source#environment for more options
# This script assumes go, git, tar are in your $PATH already
# If successful, tar.gz files named $GOOS-$GOARCH.tar.gz are generated.

set -euo pipefail

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

# execute `go build` for binary, inserting the git sha1 into the binary
# args: GOOS ($1), GOARCH ($2), package ($3)
goBuild () {
	echo Building ${NOMS_GIT_REPO}/$3 for $1-$2...
    GOOS=$1 GOARCH=$2 go build \
  		-ldflags "-X github.com/attic-labs/noms/go/constants.NomsGitSHA=${NOMS_GIT_REV}" \
  		${NOMS_GIT_REPO}/$3
}

# execute `goBuild` for binaries to be built
# args: GOOS ($1) and GOARCH ($2)
# creates $GOOS-$GOARCH.tar.gz of binaries if successful
goBuildPlatform () {
	rm -rf $1-$2.tar.gz
	binaries=""
	for bin in "${BINARIES_TO_BUILD[@]}"
	do
		goBuild $1 $2 $bin
		binaries+=" "
		binaries+=`basename $bin`
	done

	# bundle the built files
	tar czvf $1-$2.tar.gz ${binaries}
	rm ${binaries}
}

echoBuildEnv () {
	date
	uname -a
	go version
	echo Building from $NOMS_GIT_REV...
}

echoBuildEnv
goTest

# default to building for Mac OS X
BUILD_TARGETS=(darwin-amd64)
if [ $# -gt 0 ] ; then
	BUILD_TARGETS=( "$@" )
fi

GOOS_GOARCH=""
for target in "${BUILD_TARGETS[@]}" ; do
	# split build target from GOOS-GOARCH into GOOS and GOARCH
	IFS='-' read -ra GOOS_GOARCH <<< "$target"
	goBuildPlatform "${GOOS_GOARCH[0]}" "${GOOS_GOARCH[1]}"
done
