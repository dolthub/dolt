#!/bin/bash

SRC="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."
TMP=$(mktemp /tmp/badger-coverage-XXXXX.txt)

BUILD=$1
OUT=$2

set -e

pushd $SRC &> /dev/null

# create coverage output
echo 'mode: atomic' > $OUT
for PKG in $(go list ./...|grep -v -E 'vendor'); do
  go test -covermode=atomic -coverprofile=$TMP $PKG
  tail -n +2 $TMP >> $OUT
done

popd &> /dev/null
