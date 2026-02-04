#!/bin/bash

set -e
set -o pipefail

script_dir=$(dirname "$0")
cd $script_dir/../..

GO_BUILD_VERSION=1.25.6

if (( $# != 1 )); then
  echo "usage: build.sh linux-arm64|linux-amd64|darwin-arm64|darwin-amd64|windows-amd64"
  exit 2
fi

TUPLE=$1
shift

docker run --rm \
  -v `pwd`:/src \
  -e OS_ARCH_TUPLES="$TUPLE" \
  golang:"$GO_BUILD_VERSION"-trixie \
  /src/utils/publishrelease/buildindocker.sh
