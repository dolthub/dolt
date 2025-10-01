#!/bin/bash

set -e
set -o pipefail

script_dir=$(dirname "$0")
cd $script_dir/../..

[ ! -z "$GO_BUILD_VERSION" ] || (echo "Must supply GO_BUILD_VERSION"; exit 1)
[ ! -z "$PROFILE" ] || (echo "Must supply PROFILE"; exit 1)

docker run --rm \
       -v `pwd`:/src \
       -v "$PROFILE":/cpu.pprof \
       -e GO_BUILD_FLAGS='-pgo=/cpu.pprof' \
       golang:"$GO_BUILD_VERSION"-trixie \
       /src/utils/publishrelease/buildindocker.sh
