#!/bin/bash

set -e
set -o pipefail

script_dir=$(dirname "$0")
cd $script_dir/../..

BINS="dolt git-dolt git-dolt-smudge"
OSES="windows linux darwin"
ARCHS="amd64"

for os in $OSES; do
  for arch in $ARCHS; do
    o="out/dolt-$os-$arch"
    mkdir -p "$o/bin"
    cp Godeps/LICENSES "$o/"
    for bin in $BINS; do
      echo Building "$o/$bin"
      obin="$bin"
      if [ "$os" = windows ]; then
        obin="$bin.exe"
      fi
      GOOS="$os" GOARCH="$arch" go build -o "$o/bin/$obin" "./cmd/$bin/"
    done
    if [ "$os" = windows ]; then
      (cd out && zip -r "dolt-$os-$arch" "dolt-$os-$arch")
    else
      tar czf "out/dolt-$os-$arch.tar.gz" -C out "dolt-$os-$arch"
    fi
  done
done

render_install_sh() {
  local parsed=(`grep "Version = " ./cmd/dolt/dolt.go`)
  local DOLT_VERSION=`eval echo ${parsed[2]}`
  sed 's|__DOLT_VERSION__|'"$DOLT_VERSION"'|' utils/publishrelease/install.sh
}

render_install_sh > out/install.sh
chmod 755 out/install.sh
