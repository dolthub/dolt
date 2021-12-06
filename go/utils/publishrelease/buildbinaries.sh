#!/bin/bash

set -e
set -o pipefail

script_dir=$(dirname "$0")
cd $script_dir/../..

[ ! -z "$GO_BUILD_VERSION" ] || (echo "Must supply GO_BUILD_VERSION"; exit 1)

docker run --rm -v `pwd`:/src golang:"$GO_BUILD_VERSION"-buster /bin/bash -c '
set -e
set -o pipefail
apt-get update && apt-get install -y zip
cd /src

BINS="dolt git-dolt git-dolt-smudge"
OS_ARCH_TUPLES="windows-amd64 linux-amd64 darwin-amd64 darwin-arm64"

for tuple in $OS_ARCH_TUPLES; do
  os=`echo $tuple | sed 's/-.*//'`
  arch=`echo $tuple | sed 's/.*-//'`
  o="out/dolt-$os-$arch"
  mkdir -p "$o/bin"
  cp Godeps/LICENSES "$o/"
  for bin in $BINS; do
    echo Building "$o/$bin"
    obin="$bin"
    if [ "$os" = windows ]; then
      obin="$bin.exe"
    fi
    CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -o "$o/bin/$obin" "./cmd/$bin/"
  done
  if [ "$os" = windows ]; then
    (cd out && zip -r "dolt-$os-$arch" "dolt-$os-$arch")
  else
    tar czf "out/dolt-$os-$arch.tar.gz" -C out "dolt-$os-$arch"
  fi
done

render_install_sh() {
  local parsed=(`grep "Version = " ./cmd/dolt/dolt.go`)
  local DOLT_VERSION=`eval echo ${parsed[2]}`
  sed '\''s|__DOLT_VERSION__|'\''"$DOLT_VERSION"'\''|'\'' utils/publishrelease/install.sh
}

render_install_sh > out/install.sh
chmod 755 out/install.sh
'
