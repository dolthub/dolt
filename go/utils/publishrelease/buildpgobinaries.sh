#!/bin/bash

set -e
set -o pipefail

script_dir=$(dirname "$0")
cd $script_dir/../..

[ ! -z "$GO_BUILD_VERSION" ] || (echo "Must supply GO_BUILD_VERSION"; exit 1)
[ ! -z "$PROFILE" ] || (echo "Must supply PROFILE"; exit 1)

docker run --rm -v `pwd`:/src -v "$PROFILE":/cpu.pprof golang:"$GO_BUILD_VERSION"-bookworm /bin/bash -c '
set -e
set -o pipefail
apt-get update && apt-get install -y p7zip-full pigz
cd /src

BINS="dolt"
OS_ARCH_TUPLES="windows-amd64 linux-amd64 linux-arm64 darwin-amd64 darwin-arm64"

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
    CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" go build -pgo=/cpu.pprof -trimpath -ldflags="-s -w" -o "$o/bin/$obin" "./cmd/$bin/"
  done
  if [ "$os" = windows ]; then
    (cd out && 7z a "dolt-$os-$arch.zip" "dolt-$os-$arch" && 7z a "dolt-$os-$arch.7z" "dolt-$os-$arch")
  else
    tar cf - -C out "dolt-$os-$arch" | pigz -9 > "out/dolt-$os-$arch.tar.gz"
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
