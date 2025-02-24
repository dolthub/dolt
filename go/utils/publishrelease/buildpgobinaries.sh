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
apt-get update && apt-get install -y p7zip-full pigz curl xz-utils mingw-w64 clang-15

cd /
curl -o optcross.tar.xz https://dolthub-tools.s3.us-west-2.amazonaws.com/optcross/"$(uname -m)"-linux_20240515_0.0.2.tar.xz
tar Jxf optcross.tar.xz
export PATH=/opt/cross/bin:"$PATH"

cd /src

BINS="dolt"
OS_ARCH_TUPLES="darwin-amd64 darwin-arm64 windows-amd64 linux-amd64 linux-arm64"

declare -A platform_cc
platform_cc["linux-arm64"]="aarch64-linux-musl-gcc"
platform_cc["linux-amd64"]="x86_64-linux-musl-gcc"
platform_cc["darwin-arm64"]="clang-15 --target=aarch64-darwin --sysroot=/opt/cross/darwin-sysroot -mmacosx-version-min=12.0"
platform_cc["darwin-amd64"]="clang-15 --target=x86_64-darwin --sysroot=/opt/cross/darwin-sysroot -mmacosx-version-min=12.0"
platform_cc["windows-amd64"]="x86_64-w64-mingw32-gcc"

declare -A platform_as
platform_as["linux-arm64"]="aarch64-linux-musl-as"
platform_as["linux-amd64"]="x86_64-linux-musl-as"
platform_as["darwin-arm64"]="clang-15 --target=aarch64-darwin --sysroot=/opt/cross/darwin-sysroot -mmacosx-version-min=12.0"
platform_as["darwin-amd64"]="clang-15 --target=x86_64-darwin --sysroot=/opt/cross/darwin-sysroot -mmacosx-version-min=12.0"
platform_as["windows-amd64"]="x86_64-w64-mingw32-as"

# Note: the extldflags below for the MacOS builds specify an SDK version of 14.4
# This corresponds to our currently installed toolchain, but should change if the
# toolchain changes.
declare -A platform_go_ldflags
platform_go_ldflags["linux-arm64"]="-s -w"
platform_go_ldflags["linux-amd64"]="-s -w"
platform_go_ldflags["darwin-arm64"]="-s -w -compressdwarf=false -extldflags -Wl,-platform_version,macos,12.0,14.4"
platform_go_ldflags["darwin-amd64"]="-s -w -compressdwarf=false -extldflags -Wl,-platform_version,macos,12.0,14.4"
platform_go_ldflags["windows-amd64"]="-s -w"

declare -A platform_cgo_ldflags
platform_cgo_ldflags["linux-arm64"]="-static -s"
platform_cgo_ldflags["linux-amd64"]="-static -s"
platform_cgo_ldflags["darwin-arm64"]=""
platform_cgo_ldflags["darwin-amd64"]=""
platform_cgo_ldflags["windows-amd64"]=""

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
    CGO_ENABLED=1 \
      GOOS="$os" \
      GOARCH="$arch" \
      CC="${platform_cc[${tuple}]}" \
      AS="${platform_as[${tuple}]}" \
      CGO_LDFLAGS="${platform_cgo_ldflags[${tuple}]}" \
      go build \
        -pgo=/cpu.pprof \
        -ldflags="${platform_go_ldflags[${tuple}]}" \
        -trimpath \
        -o "$o/bin/$obin" "./cmd/$bin/"
  done
  if [ "$os" = windows ]; then
    (cd out && 7z a "dolt-$os-$arch.zip" "dolt-$os-$arch" && 7z a "dolt-$os-$arch.7z" "dolt-$os-$arch")
  else
    tar cf - -C out "dolt-$os-$arch" | pigz -9 > "out/dolt-$os-$arch.tar.gz"
  fi
done

render_install_sh() {
  local parsed=(`grep "Version = " ./cmd/dolt/doltversion/version.go`)
  local DOLT_VERSION=`eval echo ${parsed[2]}`
  sed '\''s|__DOLT_VERSION__|'\''"$DOLT_VERSION"'\''|'\'' utils/publishrelease/install.sh
}

render_install_sh > out/install.sh
chmod 755 out/install.sh
'
