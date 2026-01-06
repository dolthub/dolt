#!/bin/bash
# -*- mode: shell-script; indent-tabs-mode: nil; sh-basic-offset: 2; -*-

# Run this from within a golang docker container.
# Expects two env variables:
# * GO_BUILD_FLAGS - any extra flags to be passed to `go build`
# * OS_ARCH_TUPLES - the arch tuples to build releases for
#
# Expects the go.mod source root to be in /src.
# Will place the built binaries in /src/out.

set -e
set -o pipefail

KNOWN_OS_ARCH_TUPLES="darwin-amd64 darwin-arm64 windows-amd64 linux-amd64 linux-arm64"

if [[ -z "$OS_ARCH_TUPLES" ]]; then
  OS_ARCH_TUPLES="$KNOWN_OS_ARCH_TUPLES"
fi

for tuple in $OS_ARCH_TUPLES; do
    found=0
  for known in $KNOWN_OS_ARCH_TUPLES; do
    if [[ $tuple == $known ]]; then
      found=1
    fi
  done
  if (( found == 0 )); then
    echo "buildindocker.sh: Unknown OS_ARCH_TUPLE $tuple supplied. Known tuples: $KNOWN_OS_ARCH_TUPLES."
    exit 2
  fi
done

apt-get update && apt-get install -y p7zip-full pigz curl xz-utils mingw-w64 clang-19 rpm

cd /
curl -o optcross.tar.xz https://dolthub-tools.s3.us-west-2.amazonaws.com/optcross/"$(uname -m)"-linux_20250327_0.0.3_trixie.tar.xz
tar Jxf optcross.tar.xz
curl -o icustatic.tar.xz https://dolthub-tools.s3.us-west-2.amazonaws.com/icustatic/20250327_0.0.3_trixie.tar.xz
tar Jxf icustatic.tar.xz
export PATH=/opt/cross/bin:"$PATH"

cd /src

parsed=(`grep "Version = " ./cmd/dolt/doltversion/version.go`)
DOLT_VERSION=`eval echo ${parsed[2]}`

BINS="dolt"

declare -A platform_cc
platform_cc["linux-arm64"]="aarch64-linux-musl-gcc"
platform_cc["linux-amd64"]="x86_64-linux-musl-gcc"
platform_cc["darwin-arm64"]="clang-19 --target=aarch64-darwin --sysroot=/opt/cross/darwin-sysroot -mmacosx-version-min=12.0"
platform_cc["darwin-amd64"]="clang-19 --target=x86_64-darwin --sysroot=/opt/cross/darwin-sysroot -mmacosx-version-min=12.0"
platform_cc["windows-amd64"]="x86_64-w64-mingw32-gcc"

declare -A platform_cxx
platform_cxx["linux-arm64"]="aarch64-linux-musl-g++"
platform_cxx["linux-amd64"]="x86_64-linux-musl-g++"
platform_cxx["darwin-arm64"]="clang++-19 --target=aarch64-darwin --sysroot=/opt/cross/darwin-sysroot -mmacosx-version-min=12.0 --stdlib=libc++"
platform_cxx["darwin-amd64"]="clang++-19 --target=x86_64-darwin --sysroot=/opt/cross/darwin-sysroot -mmacosx-version-min=12.0 --stdlib=libc++"
platform_cxx["windows-amd64"]="x86_64-w64-mingw32-g++"

declare -A platform_as
platform_as["linux-arm64"]="aarch64-linux-musl-as"
platform_as["linux-amd64"]="x86_64-linux-musl-as"
platform_as["darwin-arm64"]="clang-19 --target=aarch64-darwin --sysroot=/opt/cross/darwin-sysroot -mmacosx-version-min=12.0"
platform_as["darwin-amd64"]="clang-19 --target=x86_64-darwin --sysroot=/opt/cross/darwin-sysroot -mmacosx-version-min=12.0"
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
platform_cgo_ldflags["windows-amd64"]="-static-libgcc -static-libstdc++"

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
      CXX="${platform_cxx[${tuple}]}" \
      AS="${platform_as[${tuple}]}" \
      CGO_LDFLAGS="${platform_cgo_ldflags[${tuple}]}" \
      go build \
        $GO_BUILD_FLAGS \
        -ldflags="${platform_go_ldflags[${tuple}]}" \
        -tags icu_static \
        -trimpath \
        -o "$o/bin/$obin" "./cmd/$bin/"
  done
  if [ "$os" = windows ]; then
    (cd out && 7z a "dolt-$os-$arch.zip" "dolt-$os-$arch" && 7z a "dolt-$os-$arch.7z" "dolt-$os-$arch")
  else
    tar cf - -C out "dolt-$os-$arch" | pigz -9 > "out/dolt-$os-$arch.tar.gz"
    if [ "$os" = linux ]; then
      rpmarch="x86_64"
      if [ "$arch" = "arm64" ]; then
        rpmarch="aarch64"
      fi
      (
        cd utils/rpmbuild/SOURCES;
        ln -sf ../../../out/"dolt-$os-$arch.tar.gz" .
        cd ..
        rpmbuild -bb --target ${rpmarch} --define "_topdir $(pwd)" --define "_prefix /usr/local" --define "DOLT_VERSION ${DOLT_VERSION}" --define "DOLT_ARCH ${arch}" SPECS/dolt.spec;
        mv RPMS/${rpmarch}/dolt*rpm ../../out
      )
    fi
  fi
done

render_install_sh() {
  sed 's|__DOLT_VERSION__|'"$DOLT_VERSION"'|' utils/publishrelease/install.sh
}

render_install_sh > out/install.sh
chmod 755 out/install.sh
