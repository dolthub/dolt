#!/bin/bash

set -eo pipefail

function download_release() {
  ver=$1
  dirname=binaries/"$ver"
  mkdir "$dirname"
  basename=dolt-"$PLATFORM_TUPLE"
  filename="$basename".tar.gz
  filepath=binaries/"$ver"/"$filename"
  url="https://github.com/liquidata-inc/dolt/releases/download/$ver/$filename"
  curl -L -o "$filepath" "$url"
  cd "$dirname" && tar zxf "$filename"
  echo "$dirname"/"$basename"/bin
}

get_platform_tuple() {
  OS=$(uname)
  ARCH=$(uname -m)
  if [ "$OS" != Linux -a "$OS" != Darwin ]; then
    echo "tests only support linux or macOS." 1>&2
    exit 1
  fi
  if [ "$ARCH" != x86_64 -a "$ARCH" != i386 -a "$ARCH" != i686 ]; then
    echo "tests only support x86_64 or x86." 1>&2
    exit 1
  fi
  if [ "$OS" == Linux ]; then
    PLATFORM_TUPLE=linux
  else
    PLATFORM_TUPLE=darwin
  fi
  if [ "$ARCH" == x86_64 ]; then
    PLATFORM_TUPLE="$PLATFORM_TUPLE"-amd64
  else
    PLATFORM_TUPLE="$PLATFORM_TUPLE"-386
  fi
  echo "$PLATFORM_TUPLE"
}

PLATFORM_TUPLE=`get_platform_tuple`

setup_test_repos() {
  ./setup_repo.sh "$1"
  mkdir "$1-remote"
  pushd "$1"
  dolt remote add origin "file://../$1-remote"
  # branches created in setup_repo.sh
  dolt push origin init
  dolt push origin master
  dolt push origin other
  popd
  dolt clone "file://$1-remote" "$1-clone"
}

TOP_DIR=`pwd`
function cleanup() {
  pushd $TOP_DIR
  rm -rf binaries
  rm -rf repo*
  popd
}
mkdir binaries
trap cleanup "EXIT"

bin=`download_release "v0.15.2"`
PATH="`pwd`"/"$bin":"$PATH" setup_test_repos "repo"
TEST_REPO="repo" bats migrate.bats
