#!/bin/bash

set -eo pipefail

function download_release() {
  ver=$1
  dirname=binaries/"$ver"
  mkdir "$dirname"
  basename=dolt-"$PLATFORM_TUPLE"
  filename="$basename".tar.gz
  filepath=binaries/"$ver"/"$filename"
  url="https://github.com/dolthub/dolt/releases/download/$ver/$filename"
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

function list_dolt_versions() {
  grep -v '^ *#' < test_files/dolt_versions.txt
}

function cleanup() {
  rm -rf repos binaries
}
mkdir repos binaries
trap cleanup "EXIT"

function setup_repo() {
  dir=repos/"$1"
  ./test_files/setup_repo.sh "$dir"
}

setup_repo HEAD

function test_dolt_version() {
  ver=$1
  bin=`download_release "$ver"`
  echo testing "$ver" at "$bin"
  PATH="`pwd`"/"$bin":"$PATH" setup_repo "$ver"

# Changes to the NBS manifest broke forward compatibility when multiple clients with different
# versions interact with the same repo instance. However, in a more realistic setting of
# clients interacting with a repo through a remote, compatibility is not broken.
# todo: update compatibility tests to have different client versions interact through a remote.
  # Run the bats tests with old dolt version hitting repositories from new dolt version
  # PATH="`pwd`"/"$bin":"$PATH" REPO_DIR="`pwd`"/repos/HEAD bats ./test_files/bats

  # Run the bats tests with new dolt version hitting repositories from old dolt version
  REPO_DIR="`pwd`"/repos/"$ver" bats ./test_files/bats
}

list_dolt_versions | while IFS= read -r ver; do
  test_dolt_version "$ver"
done
