#!/bin/bash

set -eo pipefail

PLATFORM_TUPLE=""
DEFAULT_BRANCH=""

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

function cleanup() {
  rm -rf repos binaries
}

function setup_repo() {
  dir=repos/"$1"
  unset DEFAULT_BRANCH
  ./test_files/setup_repo.sh "$dir"
  DEFAULT_BRANCH=$(cat "$dir/default_branch.var")
}

function list_backward_compatible_versions() {
  grep -v '^ *#' < test_files/backward_compatible_versions.txt
}

function test_backward_compatibility() {
  ver=$1
  bin=`download_release "$ver"`

  # create a Dolt repository using version "$ver"
  PATH="`pwd`"/"$bin":"$PATH" setup_repo "$ver"

  echo "Run the bats tests with current Dolt version hitting repositories from older Dolt version $ver"
  DOLT_LEGACY_BIN="$(pwd)/$bin/dolt" DEFAULT_BRANCH="$DEFAULT_BRANCH" REPO_DIR="$(pwd)/repos/$ver" DOLT_VERSION="$ver" bats ./test_files/bats
}

function list_forward_compatible_versions() {
  grep -v '^ *#' < test_files/forward_compatible_versions.txt
}

function test_forward_compatibility() {
  ver=$1
  bin=`download_release "$ver"`

  echo "Run the bats tests using older Dolt version $ver hitting repositories from the current Dolt version"
  
  # Push this repo to a file remote in preparation to clone it. This
  # prunes out certain aspects of the storage (certain refs) that may
  # not be compatible with older versions.
  if [ ! -d repos/HEAD/file-remote ]
  then
      cd repos/HEAD
      mkdir file-remote
      dolt remote add file-remote file://file-remote
      dolt push file-remote "$DEFAULT_BRANCH"
      dolt push file-remote init
      dolt push file-remote no-data
      dolt push file-remote other
      dolt push file-remote check_merge
      cd ../../
  fi
  REMOTE="`pwd`"/repos/HEAD/file-remote

  # Clone from the remote and establish local branches

  if [ -d "repos/$ver" ]
  then
      rm -rf "repos/$ver"
  fi
  
  cd repos
  # Make sure these clone and setup commands are run with the version of dolt under test
  relpath="`pwd`"/../"$bin":"$PATH"
  echo "cloning current dolt repo with " `PATH=$relpath dolt version`
  echo PATH="$relpath" dolt clone "file://$REMOTE" $ver
  PATH="$relpath" dolt clone "file://$REMOTE" $ver
  cd $ver
  PATH="$relpath" dolt branch no-data origin/no-data
  PATH="$relpath" dolt branch init origin/init
  PATH="$relpath" dolt branch other origin/other
  PATH="$relpath" dolt branch check_merge origin/check_merge
  # Also copy the files exported by setup_repo
  cp ../../repos/HEAD/*.csv ./
  cp ../../repos/HEAD/*.json ./
  cd ../../

  # Run the bats tests
  PATH="`pwd`"/"$bin":"$PATH" dolt version
  echo "Run the bats tests with older Dolt version $ver hitting repositories from the current Dolt version"
  PATH="`pwd`"/"$bin":"$PATH" REPO_DIR="`pwd`"/repos/$ver DOLT_CLIENT_BIN="`pwd`"/"$bin"/dolt bats ./test_files/bats
}

_main() {
  PLATFORM_TUPLE=`get_platform_tuple`

  # make directories and cleanup when killed
  mkdir repos binaries
  trap cleanup "EXIT"

  # test backward compatibility
  list_backward_compatible_versions | while IFS= read -r ver; do
    test_backward_compatibility "$ver"
  done

  # setup repo for current dolt version
  setup_repo HEAD

  # test forward compatibility
  if [ -s "test_files/forward_compatible_versions.txt" ]; then
      list_forward_compatible_versions | while IFS= read -r ver; do
        test_forward_compatibility "$ver"
      done
  fi

  # sanity check: run tests against current version
  echo "Run the bats tests using current Dolt version hitting repositories from the current Dolt version"
  DEFAULT_BRANCH="$DEFAULT_BRANCH" REPO_DIR="$(pwd)/repos/HEAD" bats ./test_files/bats
}

_main
