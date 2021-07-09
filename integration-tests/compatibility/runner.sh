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

function cleanup() {
  rm -rf repos binaries
}
mkdir repos binaries
trap cleanup "EXIT"

function setup_repo() {
  dir=repos/"$1"
  ./test_files/setup_repo.sh "$dir"
}


#
#   Backward Compatibility
#

function list_backward_compatible_versions() {
  grep -v '^ *#' < test_files/backward_compatible_versions.txt
}

function test_backward_compatibility() {
  ver=$1
  bin=`download_release "$ver"`

  # create a Dolt repository using version "$ver"
  PATH="`pwd`"/"$bin":"$PATH" setup_repo "$ver"

  echo "Run the bats tests with current Dolt version hitting repositories from older Dolt version $ver"
  REPO_DIR="`pwd`"/repos/"$ver" bats ./test_files/bats
}

list_backward_compatible_versions | while IFS= read -r ver; do
  test_backward_compatibility "$ver"
done


#
#   Forward Compatibility
#

setup_repo HEAD

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
      dolt push file-remote master
      dolt push file-remote init
      dolt push file-remote no-data
      dolt push file-remote other
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
  # Also copy the files exported by setup_repo
  cp ../../repos/HEAD/*.csv ./
  cp ../../repos/HEAD/*.json ./
  cd ../../

  # Run the bats tests
  PATH="`pwd`"/"$bin":"$PATH" dolt version
  echo PATH="`pwd`"/"$bin":"$PATH" REPO_DIR="`pwd`"/repos/$ver bats ./test_files/bats
  PATH="`pwd`"/"$bin":"$PATH" REPO_DIR="`pwd`"/repos/$ver bats ./test_files/bats
}

if [ -s "test_files/forward_compatible_versions.txt" ]; then
    list_forward_compatible_versions | while IFS= read -r ver; do
      test_forward_compatibility "$ver"
    done
fi

# sanity check
echo "Run the bats tests using current Dolt version hitting repositories from the current Dolt version"
REPO_DIR="`pwd`"/repos/HEAD bats ./test_files/bats
