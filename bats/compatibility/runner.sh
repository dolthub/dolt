#!/bin/bash

function build_dolt() {
  pushd "$DOLT_DIR" > /dev/null || exit
  git checkout "$1" > /dev/null
  go install .
  popd > /dev/null || exit
}

function setup_dir() {
  if [ -d "$1" ]; then rm -r "$1"; fi
  mkdir "$1"
  pushd "$1" > /dev/null || exit

  cp -r "$TOP_DIR"/bats/* .

  mkdir "repo"
  pushd "repo" > /dev/null || exit
  "$TOP_DIR/setup_repo.sh" > setup_repo.log
  popd > /dev/null || exit

  num_tests=$(($(grep -c '@test' *.bats)))
  for ((i=1;i<=$num_tests;i++))
  do
    cp -r repo/ "test$i"
  done

  popd > /dev/null || exit
}

function run_bats_tests() {
  pushd "$1" > /dev/null || exit
  bats_out=$(bats .)
  STATUS=$?
  echo "$bats_out" | tr -cd "[:print:]\n" | sed 's/\[3J\[H\[2J//'
  popd > /dev/null || exit
  return $STATUS
}

assert_linux_or_macos() {
  OS=$(uname)
  ARCH=$(uname -m)
  if [ "$OS" != Linux -a "$OS" != Darwin ]; then
    fail "E_UNSUPPORTED_OS" "dolt install.sh only supports macOS and Linux."
  fi
  if [ "$ARCH" != x86_64 -a "$ARCH" != i386 -a "$ARCH" != i686 ]; then
    fail "E_UNSUPPOSED_ARCH" "dolt install.sh only supports installing dolt on x86_64 or x86."
  fi

  if [ "$OS" == Linux ]; then
    PLATFORM_TUPLE=linux
  else
    PLATFORM_TUPLE=darwin
  fi
  if [ "$ARCH" == x86_64 ]; then
    PLATFORM_TUPLE=$PLATFORM_TUPLE-amd64
  else
    PLATFORM_TUPLE=$PLATFORM_TUPLE-386
  fi
  echo "platform: $PLATFORM_TUPLE"
}

function download_and_install_release() {
  curl -L $1 > f
  tar zxf f
  if [[ -z "${CI_BIN}" ]]; then
    install "dolt-$PLATFORM_TUPLE/bin/dolt" "$CI_BIN"
  else
    [ -d /usr/local/bin ] || install -o 0 -g 0 -d /usr/local/bin
    sudo install -o 0 -g 0 "dolt-$PLATFORM_TUPLE/bin/dolt /usr/local/bin"
  fi
}

# ensure that we have a clean working change set before we begin
if [[ $(git diff --stat) != '' ]]; then
  echo "cannot run compatibility tests with git working changes"
  exit 1
fi

# copy all the test files to create test_env
TEST_ENV="env_test"
rm -r $TEST_ENV
mkdir $TEST_ENV
cp -r test_files/* $TEST_ENV
pushd $TEST_ENV > /dev/null || exit

TOP_DIR=$(pwd)
STARTING_BRANCH=$(git rev-parse --abbrev-ref HEAD)
DOLT_DIR="../../../go/cmd/dolt/"
STATUS=0

# Set the PLATFORM_TUPLE var
assert_linux_or_macos

# for each legacy version, setup a repository
# using dolt built from the current branch
build_dolt "$STARTING_BRANCH"
while IFS= read -r VER
do
  setup_dir "$VER-forward_compat"
done < <(grep -v '^ *#' < dolt_versions.txt)


while IFS= read -r VER
do
  download_and_install_release "https://github.com/liquidata-inc/dolt/releases/download/$VER/dolt-$PLATFORM_TUPLE.tar.gz"
  setup_dir "$VER-backward_compat"

  # run compatibility.bats to ensure dolt @ $VER can
  # read a repo created with dolt @ HEAD
  echo
  echo "testing dolt @ $(dolt version) against repo in $VER-forward_compat/"
  run_bats_tests "$VER-forward_compat"
  STATUS=$((STATUS+$?))
  echo
done < <(grep -v '^ *#' < dolt_versions.txt)

# now build dolt @ HEAD and make sure we can read
# all of the legacy repositories we created
build_dolt "$STARTING_BRANCH"

while IFS= read -r VER
do
  echo
  echo "testing dolt @ $(git rev-parse --abbrev-ref HEAD) against repo in $VER-backward_compat/"
  run_bats_tests "$VER-backward_compat"
  STATUS=$((STATUS+$?))
  echo
done < <(grep -v '^ *#' < dolt_versions.txt)

popd > /dev/null || exit

exit $STATUS
