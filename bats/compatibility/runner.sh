#!/bin/bash

function build_dolt() {
  pushd "$dolt_dir" > /dev/null || exit
  git checkout "$1" > /dev/null
  go install .
  popd > /dev/null || exit
}

function setup_dir() {
  if [ -d "$1" ]; then rm -r "$1"; fi
  mkdir "$1"
  pushd "$1" > /dev/null || exit
  "$top_dir/setup_repo.sh" > setup_repo.log
  cp -r "$top_dir"/bats/* .
  popd > /dev/null || exit
}

function run_bats_tests() {
  pushd "$1" > /dev/null || exit
  cwd=$(pwd)
  hash=$(git rev-parse HEAD)
  echo "testing dolt @ $hash against repo in $cwd"
  bats .
  echo
  popd > /dev/null || exit
}

# ensure that we have a clean working change set before we begin
if [[ $(git diff --stat) != '' ]]; then
  echo "cannot run compatibility tests with git working changes"
  exit
fi

# copy all the test files to take them out of source control
# when we checkout different Dolt releases we don't want to
# delete our environment
test_env="env_test"
rm -r $test_env
mkdir $test_env
cp -r test_files/* $test_env
pushd $test_env > /dev/null || exit

top_dir=$(pwd)
starting_branch=$(git rev-parse --abbrev-ref HEAD)
dolt_dir="../../../go/cmd/dolt/"

# setup a repository with dolt built
# from the current branch
build_dolt "$starting_branch"
setup_dir "head"

while IFS= read -r ver
do

  build_dolt "$ver"
  setup_dir "$ver"

  # run compatibility.bats to ensure dolt @ $ver can
  # read a repo created with dolt @ HEAD
  ver_hash=$(git rev-parse HEAD)
  echo "hash for dolt @ $ver: $ver_hash"
  run_bats_tests head

done < <(grep -v '^ *#' < dolt_versions.txt)

# now build dolt @ HEAD and make sure we can read
# all of the legacy repositories we created
build_dolt "$starting_branch"

while IFS= read -r ver
do
  head_hash=$(git rev-parse HEAD)
  echo "hash for dolt @ head: $head_hash"
  run_bats_tests "$ver"

done < <(grep -v '^ *#' < dolt_versions.txt)


popd > /dev/null || exit