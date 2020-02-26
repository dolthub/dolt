#!/bin/bash


if [[ $(git diff --stat) != '' ]]; then
  echo "cannot run compatibility test with git working changes"
  exit
fi

dolt_dir="../../../go/cmd/dolt/"

starting_branch=$(git rev-parse --abbrev-ref HEAD)
top_dir=$(pwd)

function build_dolt() {
  # go back to initial branch
  pushd "$dolt_dir" > /dev/null || exit
  git checkout "$1" > /dev/null
  go install .
  popd > /dev/null || exit
}

function setup_dir() {
  if [ -d "$1" ]; then rm -r "$1"; fi
  mkdir "$1"
  pushd "$1" > /dev/null || exit
  "$top_dir"/setup_repo.sh > setup_repo.log
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

setup_dir "head"

while IFS= read -r ver
do

  build_dolt "$ver"
  setup_dir "$ver"

  # ensure we can read the repo
  # create with dolt @ head
  ver_hash=$(git rev-parse HEAD)
  echo "hash for dolt @ $ver: $ver_hash"
  run_bats_tests head

done < <(grep -v '^ *#' < dolt_versions.txt)

# now build dolt @ head and make sure we can read
# all of the legacy repositories we created
build_dolt "$starting_branch"

while IFS= read -r ver
do
  head_hash=$(git rev-parse HEAD)
  echo "hash for dolt @ head: $head_hash"
  run_bats_tests "$ver"

done < <(grep -v '^ *#' < dolt_versions.txt)
