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
  pushd "$dolt_dir" || exit
  git checkout "$1"
  echo "installing dolt @ $1"
  go install .
  popd || exit
}

function setup_dir() {
  echo "creating repo with dolt @ $1"
  if [ -d "$1" ]; then rm -r "$1"; fi
  mkdir "$1"
  pushd "$1" || exit
  "$top_dir"/setup_repo.sh
  cp -r "$top_dir"/bats/* .
  ls -l *bats
  popd || exit
}

function run_bats_tests() {
  pushd "$1" || exit
  bats .
  popd || exit
}

setup_dir "head"

while IFS= read -r ver
do

  build_dolt "$ver"
  setup_dir "$ver"

  # ensure we can read the repo
  # create with dolt @ head
  run_bats_tests head

done < <(grep -v '^ *#' < dolt_versions.txt)

# now build dolt @ head and make sure we can read
# all of the legacy repositories we created
build_dolt "$starting_branch"

while IFS= read -r ver
do

  run_bats_tests "$ver"

done < <(grep -v '^ *#' < dolt_versions.txt)
