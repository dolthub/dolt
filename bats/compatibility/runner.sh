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

  cp -r "$top_dir"/bats/* .

  mkdir "repo"
  pushd "repo" > /dev/null || exit
  "$top_dir/setup_repo.sh" > setup_repo.log
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
  status=$?
  echo "$bats_out" | tr -cd "[:print:]\n" | sed 's/\[3J\[H\[2J//'
  popd > /dev/null || exit
  return $status
}

# ensure that we have a clean working change set before we begin
if [[ $(git diff --stat) != '' ]]; then
  echo "cannot run compatibility tests with git working changes"
  exit 1
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

# for each legacy version, setup a repository
# using dolt built from the current branch
build_dolt "$starting_branch"
while IFS= read -r ver
do
  setup_dir "$ver-forward_compat"
done < <(grep -v '^ *#' < dolt_versions.txt)

status=0

while IFS= read -r ver
do
  build_dolt "$ver"
  setup_dir "$ver-backward_compat"

  # run compatibility.bats to ensure dolt @ $ver can
  # read a repo created with dolt @ HEAD
  echo
  echo "testing dolt @ $(git describe --tags) against repo in $ver-forward_compat/"
  run_bats_tests "$ver-forward_compat"
  status=$((status+$?))
  echo
done < <(grep -v '^ *#' < dolt_versions.txt)

# now build dolt @ HEAD and make sure we can read
# all of the legacy repositories we created
build_dolt "$starting_branch"

while IFS= read -r ver
do
  echo
  echo "testing dolt @ $(git rev-parse --abbrev-ref HEAD) against repo in $ver-backward_compat/"
  run_bats_tests "$ver-backward_compat"
  status=$((status+$?))
  echo
done < <(grep -v '^ *#' < dolt_versions.txt)

popd > /dev/null || exit

exit $status
