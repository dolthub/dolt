#!/bin/bash

if [[ $(git diff --stat) != '' ]]; then
  echo "cannot run compatibility test with git working changes"
  exit
fi

starting_branch=$(git rev-parse --abbrev-ref HEAD)

mkdir "head"
pushd head || exit
dolt init
../setup_repo.sh
popd || exit

# https://github.com/koalaman/shellcheck/wiki/SC2013
while IFS= read -r ver
do
  echo "checking compatibility for: $ver"
  mkdir "$ver"
  pushd "$ver" || exit

  pushd ../../../go/dolt/cmd/ || exit
  git checkout tags/"$ver"
  go install .
  popd || exit

  ../setup_repo.sh

  pushd ../head || exit
  # ensure we can read the repo
  dolt schema show
  popd || exit

  popd || exit
done < <(grep -v '^ *#' < versions.txt)

# go back to initial branch
pushd ../../../go/dolt/cmd/ || exit
git checkout $"starting_branch"
go install .
popd || exit

while IFS= read -r ver
do
  echo "checking compatibility for: $ver"
  pushd "$ver" || exit

  # ensure we can read the repo
  dolt schema show

  popd || exit
  rm -r $"ver"

done < <(grep -v '^ *#' < versions.txt)

# cleanup
rm -r head