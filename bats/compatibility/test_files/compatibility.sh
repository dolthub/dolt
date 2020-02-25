#!/bin/bash

if [[ $(git diff --stat) != '' ]]; then
  echo "cannot run compatibility test with git working changes"
#  exit
fi

echo "one"

starting_branch=$(git rev-parse --abbrev-ref HEAD)

echo "two"

if [ -d head ]; then rm -r head; fi
mkdir head && cd head
echo "three"
dolt init
../setup_repo.sh
cd ..

# https://github.com/koalaman/shellcheck/wiki/SC2013
while IFS= read -r ver
do
  pushd ../../../go/cmd/dolt || exit
  git checkout tags/"$ver"
  go install .
  popd || exit

  if [ -d "$ver" ]; then rm -r head; fi
  mkdir "$ver" && cd "$ver"

  echo "creating repo with dolt @ $ver"
  ../setup_repo.sh

  pushd ../head || exit
  # ensure we can read the repo
  dolt schema show

  cd ..

done < <(grep -v '^ *#' < versions.txt)

# go back to initial branch
pushd ../../../go/cmd/dolt/ || exit
git checkout $starting_branch
echo "installing dolt @ $starting_branch"
go install .
popd || exit

while IFS= read -r ver
do
  echo "checking compatibility for: $ver"
  cd "$ver" || exit
  pwd

  # ensure we can read the repo
  dolt schema show

  cd .. || exit
#  rm -r $"$ver"

done < <(grep -v '^ *#' < versions.txt)

# cleanup
rm -r head