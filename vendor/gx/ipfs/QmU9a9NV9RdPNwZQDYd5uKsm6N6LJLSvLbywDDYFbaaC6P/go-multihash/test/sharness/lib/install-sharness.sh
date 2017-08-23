#!/bin/sh
# install sharness.sh
#
# Copyright (c) 2014 Juan Batiz-Benet
# MIT Licensed; see the LICENSE file in this repository.
#

# settings
version=50229a79ba22b2f13ccd82451d86570fecbd194c
urlprefix=https://github.com/mlafeldt/sharness.git
clonedir=lib
sharnessdir=sharness

die() {
  echo >&2 "$@"
  exit 1
}

mkdir -p "$clonedir" || die "Could not create '$clonedir' directory"
cd "$clonedir" || die "Could not cd into '$clonedir' directory"

git clone "$urlprefix" || die "Could not clone '$urlprefix'"
cd "$sharnessdir" || die "Could not cd into '$sharnessdir' directory"
git checkout "$version" || die "Could not checkout '$version'"

exit 0
