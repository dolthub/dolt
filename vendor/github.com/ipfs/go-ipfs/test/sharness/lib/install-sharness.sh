#!/bin/sh
# install sharness.sh
#
# Copyright (c) 2014 Juan Batiz-Benet
# MIT Licensed; see the LICENSE file in this repository.
#

# settings
version=5eee9b51b5621cec95a64018f0cc779963b230d2
urlprefix=https://github.com/mlafeldt/sharness.git
if test ! -n "$clonedir" ; then
  clonedir=lib
fi
sharnessdir=sharness

if test -f "$clonedir/$sharnessdir/SHARNESS_VERSION_$version"
then
  # There is the right version file. Great, we are done!
  exit 0
fi

die() {
  echo >&2 "$@"
  exit 1
}

checkout_version() {
  git checkout "$version" || die "Could not checkout '$version'"
  rm -f SHARNESS_VERSION_* || die "Could not remove 'SHARNESS_VERSION_*'"
  touch "SHARNESS_VERSION_$version" || die "Could not create 'SHARNESS_VERSION_$version'"
  echo "Sharness version $version is checked out!"
}

if test -d "$clonedir/$sharnessdir/.git"
then
  # We need to update sharness!
  cd "$clonedir/$sharnessdir" || die "Could not cd into '$clonedir/$sharnessdir' directory"
  git fetch || die "Could not fetch to update sharness"
  checkout_version
else
  # We need to clone sharness!
  mkdir -p "$clonedir" || die "Could not create '$clonedir' directory"
  cd "$clonedir" || die "Could not cd into '$clonedir' directory"

  git clone "$urlprefix" || die "Could not clone '$urlprefix'"
  cd "$sharnessdir" || die "Could not cd into '$sharnessdir' directory"
  checkout_version
fi
exit 0
