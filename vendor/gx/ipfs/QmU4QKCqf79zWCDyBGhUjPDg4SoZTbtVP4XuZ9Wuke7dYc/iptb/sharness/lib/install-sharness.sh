#!/bin/sh
# install-sharness.sh
#
# Copyright (c) 2014 Juan Batiz-Benet
# Copyright (c) 2015 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#
# This script checks that Sharness is installed in:
#
# $(pwd)/$clonedir/$sharnessdir/
#
# where $clonedir and $sharnessdir are configured below.
#
# If Sharness is not installed, this script will clone it
# from $urlprefix (defined below).
#
# If Sharness is not uptodate with $version (defined below),
# this script will fetch and will update the installed
# version to $version.
#

# settings
version=35e1480425c022cb964b614621bdcd21ceaf2e94
urlprefix=https://github.com/mlafeldt/sharness.git
clonedir=lib
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
else
    # We need to clone sharness!
    mkdir -p "$clonedir" || die "Could not create '$clonedir' directory"
    cd "$clonedir" || die "Could not cd into '$clonedir' directory"

    git clone "$urlprefix" || die "Could not clone '$urlprefix'"
    cd "$sharnessdir" || die "Could not cd into '$sharnessdir' directory"
fi

checkout_version
