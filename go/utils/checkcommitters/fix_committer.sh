#!/bin/bash

set -eo pipefail

force=
if [[ $# -gt 0 ]]; then
    if [ "$1" = "-f" ]; then
        force=`shift`
    fi
fi

if [[ $# -ne 4 && $# -ne 2 ]]; then
    echo "Usage: fix_committer.sh [-f] TARGET_BRANCH WRONG_EMAIL [RIGHT_NAME RIGHT_EMAIL]" 1>&2
    echo "  Example: fix_committer.sh master nobody@github.com \"Aaron Son\" \"aaron@liquidata.co\"" 1>&2
    echo "  If RIGHT_NAME and RIGHT_EMAIL are ommitted, they are taken to be the current user from git config" 1>&2
    exit 1
fi

script_dir=$(dirname "$0")
cd $script_dir/../../..

target=$1
wrongemail=$2
if [[ $# -eq 4 ]]; then
    rightname=$3
    rightemail=$4
else
    rightname=$(git config --get user.name)
    rightemail=$(git config --get user.email)
fi

mergebase=`git merge-base HEAD "remotes/origin/$target"`

exec git "$force" filter-branch --env-filter '
OLD_EMAIL="'"$wrongemail"'"
CORRECT_NAME="'"$rightname"'"
CORRECT_EMAIL="'"$rightemail"'"
if [ "$GIT_COMMITTER_EMAIL" = "$OLD_EMAIL" ]
then
    export GIT_COMMITTER_NAME="$CORRECT_NAME"
    export GIT_COMMITTER_EMAIL="$CORRECT_EMAIL"
fi
if [ "$GIT_AUTHOR_EMAIL" = "$OLD_EMAIL" ]
then
    export GIT_AUTHOR_NAME="$CORRECT_NAME"
    export GIT_AUTHOR_EMAIL="$CORRECT_EMAIL"
fi
' ${mergebase}..HEAD
