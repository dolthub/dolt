#!/bin/bash

set -eo pipefail

script_dir=$(dirname "$0")
cd $script_dir/../..

bad_files=$(goimports -l -local github.com/liquidata-inc/dolt .)
if [ "$bad_files" != "" ]; then
    echo "ERROR: The following files do not match goimports output:"
    echo "$bad_files"
    echo
    echo "Please format the go code in the repository with './utils/repofmt/format_repo.sh'"
    exit 1
fi

bad_files=$(find . -name '*.go' | while read f; do
    if [[ $(awk '/import \(/{flag=1;next}/\)/{flag=0}flag' < $f | egrep -c '$^') -gt 2 ]]; then
        echo $f
    fi
done)

if [ "$bad_files" != "" ]; then
    echo "ERROR: The following files have more than three import groups:"
    echo "$bad_files"
    echo
    echo "Please format the go code in the repository with './utils/repofmt/format_repo.sh'"
    exit 1
fi
