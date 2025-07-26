#!/bin/bash

set -eo pipefail

script_dir=$(dirname "$0")
cd $script_dir/../..

paths=`find . -maxdepth 1 -mindepth 1 \( -type d -print -o -type f -name '*.go' -print \)`

goimports -w -local github.com/dolthub/dolt,github.com/dolthub/eventsapi_schema $paths

bad_files=$(find $paths -name '*.go' | while read f; do
    if [[ $(awk '/import \(/{flag=1;next}/\)/{flag=0}flag' < $f | egrep -c '$^') -gt 2 ]]; then
        echo $f
    fi
done)

if [ "$bad_files" != "" ]; then
    for f in $bad_files; do
        awk '/import \(/{flag=1}/\)/{flag=0}flag&&!/^$/||!flag' < "$f" > "$f.bak"
        mv "$f.bak" "$f"
    done
    goimports -w -local github.com/dolthub/dolt,github.com/dolthub/eventsapi_schema .
fi
