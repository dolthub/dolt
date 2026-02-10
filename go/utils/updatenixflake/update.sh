#!/bin/bash

set -eo pipefail

cd "$( dirname "${BASH_SOURCE[0]}" )"/../../..

OUT=$(mktemp -d -t nar-hash-XXXXXX)
cleanup() {
  rm -rf "$OUT"
}
trap 'cleanup' EXIT

(cd go; go mod vendor -o "$OUT")
h=`cd go; go run ./utils/updatenixflake -sri "$OUT"`
line="  vendorHash = \"$h\";"
sed -i'' default.nix -e 's|  vendorHash.*|'"$line"'|'
