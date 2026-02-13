#!/bin/bash

set -eou pipefail

FLATC="$1"
if [ ! -x "$FLATC" ]; then
  echo "$FLATC is not an executable. Did you remember to run 'bazel build //:flatc' in $(dirname $(dirname $FLATC))"
  exit 1
fi
shift

OUTDIR="$1"
shift

COPYRIGHTPATH="$1"
shift

# generate golang (de)serialization package
"$FLATC" -o "$OUTDIR" --gen-onefile --filename-suffix "" --gen-mutable --go-namespace "serial" --go "$@"

# prefix files with copyright header
for FILE in "$OUTDIR"/*.go;
do
  tmp="$OUTDIR"/tmp
  mv "$FILE" "$tmp"
  cat "$COPYRIGHTPATH" "$tmp" > $FILE
  rm "$tmp"
done
