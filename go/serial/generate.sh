#!/bin/bash

set -eou pipefail
SRC=$(dirname ${BASH_SOURCE[0]})

GEN_DIR="$SRC/../gen/fb/serial"

# cleanup old generated files
if [ ! -z "$(ls $GEN_DIR)" ]; then
    rm $GEN_DIR/*.go
fi

FLATC=${FLATC:-$SRC/../../proto/third_party/flatbuffers/bazel-bin/flatc}

if [ ! -x "$FLATC" ]; then
  echo "$FLATC is not an executable. Did you remember to run 'bazel build //:flatc' in $(dirname $(dirname $FLATC))"
  exit 1
fi

# generate golang (de)serialization package
"$FLATC" -o $GEN_DIR --gen-onefile --filename-suffix "" --gen-mutable --go-namespace "serial" --go \
  addressmap.fbs \
  blob.fbs \
  branchcontrol.fbs \
  collation.fbs \
  commit.fbs \
  commitclosure.fbs \
  encoding.fbs \
  foreign_key.fbs \
  mergeartifacts.fbs \
  prolly.fbs \
  rootvalue.fbs \
  schema.fbs \
  stash.fbs \
  stashlist.fbs \
  storeroot.fbs \
  stat.fbs \
  table.fbs \
  tag.fbs \
  tuple.fbs \
  workingset.fbs \
  vectorindexnode.fbs

# prefix files with copyright header
for FILE in $GEN_DIR/*.go;
do
  mv $FILE "tmp.go"
  cat "copyright.txt" "tmp.go" >> $FILE
  rm "tmp.go"
done

cp fileidentifiers.go $GEN_DIR

# format and remove unused imports
goimports -w $GEN_DIR
