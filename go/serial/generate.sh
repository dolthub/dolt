#!/bin/bash

GEN_DIR="../gen/fb/serial"

# cleanup old generated files
rm $GEN_DIR/*.go

# generate golang (de)serialization package
flatc -o $GEN_DIR --gen-onefile --filename-suffix "" --gen-mutable --go-namespace "serial" --go \
  commit.fbs \
  database.fbs \
  prolly.fbs \
  rootvalue.fbs \
  schema.fbs \
  storeroot.fbs \
  table.fbs \
  tag.fbs \
  workingset.fbs

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
