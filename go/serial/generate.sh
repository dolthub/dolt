#!/bin/bash

GEN_DIR="../gen/fb/serial"

# cleanup old generated files
rm $GEN_DIR/*.go

# generate golang (de)serialization package
flatc -o $GEN_DIR --gen-onefile --filename-suffix "" --gen-mutable --go-namespace "serial" --go \
  database.fbs \
  prolly.fbs \
  schema.fbs \
  table.fbs

# prefix files with copyright header
for FILE in $GEN_DIR/*.go;
do
  mv $FILE "tmp.go"
  cat "copyright.txt" "tmp.go" >> $FILE
  rm "tmp.go"
done

# format and remove unused imports
goimports -w $GEN_DIR
