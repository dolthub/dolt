#!/bin/bash

GEN_DIR="../gen/fb/serial/"

rm $GEN_DIR*.go

flatc -o $GEN_DIR --gen-onefile --filename-suffix "" --gen-mutable --go-namespace "serial" --go \
  database.fbs \
  prolly.fbs \
  schema.fbs

goimports -w $GEN_DIR