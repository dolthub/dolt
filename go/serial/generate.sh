#!/bin/bash

GEN_DIR="../gen/fb/serial/"

flatc -o $GEN_DIR --gen-onefile --filename-suffix "" --gen-mutable --go-namespace "serial" --go \
  common.fbs \
  database.fbs \
  prolly.fbs \
  schema.fbs

goimports -w $GEN_DIR