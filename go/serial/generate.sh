#!/bin/bash

# todo(andy): assert directory contents

flatc -o "../gen/fb/serial/" --gen-onefile --filename-suffix "" --gen-mutable --go-namespace "serial" --go \
  common.fbs \
  database.fbs \
  prolly.fbs \
  schema.fbs \
  transaction.fbs
