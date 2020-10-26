#!/bin/bash

echo "Creating data directory and configuring Dolt"
mkdir /test
cd /test || return
dolt config --global --add user.name benchmark
dolt config --global --add user.email benchmark@dolthub.com
dolt init
dolt sql-server --host=0.0.0.0

