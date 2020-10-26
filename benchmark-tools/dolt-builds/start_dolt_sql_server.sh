#!/bin/bash
set -e

echo "Creating data directory and configuring Dolt"
[ -d /test ] || mkdir /test
cd /test
dolt config --global --add user.name benchmark
dolt config --global --add user.email benchmark@dolthub.com
dolt init
exec dolt sql-server --host=0.0.0.0