#!/bin/bash

# Script to prepare database copies for dictionary building
# For each directory (which is a cloned database), creates two copies:
# <db>_cgo and <db>_native, runs dolt gc --full on each, and shows disk usage
#
# Prerequisites - build and install the custom dolt binaries:
# 1. Build CGO version:     cd .../dolt/go && go install -o dolt_cgo ./cmd/dolt
# 2. Build native version:  cd .../dolt/go && go install -tags zstd_native -o dolt_native ./cmd/dolt

set -e

if [ $# -eq 0 ]; then
    echo "Usage: $0 <db1> <db2> ..."
    echo "Each argument should be a directory containing a cloned dolt database"
    exit 1
fi

for db in "$@"; do
    if [ ! -d "$db" ]; then
        echo "Warning: $db is not a directory, skipping"
        continue
    fi
    
    if [ ! -d "$db/.dolt" ]; then
        echo "Warning: $db does not appear to be a dolt database (no .dolt directory), skipping"
        continue
    fi
    
    echo "Processing database: $db"

    cgo_db="${db}_cgo"
    native_db="${db}_native"
    cp -r "$db" "$cgo_db"
    cp -r "$db" "$native_db"

    echo "  Running dolt_cgo gc --full on $cgo_db..."
    (cd "$cgo_db" && time dolt_cgo gc --full)
    
    echo "  Running dolt_native gc --full on $native_db..."
    (cd "$native_db" && time dolt_native gc --full)
    
    # Print disk usage
    echo "  Disk usage for $cgo_db:"
    (cd "$cgo_db" && du -sch .dolt)
    
    echo "  Disk usage for $native_db:"
    (cd "$native_db" && du -sch .dolt)
    
    echo ""
done

echo "Done preparing databases"