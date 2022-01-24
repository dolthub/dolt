#!/bin/bash

WD=$(pwd)

# Clone and checkout to release 0.34.5 which has the old version of import.
# Add check if for directory exists
if ! [ -d "$WD/dolt" ]; then
  git clone git@github.com:dolthub/dolt.git
  cd dolt/go
  git checkout tags/v0.34.5
fi

# Build that binary and store locally if hasn't been built already.
if ! [ -f "$WD/old-dolt" ]; then
  cd $WD/dolt/go
  go build -o $WD/old-dolt "./cmd/dolt/"
fi

# Generate the test file
cd $WD
echo "generating test file"
python3 csv_gen.py '{
    "cols": [
        {"name":"pk", "type":"int", "generator":"shuffled_sequential"},
        {"name":"c1", "type":"uuid"},
        {"name":"c2", "type":"string", "length":512},
        {"name":"c3", "type":"float"},
        {"name":"c4", "type":"int"}
    ],
    "row_count": 1000000
}' > benchmark.csv

# Run the current version of dolt TODO: Assumes no storage version changes... Change if there is
echo "Running the current version of import"
rm -rf .dolt
dolt init
time dolt table import -c --pk=pk current_version benchmark.csv

# Run the old version of dolt
echo "Running version 0.34.5"
time ./old-dolt table import -c --pk=pk old_version benchmark.csv

# Run the current version of export
echo "Running the current version of export"
time dolt table export current_version export.csv

# Run the old version of export
time ./old-dolt table export -f old_version export.csv
