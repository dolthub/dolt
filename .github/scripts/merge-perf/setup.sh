#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "usage: setup.sh <dolt-dir> <data-dir>"
    exit 1
fi

DIR=$1
DATA=$2

rm -rf $DIR
mkdir $DIR
cd $DIR

dolt init

dolt sql < $DATA/create.sql

for $t in $(ls "$DATA/table_*"); do
    dolt table import --disable-fk-checks -u "table${t}" "$DATA/$t"
done

dolt commit -Am "add tables"

dolt sql < $DATA/diverge_main.sql

dolt commit -Am "add rows to conflict"

dolt checkout -b feature
dolt reset --hard head~1

dolt sql < $DATA/branch.sql

dolt commit -Am "new branch"

