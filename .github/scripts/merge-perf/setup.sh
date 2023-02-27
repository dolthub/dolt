#!/bin/bash

DIR=$1
DATA=$2

rm -rf $DIR
mkdir $DIR
cd $DIR

dolt init

dolt sql < $DATA/create.sql

for $t in $(ls "$DATA/table_*"); do
    dolt table import -u "table${t}" $t
done

dolt checkout -b feature
dolt reset --hard head~1

dolt sql < $DATA/branch.sql

dolt commit -Am "new branch"

