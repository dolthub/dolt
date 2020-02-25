#!/bin/bash

mkdir tmp
cp test_files/* tmp
cd tmp

./compatibility.sh

cd ..
rm -r tmp