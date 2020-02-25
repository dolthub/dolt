#!/bin/bash

test_env="test_env"

# copy all the test files to take them out of source control
# when we checkout different releases in compatilibility.sh
# we need the test files to remain
rm -r $test_env
mkdir $test_env
cp -r test_files/* $test_env
cd $test_env

./compatibility.sh

cd ..
#rm -r $test_env