#!/bin/bash

set -eo pipefail

script_dir=$(dirname "$0")
cd $script_dir/../..

all_cnt=$(grep "func Test.*" libraries/doltcore/sqle/enginetest/dolt_engine_test.go | wc -l)
prep_cnt=$(grep "func Test.*Prepared" libraries/doltcore/sqle/enginetest/dolt_engine_test.go | wc -l | sed 's/^ *//g')
skip_cnt=$(grep "SkipPreparedsCount" libraries/doltcore/sqle/enginetest/dolt_engine_test.go | awk '{print $4}' | sed 's/^ *//g')

expected=$(($all_cnt-$skip_cnt))
if [[ "$expected" != "$prep_cnt" ]]; then
    echo "Expected '$expected' TestPrepared enginetests in dolt_engine_test.go, found: '$prep_cnt'"
    echo "Either increment SkipPreparedsCount or add a prepared test for the new test suite"
fi

