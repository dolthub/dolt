#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt table import -c -pk=Timestamp test `batshelper sql-reserved-column-name.csv`
}

teardown() {
    teardown_common
}

@test "run sql select on a table with a column name that is an sql reserved word" {
      run dolt sql -q "select * from test where \`Timestamp\`='1'"
      [ "$status" -eq 0 ]
      [[ "$output" =~ "Timestamp" ]] || false
      [[ "$output" =~ "1.1" ]] || false
      run dolt sql -q "select * from test where Timestamp='1'"
      [ "$status" -eq 0 ]
      [[ "$output" =~ "1.1" ]] || false
}
