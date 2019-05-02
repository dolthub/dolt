#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
    dolt table import -c -pk=Timestamp test $BATS_TEST_DIRNAME/helper/sql-reserved-column-name.csv
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

@test "run sql select on a table with a column name that is an sql reserved word" {
      run dolt sql -q "select * from test where \`Timestamp\`='1'"
      [ "$status" -eq 0 ]
      [[ "$output" =~ "Timestamp" ]] || false
      [[ "$output" =~ "1.1" ]] || false
      run dolt sql -q "select * from test where Timestamp='1'"
      [ "$status" -eq 1 ]
      skip "Bad error message for unquoted sql reserved word column names"
      [[ ! "$output" =~ "Unknown column" ]] || false
}
