#!/usr/bin/env bats

setup() {
    load $BATS_TEST_DIRNAME/helper/common.bash
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
    dolt table import -c -pk=Timestamp test `batshelper sql-reserved-column-name.csv`
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
      [ "$status" -eq 0 ]
      [[ "$output" =~ "1.1" ]] || false
}
