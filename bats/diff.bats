#!/usr/bin/env bats

setup() {
    load $BATS_TEST_DIRNAME/helper/common.bash
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

@test "diff summary works" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
    dolt table put-row test pk:1 c1:1 c2:1 c3:1 c4:1 c5:1
    dolt add test
    dolt commit -m "table created"
    dolt table put-row test pk:2 c1:11 c2:0 c3:0 c4:0 c5:0
    dolt table put-row test pk:3 c1:11 c2:0 c3:0 c4:0 c5:0
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2 Rows Unmodified" ]] || false
    [[ "$output" =~ "2 Rows Added" ]] || false
    [[ "$output" =~ "0 Rows Deleted" ]] || false
    [[ "$output" =~ "0 Rows Modified" ]] || false
    [[ "$output" =~ "(2 entries vs 4 entries)" ]] || false

    dolt add test
    dolt commit -m "added two rows"
    dolt table put-row test pk:0 c1:11 c2:0 c3:0 c4:0 c5:0
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified" ]] || false
    [[ "$output" =~ "0 Rows Added" ]] || false
    [[ "$output" =~ "0 Rows Deleted" ]] || false
    [[ "$output" =~ "1 Row Modified" ]] || false
    [[ "$output" =~ "(4 entries vs 4 entries)" ]] || false

    dolt add test
    dolt commit -m "modified first row"
    dolt table rm-row test 0
    run dolt diff --summary
    echo "OUTPUT = $output"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "3 Rows Unmodified" ]] || false
    [[ "$output" =~ "0 Rows Added" ]] || false
    [[ "$output" =~ "1 Row Deleted" ]] || false
    [[ "$output" =~ "0 Rows Modified" ]] || false
    [[ "$output" =~ "(4 entries vs 3 entries)" ]] || false
}

@test "diff summary comparing commits" {
  dolt checkout -b firstbranch
  dolt table create -s=`batshelper 1pk5col-ints.schema` test
  dolt table put-row test pk:0 c1:0 c2:0 c3:0 c4:0 c5:0
  dolt add test
  dolt commit -m "Added one row"
  dolt checkout -b newbranch
  dolt table put-row test pk:1 c1:1 c2:1 c3:1 c4:1 c5:1
  dolt add test
  dolt commit -m "Added another row"
  run dolt diff --summary firstbranch newbranch 
  echo "OUTPUT = $output"
  [ "$status" -eq 1 ]
}