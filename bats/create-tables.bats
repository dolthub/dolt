#!/usr/bin/env bats

setup() {
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

@test "create a single primary key table" {
    run dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "create a two primary key table" {
    run dolt table create -s=$BATS_TEST_DIRNAME/helper/2pk5col-ints.schema test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "create a table that uses all supported types" {
    run dolt table create -s=$BATS_TEST_DIRNAME/helper/1pksupportedtypes.schema test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "create a table that uses unsupported blob type" {
    run dolt table create -s=$BATS_TEST_DIRNAME/helper/1pkunsupportedtypes.schema test
    skip "Can create a blob type in schema now but I should not be able to. Also can create a column of type poop that gets converted to type bool."
    [ "$status" -eq 1 ]

}

@test "create a repo with two tables" {
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test1
    dolt table create -s=$BATS_TEST_DIRNAME/helper/2pk5col-ints.schema test2
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ "$output" =~ "test2" ]] || false
    [ "${#lines[@]}" -eq 3 ]
}

@test "create a table with json import" {
    run dolt table import -c -s $BATS_TEST_DIRNAME/helper/employees-sch.json employees $BATS_TEST_DIRNAME/helper/employees-tbl.json
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select employees
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tim" ]] || false
    [ "${#lines[@]}" -eq 4 ]
}

@test "create a table with json import. no schema" {
    run dolt table import -c employees $BATS_TEST_DIRNAME/helper/employees-tbl.json
    [ "$status" -ne 0 ]
    [ "$output" = "Please specify schema file for .json tables." ] 
}

@test "import data from csv and create the table" {
    run dolt table import -c --pk=pk test $BATS_TEST_DIRNAME/helper/1pk5col-ints.csv
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "create a table with two primary keys from csv import" {
    run dolt table import -c --pk=pk1,pk2 test $BATS_TEST_DIRNAME/helper/2pk5col-ints.csv
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "import data from psv and create the table" {
    run dolt table import -c --pk=pk test $BATS_TEST_DIRNAME/helper/1pk5col-ints.psv
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "create two table with the same name" {
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    run dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
    [ "$status" -ne 0 ]
    [[ "$output" =~ "already exists." ]] || false
}

@test "create a table from CSV with common column name patterns" {
    run dolt table import -c --pk=UPPERCASE test $BATS_TEST_DIRNAME/helper/caps-column-names.csv
    [ "$status" -eq 0 ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "UPPERCASE" ]] || false
}


@test "create a table from excel import with multiple sheets" {
    run dolt table import -c --pk=id employees $BATS_TEST_DIRNAME/helper/employees.xlsx
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select employees
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tim" ]] || false
    [ "${#lines[@]}" -eq 4 ]
    run dolt table import -c --pk=number basketball $BATS_TEST_DIRNAME/helper/employees.xlsx
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
     run dolt table select basketball
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tim" ]] || false
    [ "${#lines[@]}" -eq 5 ]
}

@test "create a basic table (int types) using sql" {
    run dolt sql -q "create table test (pk int, c1 int, c2 int, c3 int, c4 int, c5 int, primary key (pk))"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    # use bash -c so I can | the output to grep
    run bash -c "dolt table schema | grep -c '\"kind\": \"int\"'"
    [ "$status" -eq 0 ]
    [ "$output" -eq 6 ]
}

@test "create a table with sql with multiple primary keys" {
    run dolt sql -q "create table test (pk1 int, pk2 int, c1 int, c2 int, c3 int, c4 int, c5 int, primary key (pk1), primary key (pk2))"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run bash -c "dolt table schema | grep -c '\"is_part_of_pk\": true'"
    [ "$status" -eq 0 ]
    [ "$output" -eq 2 ]
}

@test "create a table using sql with not null constraint" {
    run dolt sql -q "create table test (pk int not null, c1 int, c2 int, c3 int, c4 int, c5 int, primary key (pk))"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table schema test
    [ "$status" -eq 0 ]
    skip "sql create table does not support not null constraint yet"
    [[ "$output" =~ "not_null" ]] || false
}

@test "create a table using sql with a float" {
    run dolt sql -q "create table test (pk int not null, c1 float, primary key (pk))"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table schema test
    [[ "$output" =~ "float" ]] || false
}

@test "create a table using sql with a string" {
    run dolt sql -q "create table test (pk int not null, c1 varchar, primary key (pk))"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table schema test
    [[ "$output" =~ "string" ]] || false
}


@test "create a table using sql with an unsigned int" {
    run dolt sql -q "create table test (pk int not null, c1 int unsigned, primary key (pk))"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table schema test
    skip "dolt sql does not support the unsigned keyword yet"
    [[ "$output" =~ "uint" ]] ||false
}

@test "create a table using sql with a boolean" {
    run dolt sql -q "create table test (pk int not null, c1 bool, primary key (pk))"
    skip "dolt sql does not support boolean types"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "create a table using sql with a uuid type" {
    run dolt sql -q "create table test (pk int not null, c1 uuid, primary key (pk))"
    skip "dolt sql does not support uuid types"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}