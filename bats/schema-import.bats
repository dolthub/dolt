#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    cat <<DELIM > 1pk5col-ints.csv
pk,c1,c2,c3,c4,c5
0,1,2,3,4,5
1,1,2,3,4,5
DELIM

    cat <<DELIM > 1pksupportedtypes.csv
pk, int, string, boolean, float, uint, uuid
0, 0, "asdf", TRUE, 0.0, 0, "00000000-0000-0000-0000-000000000000"
1, -1, "qwerty", FALSE, -1.0, 1, "00000000-0000-0000-0000-000000000001"
2, 1, "", TRUE, 0.0, 0, "123e4567-e89b-12d3-a456-426655440000"
DELIM

    cat <<DELIM > abc.csv
pk,a,b,c
0, red,  1.1, true
1, blue, 2.2, false
DELIM

    cat <<DELIM > abc-xyz.csv
pk,a,b,c,x,y,z
0, red,  1.1, true,  green,  3.14, -1
1, blue, 2.2, false, yellow, 2.71, -2
DELIM
}

teardown() {
    teardown_common
}

@test "schema import create" {
    run dolt schema import -c --pks=pk test 1pk5col-ints.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Created table successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt schema show
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 10 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` INT" ]] || false
    [[ "$output" =~ "\`c1\` INT" ]] || false
    [[ "$output" =~ "\`c2\` INT" ]] || false
    [[ "$output" =~ "\`c3\` INT" ]] || false
    [[ "$output" =~ "\`c4\` INT" ]] || false
    [[ "$output" =~ "\`c5\` INT" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "schema import dry run" {
    run dolt schema import --dry-run -c --pks=pk test 1pk5col-ints.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 9 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` INT" ]] || false
    [[ "$output" =~ "\`c1\` INT" ]] || false
    [[ "$output" =~ "\`c2\` INT" ]] || false
    [[ "$output" =~ "\`c3\` INT" ]] || false
    [[ "$output" =~ "\`c4\` INT" ]] || false
    [[ "$output" =~ "\`c5\` INT" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "test" ]] || false
}

@test "schema import with a bunch of types" {
    run dolt schema import --dry-run -c --pks=pk test 1pksupportedtypes.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 10 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` INT" ]] || false
    [[ "$output" =~ "\`int\` INT" ]] || false
    [[ "$output" =~ "\`string\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`boolean\` BIT(1)" ]] || false
    [[ "$output" =~ "\`float\` FLOAT" ]] || false
    [[ "$output" =~ "\`uint\` INT UNSIGNED" ]] || false
    [[ "$output" =~ "\`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin" ]] || false
}

@test "schema import with an empty csv" {
    cat <<DELIM > empty.csv
DELIM
    run dolt schema import --dry-run -c --pks=pk test empty.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Header line is empty" ]] || false
}

@test "schema import replace" {
    dolt schema import -c --pks=pk test 1pk5col-ints.csv
    run dolt schema import -r --pks=pk test 1pksupportedtypes.csv
    [ "$status" -eq 0 ]
    run dolt schema show
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` INT" ]] || false
    [[ "$output" =~ "\`int\` INT" ]] || false
    [[ "$output" =~ "\`string\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`boolean\` BIT(1)" ]] || false
    [[ "$output" =~ "\`float\` FLOAT" ]] || false
    [[ "$output" =~ "\`uint\` INT" ]] || false
    [[ "$output" =~ "\`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin" ]] || false
}

@test "schema import with invalid names" {
    run dolt schema import -c --pks=pk 123 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    run dolt schema import -c --pks=pk dolt_docs 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
    run dolt schema import -c --pks=pk dolt_query_catalog 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
    run dolt schema import -c --pks=pk dolt_reserved 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
}

@test "schema import with multiple primary keys" {
    cat <<DELIM > 2pk5col-ints.csv
pk1,pk2,c1,c2,c3,c4,c5
0,0,1,2,3,4,5
1,1,1,2,3,4,5
DELIM
    run dolt schema import -c --pks=pk1,pk2 test 2pk5col-ints.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Created table successfully." ]] || false
    dolt schema show
    run dolt schema show
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk1\` INT" ]] || false
    [[ "$output" =~ "\`pk2\` INT" ]] || false
    [[ "$output" =~ "\`c1\` INT" ]] || false
    [[ "$output" =~ "\`c2\` INT" ]] || false
    [[ "$output" =~ "\`c3\` INT" ]] || false
    [[ "$output" =~ "\`c4\` INT" ]] || false
    [[ "$output" =~ "\`c5\` INT" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk1\`,\`pk2\`)" ]] || false
}

@test "schema import missing values in CSV rows" {
    cat <<DELIM > empty-strings-null-values.csv
pk,headerOne,headerTwo
a,"""""",1
b,"",2
c,,3
d,row four,""
e,row five,
f,row six,6
g, ,
DELIM
    run dolt schema import -c --pks=pk test empty-strings-null-values.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 7 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`headerOne\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`headerTwo\` INT" ]] || false
}

@test "schema import --keep-types" {
    cat <<DELIM > 1pk5col-strings.csv
pk,c1,c2,c3,c4,c5,c6
"0","foo","bar","baz","car","dog","tim"
"1","1","2","3","4","5","6"
DELIM

    run dolt schema import -c --keep-types --pks=pk test 1pk5col-ints.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "parameter keep-types not supported for create operations" ]] || false
    dolt schema import -c --pks=pk test 1pk5col-ints.csv
    run dolt schema import -r --keep-types --pks=pk test 1pk5col-strings.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` INT" ]] || false
    [[ "$output" =~ "\`c1\` INT" ]] || false
    [[ "$output" =~ "\`c2\` INT" ]] || false
    [[ "$output" =~ "\`c3\` INT" ]] || false
    [[ "$output" =~ "\`c4\` INT" ]] || false
    [[ "$output" =~ "\`c5\` INT" ]] || false
    [[ "$output" =~ "\`c6\` LONGTEXT" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "schema import with strings in csv" {
    cat <<DELIM > 1pk5col-strings.csv
pk,c1,c2,c3,c4,c5,c6
"0","foo","bar","baz","car","dog","tim"
"1","1","2","3","4","5","6"
DELIM
    dolt schema import -c --pks=pk test 1pk5col-strings.csv
    run dolt schema import -r --keep-types --pks=pk test 1pk5col-strings.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` INT" ]] || false
    [[ "$output" =~ "\`c1\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`c2\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`c3\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`c4\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`c5\` LONGTEXT" ]] || false
    [[ "$output" =~ "\`c6\` LONGTEXT" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "schema import supports dates and times" {
    cat <<DELIM > 1pk-datetime.csv
pk, test_date
0, 2013-09-24 00:01:35
1, "2011-10-24 13:17:42"
2, 2018-04-13
DELIM
    run dolt schema import -c --pks=pk test 1pk-datetime.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "DATETIME" ]] || false;
}

@test "schema import uses specific date/time types" {
    cat <<DELIM > chrono.csv
pk, c_date, c_time, c_datetime, c_date+time
0, "2018-04-13", "13:17:42",     "2011-10-24 13:17:42.123", "2018-04-13"
1, "2018-04-13", "13:17:42.123", "2011-10-24 13:17:42",     "13:17:42"
DELIM
    run dolt schema import -c --pks=pk test chrono.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "\`c_date\` DATE" ]] || false
    [[ "$output" =~ "\`c_time\` TIME" ]] || false
    [[ "$output" =~ "\`c_datetime\` DATETIME" ]] || false
    [[ "$output" =~ "\`c_date+time\` DATETIME" ]] || false
}

@test "schema import of two tables" {
    dolt schema import -c --pks=pk test1 1pksupportedtypes.csv
    dolt schema import -c --pks=pk test2 1pk5col-ints.csv
}

@test "schema import --update adds columns" {
    dolt table import -c -pk=pk test abc.csv
    dolt add test
    dolt commit -m "added table"
    run dolt schema import -pks=pk -u test abc-xyz.csv
    [ "$status" -eq 0 ]
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+   \`x\` LONGTEXT" ]] || false
    [[ "$output" =~ "+   \`y\` FLOAT" ]] || false
    [[ "$output" =~ "+   \`z\` INT" ]] || false
    # assert no columns were deleted/replaced
    [[ ! "$output" = "-    \`" ]] || false
    run dolt sql -r csv -q 'select * from test'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk,a,b,c,x,y,z" ]] || false
    skip "schema import --update is currently deleting table data"
    [[ "$output" =~ "0,red,1.1,true,,," ]] || false
    [[ "$output" =~ "1,blue,2.2,false,,," ]] || false
}


@test "schema import --replace adds columns" {
    dolt table import -c -pk=pk test abc.csv
    dolt add test
    dolt commit -m "added table"
    run dolt schema import -pks=pk -r test abc-xyz.csv
    [ "$status" -eq 0 ]
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+   \`x\` LONGTEXT" ]] || false
    [[ "$output" =~ "+   \`y\` FLOAT" ]] || false
    [[ "$output" =~ "+   \`z\` INT" ]] || false
    # assert no columns were deleted/replaced
    [[ ! "$output" = "-    \`" ]] || false
    run dolt sql -r csv -q 'select count(*) from test'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "COUNT(*)" ]] || false
    [[ "$output" =~ "0" ]] || false
}
