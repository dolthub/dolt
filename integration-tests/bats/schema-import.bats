#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

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
    assert_feature_version
    teardown_common
}

@test "schema-import: create" {
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
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`c1\` int" ]] || false
    [[ "$output" =~ "\`c2\` int" ]] || false
    [[ "$output" =~ "\`c3\` int" ]] || false
    [[ "$output" =~ "\`c4\` int" ]] || false
    [[ "$output" =~ "\`c5\` int" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "schema-import: dry run" {
    run dolt schema import --dry-run -c --pks=pk test 1pk5col-ints.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 9 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`c1\` int" ]] || false
    [[ "$output" =~ "\`c2\` int" ]] || false
    [[ "$output" =~ "\`c3\` int" ]] || false
    [[ "$output" =~ "\`c4\` int" ]] || false
    [[ "$output" =~ "\`c5\` int" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "test" ]] || false
}

@test "schema-import: with a bunch of types" {
    run dolt schema import --dry-run -c --pks=pk test 1pksupportedtypes.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 10 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`int\` int" ]] || false
    [[ "$output" =~ "\`string\` varchar(16383)" ]] || false
    [[ "$output" =~ "\`boolean\` tinyint" ]] || false
    [[ "$output" =~ "\`float\` float" ]] || false
    [[ "$output" =~ "\`uint\` int unsigned" ]] || false
    [[ "$output" =~ "\`uuid\` char(36) character set ascii collate ascii_bin" ]] || false
}

@test "schema-import: with an empty csv" {
    cat <<DELIM > empty.csv
DELIM
    run dolt schema import --dry-run -c --pks=pk test empty.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Header line is empty" ]] || false
}

@test "schema-import: replace" {
    dolt schema import -c --pks=pk test 1pk5col-ints.csv
    run dolt schema import -r --pks=pk test 1pksupportedtypes.csv
    [ "$status" -eq 0 ]
    run dolt schema show
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}" =~ "test" ]] || false
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`int\` int" ]] || false
    [[ "$output" =~ "\`string\` varchar(16383)" ]] || false
    [[ "$output" =~ "\`boolean\` tinyint" ]] || false
    [[ "$output" =~ "\`float\` float" ]] || false
    [[ "$output" =~ "\`uint\` int" ]] || false
    [[ "$output" =~ "\`uuid\` char(36) character set ascii collate ascii_bin" ]] || false
}

@test "schema-import: with invalid names" {
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

@test "schema-import: with multiple primary keys" {
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
    [[ "$output" =~ "\`pk1\` int" ]] || false
    [[ "$output" =~ "\`pk2\` int" ]] || false
    [[ "$output" =~ "\`c1\` int" ]] || false
    [[ "$output" =~ "\`c2\` int" ]] || false
    [[ "$output" =~ "\`c3\` int" ]] || false
    [[ "$output" =~ "\`c4\` int" ]] || false
    [[ "$output" =~ "\`c5\` int" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk1\`,\`pk2\`)" ]] || false
}

@test "schema-import: missing values in CSV rows" {
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
    [[ "$output" =~ "\`pk\` varchar(16383)" ]] || false
    [[ "$output" =~ "\`headerOne\` varchar(16383)" ]] || false
    [[ "$output" =~ "\`headerTwo\` int" ]] || false
}

@test "schema-import: --keep-types" {
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
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`c1\` int" ]] || false
    [[ "$output" =~ "\`c2\` int" ]] || false
    [[ "$output" =~ "\`c3\` int" ]] || false
    [[ "$output" =~ "\`c4\` int" ]] || false
    [[ "$output" =~ "\`c5\` int" ]] || false
    [[ "$output" =~ "\`c6\` varchar(16383)" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "schema-import: with strings in csv" {
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
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`c1\` varchar(16383)" ]] || false
    [[ "$output" =~ "\`c2\` varchar(16383)" ]] || false
    [[ "$output" =~ "\`c3\` varchar(16383)" ]] || false
    [[ "$output" =~ "\`c4\` varchar(16383)" ]] || false
    [[ "$output" =~ "\`c5\` varchar(16383)" ]] || false
    [[ "$output" =~ "\`c6\` varchar(16383)" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "schema-import: supports dates and times" {
    cat <<DELIM > 1pk-datetime.csv
pk, test_date
0, 2013-09-24 00:01:35
1, "2011-10-24 13:17:42"
2, 2018-04-13
DELIM
    run dolt schema import -c --pks=pk test 1pk-datetime.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "datetime" ]] || false;
}

@test "schema-import: uses specific date/time types" {
    cat <<DELIM > chrono.csv
pk, c_date, c_time, c_datetime, c_date+time
0, "2018-04-13", "13:17:42",     "2011-10-24 13:17:42.123", "2018-04-13"
1, "2018-04-13", "13:17:42.123", "2011-10-24 13:17:42",     "13:17:42"
DELIM
    run dolt schema import -c --pks=pk test chrono.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "\`c_date\` date" ]] || false
    [[ "$output" =~ "\`c_time\` time" ]] || false
    [[ "$output" =~ "\`c_datetime\` datetime" ]] || false
    [[ "$output" =~ "\`c_date+time\` datetime" ]] || false
}

@test "schema-import: import of two tables" {
    dolt schema import -c --pks=pk test1 1pksupportedtypes.csv
    dolt schema import -c --pks=pk test2 1pk5col-ints.csv
}

@test "schema-import: --update adds new columns" {
    dolt table import -c -pk=pk test abc.csv
    dolt add test
    dolt commit -m "added table"
    run dolt schema import -pks=pk -u test abc-xyz.csv
    [ "$status" -eq 0 ]

    dolt diff --schema
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ '+  `x` varchar(16383) NOT NULL,' ]] || false
    [[ "$output" =~ '+  `y` float NOT NULL,' ]] || false
    [[ "$output" =~ '+  `z` int NOT NULL,' ]] || false
    # assert no columns were deleted/replaced
    [[ ! "$output" = "-    \`" ]] || false

    run dolt sql -r csv -q 'select * from test'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk,a,b,c,x,y,z" ]] || false
    skip "schema import --update is currently deleting table data"
    [[ "$output" =~ "0,red,1.1,true,,," ]] || false
    [[ "$output" =~ "1,blue,2.2,false,,," ]] || false
}

@test "schema-import: --replace adds new columns" {
    dolt table import -c -pk=pk test abc.csv
    dolt add test
    dolt commit -m "added table"
    run dolt schema import -pks=pk -r test abc-xyz.csv
    [ "$status" -eq 0 ]

    dolt diff --schema
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ '+  `x` varchar(16383) NOT NULL,' ]] || false
    [[ "$output" =~ '+  `y` float NOT NULL,' ]] || false
    [[ "$output" =~ '+  `z` int NOT NULL,' ]] || false
    # assert no columns were deleted/replaced
    [[ ! "$output" = "-    \`" ]] || false

    run dolt sql -r csv -q 'select count(*) from test'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "count(*)" ]] || false
    [[ "$output" =~ "0" ]] || false
}

@test "schema-import: --replace drops missing columns" {
    cat <<DELIM > xyz.csv
pk,x,y,z
0,green,3.14,-1
1,yellow,2.71,-2
DELIM
    dolt table import -c -pk=pk test abc-xyz.csv
    dolt add test
    dolt commit -m "added test"
    run dolt schema import -pks=pk -r test xyz.csv
    [ "$status" -eq 0 ]

    dolt diff --schema
    run dolt diff --schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ '-  `a` varchar(16383) NOT NULL,' ]] || false
    [[ "$output" =~ '-  `b` float NOT NULL,' ]] || false
    [[ "$output" =~ '-  `c` tinyint NOT NULL,' ]] || false
    # assert no columns were added
    [[ ! "$output" = "+    \`" ]] || false
}

@test "schema-import: with name map" {
    cat <<JSON > name-map.json
{
    "a":"aa",
    "b":"bb",
    "c":"cc"
}
JSON
    run dolt schema import -c -pks=pk -m=name-map.json test abc.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "\`pk\` int" ]] || false
    [[ "$output" =~ "\`aa\`" ]] || false
    [[ "$output" =~ "\`bb\`" ]] || false
    [[ "$output" =~ "\`cc\`" ]] || false
    [[ ! "$output" =~ "\`a\`" ]] || false
    [[ ! "$output" =~ "\`b\`" ]] || false
    [[ ! "$output" =~ "\`c\`" ]] || false
}

@test "schema-import: failed import, duplicate column name" {
    cat <<CSV > import.csv
abc,Abc,d
1,2,3
4,5,6
CSV
    run dolt schema import -c -pks=abc test import.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "name" ]] || false
    [[ "$output" =~ "invalid schema" ]] || false
}
