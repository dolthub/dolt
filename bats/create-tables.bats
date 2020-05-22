#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE int_table (
  pk LONGTEXT NOT NULL,
  c1 LONGTEXT,
  c2 LONGTEXT,
  c3 LONGTEXT,
  c4 LONGTEXT,
  c5 LONGTEXT,
  PRIMARY KEY (pk)
);
SQL
    cat <<DELIM > 1pk5col-ints.csv
pk,c1,c2,c3,c4,c5
0,1,2,3,4,5
1,1,2,3,4,5
DELIM
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
}

teardown() {
    teardown_common
}

@test "create a table with json import" {
    run dolt table import -c -s `batshelper employees-sch.sql` employees `batshelper employees-tbl.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "employees" ]] || false
    run dolt sql -q "select * from employees"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tim" ]] || false
    [ "${#lines[@]}" -eq 7 ]
}

@test "create a table with json import. no schema." {
    run dolt table import -c employees `batshelper employees-tbl.json`
    [ "$status" -ne 0 ]
    [ "$output" = "Please specify schema file for .json tables." ]
}

@test "create a table with json import. bad json." {
    run dolt table import -c -s `batshelper employees-sch.sql` employees `batshelper employees-tbl-bad.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cause: invalid character after object key:value pair: 'b'" ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "employees" ]] || false
}

@test "create a table with json import. bad schema." {
    run dolt table import -c -s `batshelper employees-sch-bad.sql` employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error determining the output schema" ]] || false
    [[ "$output" =~ "syntax error at position 32 near 'And'" ]] || false
    [[ "$output" =~ "employees-sch-bad.sql" ]] || false
}

@test "import data from csv and create the table" {
    run dolt table import -c --pk=pk test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
}

@test "use -f to overwrite data in existing table" {
    dolt table import -c --pk=pk test `batshelper 1pk5col-ints.csv`

    run dolt table import -c --pk=pk test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "test already exists. Use -f to overwrite." ]] || false

    run dolt table import -f -c --pk=pk test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
}

@test "try to create a table with a bad csv" {
    run dolt table import -c --pk=pk test `batshelper bad.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error creating reader" ]] || false
}

@test "try to create a table with dolt table import with a bad file name" {
    run dolt table import -c test `batshelper bad.data`
    [ "$status" -eq 1 ]
}

@test "try to create a table with dolt table import with invalid name" {
    run dolt table import -c --pk=pk 123 `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    run dolt table import -c --pk=pk dolt_docs `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
    run dolt table import -c --pk=pk dolt_query_catalog `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
    run dolt table import -c --pk=pk dolt_reserved `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid table name" ]] || false
    [[ "$output" =~ "reserved" ]] || false
}

@test "create a table with two primary keys from csv import" {
    run dolt table import -c --pk=pk1,pk2 test `batshelper 2pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt sql -q 'select count(*) from test'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "4" ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
}

@test "import data from psv and create the table" {
    run dolt table import -c --pk=pk test `batshelper 1pk5col-ints.psv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
}

@test "create a table from CSV with common column name patterns" {
    run dolt table import -c --pk=UPPERCASE test `batshelper caps-column-names.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "UPPERCASE" ]] || false
}

@test "create a table from excel import with multiple sheets" {
    run dolt table import -c --pk=id employees `batshelper employees.xlsx`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "employees" ]] || false
    run dolt sql -q "select * from employees"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tim" ]] || false
    [ "${#lines[@]}" -eq 7 ]
    run dolt table import -c --pk=number basketball `batshelper employees.xlsx`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "employees" ]] || false
    [[ "$output" =~ "basketball" ]] || false
    run dolt sql -q "select * from basketball"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tim" ]] || false
    [ "${#lines[@]}" -eq 8 ]
}

@test "specify incorrect sheet name on excel import" {
    run dolt table import -c --pk=id bad-sheet-name `batshelper employees.xlsx`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table name must match excel sheet name" ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "bad-sheet-name" ]] || false
}

@test "import an .xlsx file that is not a valid excel spreadsheet" {
    run dolt table import -c --pk=id test `batshelper bad.xlsx`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not a valid xlsx file" ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false
}

@test "import a table with non UTF-8 characters in it" {
    # run dolt table import -c --pk=pk test `batshelper bad-characters.csv`
    # skip "Dolt allows you to create tables with non-UTF-8 characters right now"
    # [ "$status" -eq 1 ]
    # [[ "$output" =~ "unsupported characters" ]] || false

    run dolt table import -c --pk=pk test `batshelper bad-characters.csv`
    [ "$status" -eq 0 ]
    dolt sql -q 'select * from test'
    dolt sql -r csv -q 'select * from test' > compare.csv
    diff compare.csv `batshelper bad-characters.csv`
}

@test "dolt diff on a newly created table" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "diff --dolt a/test b/test" ]] || false
    [[ "$output" =~ "added table" ]] || false
}

@test "create a table with null values from csv import" {
    run dolt table import -c test empty-strings-null-values.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [ "${lines[3]}" = '| a  | ""        | 1         |' ]
    [ "${lines[4]}" = '| b  |           | 2         |' ]
    [ "${lines[5]}" = "| c  | <NULL>    | 3         |" ]
    [ "${lines[6]}" = '| d  | row four  | <NULL>    |' ]
    [ "${lines[7]}" = "| e  | row five  | <NULL>    |" ]
    [ "${lines[8]}" = "| f  | row six   | 6         |" ]
    [ "${lines[9]}" = "| g  | <NULL>    | <NULL>    |" ]
}

@test "table import with schema different from data file" {
    cat <<SQL > schema.sql
CREATE TABLE subset (
    pk INT NOT NULL,
    c1 INT,
    c3 INT,
    noData INT,
    PRIMARY KEY (pk)
);
SQL
    run dolt table import -s schema.sql -c subset 1pk5col-ints.csv
    [ "$status" -eq 0 ]

    # schema argument subsets the data and adds empty column
    run dolt sql -r csv -q "select * from subset"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "pk,c1,c3,noData" ]
    [ "${lines[1]}" = "0,1,3," ]
    [ "${lines[2]}" = "1,1,3," ]
}

@test "create a table with null values from csv import with json file" {
    cat <<SQL > schema.sql
CREATE TABLE empty_strings_null_values (
    pk VARCHAR(120) NOT NULL COMMENT 'tag:0',
    headerOne VARCHAR(120) COMMENT 'tag:1',
    headerTwo VARCHAR(120) COMMENT 'tag:2',
    PRIMARY KEY (pk)
);
SQL
    run dolt table import -c -s schema.sql empty_strings_null_values empty-strings-null-values.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "empty_strings_null_values" ]] || false
    run dolt sql -q "select * from empty_strings_null_values"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [ "${lines[3]}" = '| a  | ""        | 1         |' ]
    [ "${lines[4]}" = '| b  |           | 2         |' ]
    [ "${lines[5]}" = "| c  | <NULL>    | 3         |" ]
    [ "${lines[6]}" = "| d  | row four  |           |" ]
    [ "${lines[7]}" = "| e  | row five  | <NULL>    |" ]
    [ "${lines[8]}" = "| f  | row six   | 6         |" ]
    [ "${lines[9]}" = "| g  | <NULL>    | <NULL>    |" ]
}

@test "create a table with null values from json import with json file" {
    dolt sql <<SQL
CREATE TABLE test (
  pk LONGTEXT NOT NULL,
  headerOne LONGTEXT,
  headerTwo BIGINT,
  PRIMARY KEY (pk)
);
SQL
    run dolt table import -u test `batshelper empty-strings-null-values.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [ "${lines[3]}" = '| a  | ""        | 1         |' ]
    [ "${lines[4]}" = '| b  |           | 2         |' ]
    [ "${lines[5]}" = "| c  | <NULL>    | 3         |" ]
    [ "${lines[6]}" = "| d  | row four  | <NULL>    |" ]
    [ "${lines[7]}" = "| e  | row five  | <NULL>    |" ]
    [ "${lines[8]}" = "| f  | row six   | 6         |" ]
    [ "${lines[9]}" = "| g  | <NULL>    | <NULL>    |" ]
}

@test "fail to create a table with null values from json import with json file" {
    dolt sql <<SQL
CREATE TABLE test (
  pk LONGTEXT NOT NULL,
  headerOne LONGTEXT NOT NULL,
  headerTwo BIGINT NOT NULL,
  PRIMARY KEY (pk)
);
SQL
    run dolt table import -u test `batshelper empty-strings-null-values.json`
    [ "$status" -eq 1 ]
}

@test "table import -c infers types from data" {
    cat <<DELIM > types.csv
pk,str,int,bool,float, date, time, datetime
0,abc,123,false,3.14,2020-02-02,12:12:12.12,2020-02-02 12:12:12
DELIM
    run dolt table import -c --pk=pk test types.csv
    [ "$status" -eq 0 ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]]
    [[ "$output" =~ "\`pk\` INT" ]]
    [[ "$output" =~ "\`str\` LONGTEXT" ]]
    [[ "$output" =~ "\`int\` INT UNSIGNED" ]]
    [[ "$output" =~ "\`bool\` BIT(1)" ]]
    [[ "$output" =~ "\`float\` FLOAT" ]]
    [[ "$output" =~ "\`date\` DATE" ]]
    [[ "$output" =~ "\`time\` TIME" ]]
    [[ "$output" =~ "\`datetime\` DATETIME" ]]
}
