#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "create a single primary key table" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
}

@test "create a two primary key table" {
    dolt sql <<SQL
CREATE TABLE test (
  pk1 BIGINT NOT NULL COMMENT 'tag:0',
  pk2 BIGINT NOT NULL COMMENT 'tag:1',
  c1 BIGINT COMMENT 'tag:2',
  c2 BIGINT COMMENT 'tag:3',
  c3 BIGINT COMMENT 'tag:4',
  c4 BIGINT COMMENT 'tag:5',
  c5 BIGINT COMMENT 'tag:6',
  PRIMARY KEY (pk1,pk2)
);
SQL
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
}

@test "create a table that uses all supported types" {
    dolt sql <<SQL
CREATE TABLE test (
  \`pk\` BIGINT NOT NULL COMMENT 'tag:0',
  \`int\` BIGINT COMMENT 'tag:1',
  \`string\` LONGTEXT COMMENT 'tag:2',
  \`boolean\` BOOLEAN COMMENT 'tag:3',
  \`float\` DOUBLE COMMENT 'tag:4',
  \`uint\` BIGINT UNSIGNED COMMENT 'tag:5',
  \`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin COMMENT 'tag:6',
  PRIMARY KEY (pk)
);
SQL
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
}

@test "create a table that uses unsupported poop type" {
    run dolt sql <<SQL
CREATE TABLE test (
  \`pk\` BIGINT NOT NULL COMMENT 'tag:0',
  \`int\` BIGINT COMMENT 'tag:1',
  \`string\` LONGTEXT COMMENT 'tag:2',
  \`boolean\` BOOLEAN COMMENT 'tag:3',
  \`float\` DOUBLE COMMENT 'tag:4',
  \`uint\` BIGINT UNSIGNED COMMENT 'tag:5',
  \`uuid\` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin COMMENT 'tag:6',
  \`blob\` LONGBLOB COMMENT 'tag:7',
  \`poop\` POOP COMMENT 'tag:8',
  PRIMARY KEY (pk)
);
SQL
    [ "$status" -eq 1 ]
}

@test "create a repo with two tables" {
    dolt sql <<SQL
CREATE TABLE test1 (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql <<SQL
CREATE TABLE test2 (
  pk1 BIGINT NOT NULL COMMENT 'tag:10',
  pk2 BIGINT NOT NULL COMMENT 'tag:11',
  c1 BIGINT COMMENT 'tag:12',
  c2 BIGINT COMMENT 'tag:13',
  c3 BIGINT COMMENT 'tag:14',
  c4 BIGINT COMMENT 'tag:15',
  c5 BIGINT COMMENT 'tag:16',
  PRIMARY KEY (pk1,pk2)
);
SQL
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ "$output" =~ "test2" ]] || false
    [ "${#lines[@]}" -eq 3 ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ "$output" =~ "test2" ]] || false
}

@test "create a table with json import" {
    run dolt table import -c -s `batshelper employees-sch.json` employees `batshelper employees-tbl.json`
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
    run dolt table import -c -s `nativebatsdir employees-sch.json` employees `batshelper employees-tbl-bad.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error creating reader" ]] || false
    [[ "$output" =~ "employees-tbl-bad.json to" ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "employees" ]] || false
}

@test "create a table with json import. bad schema." {
    run dolt table import -c -s `nativebatsdir employees-sch-bad.json` employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error creating reader" ]] || false
    skip "Error message mentions valid table file but not invalid schema file"
    # Be careful here. "employees-sch-bad.json" matches. I think it is because
    # the command line is somehow in $output. Added " to" to make it fail.
    [[ "$output" =~ "employees-sch-bad.json to" ]] || false
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

@test "create two table with the same name" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    run dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    [ "$status" -ne 0 ]
    [[ "$output" =~ "already exists" ]] || false
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
    skip "errors with 'cause: zip: not a valid zip file'. should say not a valid xlsx file"
    [[ "$output" =~ "not a valid xlsx file" ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false
}

@test "create a basic table (int types) using sql" {
    run dolt sql <<SQL
CREATE TABLE test (
    pk BIGINT COMMENT 'tag:0',
    c1 BIGINT COMMENT 'tag:1',
    c2 BIGINT COMMENT 'tag:2',
    c3 BIGINT COMMENT 'tag:3',
    c4 BIGINT COMMENT 'tag:4',
    c5 BIGINT COMMENT 'tag:5',
    PRIMARY KEY (pk)
);
SQL
    [ "$status" -eq 0 ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL COMMENT 'tag:0'" ]] || false
    [[ "$output" =~ "\`c1\` BIGINT COMMENT 'tag:1'" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT COMMENT 'tag:2'" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT COMMENT 'tag:3'" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT COMMENT 'tag:4'" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT COMMENT 'tag:5'" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "create a table with sql with multiple primary keys" {
    run dolt sql <<SQL
CREATE TABLE test (
    pk1 BIGINT COMMENT 'tag:0',
    pk2 BIGINT COMMENT 'tag:1',
    c1 BIGINT COMMENT 'tag:2',
    c2 BIGINT COMMENT 'tag:3',
    c3 BIGINT COMMENT 'tag:4',
    c4 BIGINT COMMENT 'tag:5',
    c5 BIGINT COMMENT 'tag:6',
    PRIMARY KEY (pk1),
    PRIMARY KEY (pk2)
);
SQL
    [ "$status" -eq 0 ]
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk1\` BIGINT NOT NULL COMMENT 'tag:0'" ]] || false
    [[ "$output" =~ "\`pk2\` BIGINT NOT NULL COMMENT 'tag:1'" ]] || false
    [[ "$output" =~ "\`c1\` BIGINT COMMENT 'tag:2'" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT COMMENT 'tag:3'" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT COMMENT 'tag:4'" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT COMMENT 'tag:5'" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT COMMENT 'tag:6'" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk1\`,\`pk2\`)" ]] || false
}

@test "create a table using sql with not null constraint" {
    run dolt sql <<SQL
CREATE TABLE test (
    pk BIGINT NOT NULL COMMENT 'tag:0',
    c1 BIGINT COMMENT 'tag:1',
    c2 BIGINT COMMENT 'tag:2',
    c3 BIGINT COMMENT 'tag:3',
    c4 BIGINT COMMENT 'tag:4',
    c5 BIGINT COMMENT 'tag:5',
    PRIMARY KEY (pk)
);
SQL
    [ "$status" -eq 0 ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL COMMENT 'tag:0'" ]] || false
    [[ "$output" =~ "\`c1\` BIGINT COMMENT 'tag:1'" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT COMMENT 'tag:2'" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT COMMENT 'tag:3'" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT COMMENT 'tag:4'" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT COMMENT 'tag:5'" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "create a table using sql with a float" {
    run dolt sql <<SQL
CREATE TABLE test (
    pk BIGINT NOT NULL COMMENT 'tag:0',
    c1 DOUBLE COMMENT 'tag:1',
    PRIMARY KEY (pk)
);
SQL
    [ "$status" -eq 0 ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\` " ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL COMMENT 'tag:0'" ]] || false
    [[ "$output" =~ "\`c1\` DOUBLE COMMENT 'tag:1'" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}


@test "create a table using sql with a string" {
    run dolt sql <<SQL
CREATE TABLE test (
    pk BIGINT NOT NULL COMMENT 'tag:0',
    c1 LONGTEXT COMMENT 'tag:1',
    PRIMARY KEY (pk)
);
SQL
    [ "$status" -eq 0 ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL COMMENT 'tag:0'" ]] || false
    [[ "$output" =~ "\`c1\` LONGTEXT COMMENT 'tag:1'" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}


@test "create a table using sql with an unsigned int" {
    run dolt sql -q "CREATE TABLE test (pk BIGINT NOT NULL, c1 BIGINT UNSIGNED, PRIMARY KEY (pk))"
    [ "$status" -eq 0 ]
    [ -z "$output" ]
    run dolt schema show test
    [[ "$output" =~ "BIGINT UNSIGNED" ]] || false
}

@test "create a table using sql with a boolean" {
    run dolt sql -q "CREATE TABLE test (pk BIGINT NOT NULL, c1 BOOLEAN, PRIMARY KEY (pk))"
    [ "$status" -eq 0 ]
    [ -z "$output" ]
}

@test "create a table with a mispelled primary key" {
    run dolt sql -q "CREATE TABLE test (pk BIGINT, c1 BIGINT, c2 BIGINT, PRIMARY KEY
(pk,noexist))"
    skip "This succeeds right now and creates a table with just one primary key pk"
    [ "$status" -eq 1 ]
}

@test "import a table with non UTF-8 characters in it" {
    run dolt table import -c --pk=pk test `batshelper bad-characters.csv`
    skip "Dolt allows you to create tables with non-UTF-8 characters right now"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unsupported characters" ]] || false
}

@test "dolt diff on a newly created table" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "diff --dolt a/test b/test" ]] || false
    [[ "$output" =~ "added table" ]] || false
}

@test "create a table with null values from csv import" {
    run dolt table import -c test `batshelper empty-strings-null-values.csv`
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
    [ "${lines[6]}" = '| d  | row four  |           |' ]
    [ "${lines[7]}" = "| e  | row five  | <NULL>    |" ]
    [ "${lines[8]}" = "| f  | row six   | 6         |" ]
    [ "${lines[9]}" = "| g  | <NULL>    | <NULL>    |" ]
}

@test "create a table with null values from csv import with json file" {
    run dolt table import -c -s `batshelper empty-strings-null-values.json` test `batshelper empty-strings-null-values.csv`
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
