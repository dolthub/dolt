#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "create a single primary key table" {
    run dolt table create -s=`batshelper 1pk5col-ints.schema` test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
}

@test "create a two primary key table" {
    run dolt table create -s=`batshelper 2pk5col-ints.schema` test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
}

@test "create a table that uses all supported types" {
    run dolt table create -s=`batshelper 1pksupportedtypes.schema` test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
}

@test "create a table that uses unsupported blob type" {
    run dolt table create -s=`batshelper 1pkunsupportedtypes.schema` test
    skip "Can create a blob type in schema now but I should not be able to. Also can create a column of type poop that gets converted to type bool."
    [ "$status" -eq 1 ]
}

@test "create a repo with two tables" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test1
    dolt table create -s=`batshelper 2pk5col-ints.schema` test2
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
    run dolt table select employees
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
    run dolt table select test
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
    skip "This panics right now with: panic: Unsupported table format should have failed before reaching here."
    [ "$status" -eq 1 ]
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
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
}

@test "create two table with the same name" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    run dolt table create -s=`batshelper 1pk5col-ints.schema` test
    [ "$status" -ne 0 ]
    [[ "$output" =~ "already exists." ]] || false
}

@test "create a table from CSV with common column name patterns" {
    run dolt table import -c --pk=UPPERCASE test `batshelper caps-column-names.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt table select test
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
    run dolt table select employees
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
    run dolt table select basketball
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
    run dolt sql -q "CREATE TABLE test (pk BIGINT, c1 BIGINT, c2 BIGINT, c3 BIGINT, c4 BIGINT, c5 BIGINT, PRIMARY KEY (pk))"
    [ "$status" -eq 0 ]
    [ -z "$output" ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    # use bash -c so I can | the output to grep
    run bash -c "dolt schema show"
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
    run dolt sql -q "CREATE TABLE test (pk1 BIGINT, pk2 BIGINT, c1 BIGINT, c2 BIGINT, c3 BIGINT, c4 BIGINT, c5 BIGINT, PRIMARY KEY (pk1), PRIMARY KEY (pk2))"
    [ "$status" -eq 0 ]
    [ -z "$output" ]
    run bash -c "dolt schema show"
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
    run dolt sql -q "CREATE TABLE test (pk BIGINT NOT NULL, c1 BIGINT, c2 BIGINT, c3 BIGINT, c4 BIGINT, c5 BIGINT, PRIMARY KEY (pk))"
    [ "$status" -eq 0 ]
    [ -z "$output" ]
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
    run dolt sql -q "CREATE TABLE test (pk BIGINT NOT NULL, c1 DOUBLE, PRIMARY KEY (pk))"
    [ "$status" -eq 0 ]
    [ -z "$output" ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\` " ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL COMMENT 'tag:0'" ]] || false
    [[ "$output" =~ "\`c1\` DOUBLE COMMENT 'tag:1'" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

   
@test "create a table using sql with a string" {
    run dolt sql -q "CREATE TABLE test (pk BIGINT NOT NULL, c1 TEXT, PRIMARY KEY (pk))"
    [ "$status" -eq 0 ]
    [ -z "$output" ]
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL COMMENT 'tag:0'" ]] || false
    [[ "$output" =~ "\`c1\` TEXT COMMENT 'tag:1'" ]] || false
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

@test "create table with sql and dolt table create table. match success/failure" {
    run dolt sql -q "CREATE TABLE 1pk (pk BIGINT NOT NULL, c1 BIGINT, PRIMARY KEY(pk))"
    [ "$status" -eq 1 ]
    skip "This case needs a lot of work."
    [ "$output" = "Invalid table name. Table names cannot start with digits." ] 
    skip "dolt table create should fail on invalid table name" 
    dolt table create -s=`batshelper 1pk5col-ints.schema` 1pk
    [ "$status" -eq 1 ]
    [ "$output" = "Invalid table name. Table names cannot start with digits." ]
    run dolt sql -q "CREATE TABLE one-pk (pk BIGINT NOT NULL, c1 BIGINT, PRIMARY KEY(pk))"
    [ "$status" -eq 1 ]
    skip "Need better error message"
    [ "$output" = "Invalid table name. Table names cannot contain dashes." ]
    dolt table create -s=`batshelper 1pk5col-ints.schema` 1pk
    [ "$status" -eq 1 ]
    [ "$output" = "Invalid table name. Table names cannot contain dashes." ]
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
    dolt table create -s=`batshelper 1pk5col-ints.schema` test
    run dolt diff
    [ $status -eq 0 ]
    [ "${lines[0]}" = "diff --dolt a/test b/test" ]
    [ "${lines[1]}" = "added table" ]
}

@test "create a table with null values from csv import" {
    run dolt table import -c test `batshelper empty-strings-null-values.csv`
    skip "Added a row with a space in between the commas that makes this panic"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 10 ]
    [ "${lines[3]}" = '| a  | ""        | 1         |' ]
    [ "${lines[4]}" = '| b  |           | 2         |' ]
    [ "${lines[5]}" = "| c  | <NULL>    | 3         |" ]
    [ "${lines[6]}" = '| d  | row four  |           |' ]
    [ "${lines[7]}" = "| e  | row five  | <NULL>    |" ]
    [ "${lines[8]}" = "| f  | row six   | 6         |" ]
    [ "${lines[9]}" = "| g  | <NULL>    | <NULL>    |" ]
}

@test "create a table with null values from csv import with schema file" {
    run dolt table import -c -s `batshelper empty-strings-null-values.schema` test `batshelper empty-strings-null-values.csv`
    skip "Added a row with a space in between the commas that makes this panic"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 10 ]
    [ "${lines[3]}" = '| a  | ""        | 1         |' ]
    [ "${lines[4]}" = '| b  |           | 2         |' ]
    [ "${lines[5]}" = "| c  | <NULL>    | 3         |" ]
    [ "${lines[6]}" = "| d  | row four  | <NULL>    |" ]
    [ "${lines[7]}" = "| e  | row five  | <NULL>    |" ]
    [ "${lines[8]}" = "| f  | row six   | 6         |" ]
    [ "${lines[9]}" = "| g  | <NULL>    | <NULL>    |" ]
}