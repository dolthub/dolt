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
    run dolt sql -q "create table test (pk int, c1 int, c2 int, c3 int, c4 int, c5 int, primary key (pk))"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false
    # use bash -c so I can | the output to grep
    run bash -c "dolt schema"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` int not null comment 'tag:0'" ]] || false
    [[ "$output" =~ "\`c1\` int comment 'tag:1'" ]] || false
    [[ "$output" =~ "\`c2\` int comment 'tag:2'" ]] || false
    [[ "$output" =~ "\`c3\` int comment 'tag:3'" ]] || false
    [[ "$output" =~ "\`c4\` int comment 'tag:4'" ]] || false
    [[ "$output" =~ "\`c5\` int comment 'tag:5'" ]] || false
    [[ "$output" =~ "primary key (\`pk\`)" ]] || false
}

@test "create a table with sql with multiple primary keys" {
    run dolt sql -q "create table test (pk1 int, pk2 int, c1 int, c2 int, c3 int, c4 int, c5 int, primary key (pk1), primary key (pk2))"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run bash -c "dolt schema"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk1\` int not null comment 'tag:0'" ]] || false
    [[ "$output" =~ "\`pk2\` int not null comment 'tag:1'" ]] || false
    [[ "$output" =~ "\`c1\` int comment 'tag:2'" ]] || false
    [[ "$output" =~ "\`c2\` int comment 'tag:3'" ]] || false
    [[ "$output" =~ "\`c3\` int comment 'tag:4'" ]] || false
    [[ "$output" =~ "\`c4\` int comment 'tag:5'" ]] || false
    [[ "$output" =~ "\`c5\` int comment 'tag:6'" ]] || false
    [[ "$output" =~ "primary key (\`pk1\`,\`pk2\`)" ]] || false
}

@test "create a table using sql with not null constraint" {
    run dolt sql -q "create table test (pk int not null, c1 int, c2 int, c3 int, c4 int, c5 int, primary key (pk))"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt schema test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` int not null comment 'tag:0'" ]] || false
    [[ "$output" =~ "\`c1\` int comment 'tag:1'" ]] || false
    [[ "$output" =~ "\`c2\` int comment 'tag:2'" ]] || false
    [[ "$output" =~ "\`c3\` int comment 'tag:3'" ]] || false
    [[ "$output" =~ "\`c4\` int comment 'tag:4'" ]] || false
    [[ "$output" =~ "\`c5\` int comment 'tag:5'" ]] || false
    [[ "$output" =~ "primary key (\`pk\`)" ]] || false
}

@test "create a table using sql with a float" {
    run dolt sql -q "create table test (pk int not null, c1 float, primary key (pk))"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt schema test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\` " ]] || false
    [[ "$output" =~ "\`pk\` int not null comment 'tag:0'" ]] || false
    [[ "$output" =~ "\`c1\` float comment 'tag:1'" ]] || false
    [[ "$output" =~ "primary key (\`pk\`)" ]] || false
}

   
@test "create a table using sql with a string" {
    run dolt sql -q "create table test (pk int not null, c1 varchar, primary key (pk))"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt schema test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` int not null comment 'tag:0'" ]] || false
    [[ "$output" =~ "\`c1\` varchar(1024) comment 'tag:1'" ]] || false
    [[ "$output" =~ "primary key (\`pk\`)" ]] || false
}


@test "create a table using sql with an unsigned int" {
    run dolt sql -q "create table test (pk int not null, c1 int unsigned, primary key (pk))"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt schema test
    [[ "$output" =~ "int unsigned" ]] || false
}

@test "create a table using sql with a boolean" {
    run dolt sql -q "create table test (pk int not null, c1 bool, primary key (pk))"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "create a table using sql with a uuid type" {
    run dolt sql -q "create table test (pk int not null, c1 uuid, primary key (pk))"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "create table with sql and dolt table create table. match success/failure" {
    run dolt sql -q "create table 1pk (pk int not null, c1 int, primary key(pk))"
    [ "$status" -eq 1 ]
    skip "This case needs a lot of work."
    [ "$output" = "Invalid table name. Table names cannot start with digits." ] 
    skip "dolt table create should fail on invalid table name" 
    dolt table create -s=`batshelper 1pk5col-ints.schema` 1pk
    [ "$status" -eq 1 ]
    [ "$output" = "Invalid table name. Table names cannot start with digits." ]
    run dolt sql -q "create table one-pk (pk int not null, c1 int, primary key(pk))"
    [ "$status" -eq 1 ]
    skip "Need better error message"
    [ "$output" = "Invalid table name. Table names cannot contain dashes." ]
    dolt table create -s=`batshelper 1pk5col-ints.schema` 1pk
    [ "$status" -eq 1 ]
    [ "$output" = "Invalid table name. Table names cannot contain dashes." ]
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
