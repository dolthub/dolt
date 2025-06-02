#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
  setup_common
}

teardown() {
  assert_feature_version
  teardown_common
}

@test "import-replace-tables: replace table using csv" {
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
    run dolt table import -r test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 2, Additions: 2, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "import-replace-tables: replace table using csv with wrong schema" {
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
    run dolt table import -r test `batshelper 2pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Field 'pk' doesn't have a default value" ]] || false
}

@test "import-replace-tables: replace table using psv" {
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
    run dolt table import -r test `batshelper 1pk5col-ints.psv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 2, Additions: 2, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "import-replace-tables: replace table using psv with wrong schema" {
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
    run dolt table import -r test `batshelper 1pk5col-ints.psv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Field 'pk1' doesn't have a default value" ]] || false
}

@test "import-replace-tables: replace table using schema with csv" {
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
    run dolt table import -r -s `batshelper employees-sch.sql` test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "schema is not supported for update or replace operations" ]] || false
}

@test "import-replace-tables: replace table using json" {
    dolt sql <<SQL
CREATE TABLE employees (
  \`id\` varchar(20) NOT NULL COMMENT 'tag:0',
  \`first name\` LONGTEXT COMMENT 'tag:1',
  \`last name\` LONGTEXT COMMENT 'tag:2',
  \`title\` LONGTEXT COMMENT 'tag:3',
  \`start date\` LONGTEXT COMMENT 'tag:4',
  \`end date\` LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (id)
);
SQL
    run dolt table import -r employees `batshelper employees-tbl.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "import-replace-tables: replace table using json with wrong schema" {
    dolt sql <<SQL
CREATE TABLE employees (
  \`idz\` varchar(20) NOT NULL COMMENT 'tag:0',
  \`first namez\` LONGTEXT COMMENT 'tag:1',
  \`last namez\` LONGTEXT COMMENT 'tag:2',
  \`titlez\` LONGTEXT COMMENT 'tag:3',
  \`start datez\` LONGTEXT COMMENT 'tag:4',
  \`end datez\` LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (idz)
);
SQL
    run dolt table import -r employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "An error occurred while moving data" ]] || false
    [[ "$output" =~ "not found in schema" ]] || false
}

@test "import-replace-tables: replace table using schema with json" {
    dolt sql <<SQL
CREATE TABLE employees (
  \`idz\` varchar(20) NOT NULL COMMENT 'tag:0',
  \`first namez\` LONGTEXT COMMENT 'tag:1',
  \`last namez\` LONGTEXT COMMENT 'tag:2',
  \`titlez\` LONGTEXT COMMENT 'tag:3',
  \`start datez\` LONGTEXT COMMENT 'tag:4',
  \`end datez\` LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (idz)
);
SQL
    run dolt table import -r -s `batshelper 1pk5col-ints.json` employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: schema is not supported for update or replace operations" ]] || false
}

@test "import-replace-tables: replace table with json when table does not exist" {
    run dolt table import -r employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "The following table could not be found:" ]] || false
}

@test "import-replace-tables: replace table with existing data using json" {
    run dolt table import -c -s `batshelper employees-sch.sql` employees `batshelper employees-tbl.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt table import -r employees `batshelper employees-tbl-new.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 4, Additions: 4, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "import-replace-tables: replace table with existing data with different schema" {
    run dolt table import -c -s `batshelper employees-sch.sql` employees `batshelper employees-tbl.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt table import -r employees `batshelper employees-tbl-schema-wrong.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cause: column position not found in schema" ]] || false
}

@test "import-replace-tables: replace table with bad json" {
    dolt sql <<SQL
CREATE TABLE employees (
  \`id\` varchar(20) NOT NULL COMMENT 'tag:0',
  \`first name\` LONGTEXT COMMENT 'tag:1',
  \`last name\` LONGTEXT COMMENT 'tag:2',
  \`title\` LONGTEXT COMMENT 'tag:3',
  \`start date\` LONGTEXT COMMENT 'tag:4',
  \`end date\` LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (id)
);
SQL
    run dolt table import -r employees `batshelper employees-tbl-bad.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "An error occurred while moving data" ]] || false
}

@test "import-replace-tables: import table with unexpected JSON format" {
    dolt sql <<SQL
CREATE TABLE employees (
  id INTEGER,
  first_name VARCHAR(20) NOT NULL,
  PRIMARY KEY (id)
);
SQL

echo "[{\"id\": 1,\"first_name\":\"Wednesday\"},{\"id\": 2,\"first_name\":\"Monday\"}]" > unexpected.json
    run dolt table import -r employees unexpected.json
    [ "$status" -eq 1 ]
    [[ "$output" =~ "An error occurred while moving data" ]] || false
    [[ "$output" =~ "unexpected JSON format received, expected format: { \"rows\": [ json_row_objects... ] }" ]] || false
}

@test "import-replace-tables: replace table using xlsx file" {
    dolt sql <<SQL
CREATE TABLE employees (
  \`id\` varchar(20) NOT NULL COMMENT 'tag:0',
  \`first\` LONGTEXT COMMENT 'tag:1',
  \`last\` LONGTEXT COMMENT 'tag:2',
  \`title\` LONGTEXT COMMENT 'tag:3',
  \`start date\` LONGTEXT COMMENT 'tag:4',
  \`end date\` LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (id)
);
SQL
    run dolt table import -r employees `batshelper employees.xlsx`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "import-replace-tables: replace table using xlsx file with wrong schema" {
    dolt sql <<SQL
CREATE TABLE employees (
  \`id\` varchar(20) NOT NULL COMMENT 'tag:0',
  \`first name\` LONGTEXT COMMENT 'tag:1',
  \`last name\` LONGTEXT COMMENT 'tag:2',
  \`title\` LONGTEXT COMMENT 'tag:3',
  \`start date\` LONGTEXT COMMENT 'tag:4',
  \`end date\` LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (id)
);
SQL
    run dolt table import -r employees `batshelper employees.xlsx`
    [ "$status" -eq 0 ]
}

@test "import-replace-tables: replace table with 2 primary keys with a csv with one primary key" {
    run dolt table import -c --pk=pk1,pk2 test `batshelper 2pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt table import -r test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Field 'pk1' doesn't have a default value" ]] || false
}

@test "import-replace-tables: replace table with 2 primary keys with a csv with 2 primary keys" {
    run dolt table import -c --pk=pk1,pk2 test `batshelper 2pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt table import -r test `batshelper 2pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 4, Additions: 4, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "import-replace-tables: replace table with a json with columns in different order" {
    dolt sql <<SQL
CREATE TABLE employees (
  \`id\` varchar(20) NOT NULL COMMENT 'tag:0',
  \`first name\` LONGTEXT COMMENT 'tag:1',
  \`last name\` LONGTEXT COMMENT 'tag:2',
  \`title\` LONGTEXT COMMENT 'tag:3',
  \`start date\` LONGTEXT COMMENT 'tag:4',
  \`end date\` LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (id)
);
SQL
    run dolt table import -r employees `batshelper employees-tbl-schema-unordered.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "import-replace-tables: replace table with a csv with columns in different order" {
    dolt sql <<SQL
CREATE TABLE employees (
  \`id\` varchar(20) NOT NULL COMMENT 'tag:0',
  \`first name\` LONGTEXT COMMENT 'tag:1',
  \`last name\` LONGTEXT COMMENT 'tag:2',
  \`title\` LONGTEXT COMMENT 'tag:3',
  \`start date\` LONGTEXT COMMENT 'tag:4',
  \`end date\` LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (id)
);
SQL
    run dolt table import -r employees `batshelper employees-tbl-schema-unordered.csv`
    echo "$output"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "import-replace-tables: compare tables in database with table imported from parquet file" {
    dolt sql -q "CREATE TABLE testTypes (pk BIGINT PRIMARY KEY, v1 TIME, v2 YEAR, v3 DATETIME, v4 BOOL, v5 ENUM('one', 'two', 'three'));"
    dolt add .
    dolt commit -m "create table"

    dolt branch new_branch

    dolt sql -q "INSERT INTO testTypes VALUES (1,'11:11:11','2020','2020-04-09 11:11:11',true,'one'),(2,'12:12:12','2020','2020-04-09 12:12:12',false,'three'),(3,'04:12:34','2019','2019-10-10 04:12:34',true,NULL),(4,NULL,'2020','2011-09-19 23:23:14',false,'two');"

    dolt add .
    dolt commit -m "add rows"

    run dolt table export testTypes test.parquet
    [ "$status" -eq 0 ]
    [ -f test.parquet ]

    dolt checkout new_branch
    dolt table import -r testTypes test.parquet
    dolt add .
    dolt commit --allow-empty -m "update table from parquet file"

    run dolt diff --stat main new_branch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "import-replace-tables: replacing a table with a subset of data correctly works" {
   dolt sql <<SQL
CREATE TABLE test (
  pk int,
  c1 int,
  c2 int,
  c3 int,
  c4 int,
  c5 int,
  PRIMARY KEY (pk)
);
SQL

    dolt sql -q "insert into test values (0,1,2,3,4,5)"

    cat <<DELIM > 1pk5col-ints-updt.csv
pk,c2
0,7
DELIM

    run dolt table import -r test 1pk5col-ints-updt.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Warning: The import file's schema does not match the table's schema" ]] || false
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -r csv -q "select * from test"
    [ "${lines[1]}" = "0,,7,,," ]
}

@test "import-replace-tables: csv files has more columns than schema" {
    cat <<SQL > schema.sql
CREATE TABLE subset (
    pk INT NOT NULL,
    c1 INT,
    c2 INT,
    c3 INT,
    PRIMARY KEY (pk)
);
SQL
    cat <<DELIM > data.csv
pk,c4,c1,c2,c3
0,4,1,2,3
DELIM

    dolt sql < schema.sql
    dolt sql -q "insert into subset values (1000, 100, 1000, 10000)"

    run dolt table import -r subset data.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Warning: The import file's schema does not match the table's schema" ]] || false
    [[ "$output" =~ "If unintentional, check for any typos in the import file's header" ]] || false
    [[ "$output" =~ "Extra columns in import file:" ]] || false
    [[ "$output" =~ "	c4" ]] || false

    # schema argument subsets the data and adds empty column
    run dolt sql -r csv -q "select * from subset ORDER BY pk"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [ "${lines[1]}" = "0,1,2,3" ]
}

@test "import-replace-tables: different schema warning lists differing columns" {
    cat <<SQL > schema.sql
CREATE TABLE t (
    pk INT NOT NULL,
    c1 INT,
    c2 INT,
    c3 INT,
    PRIMARY KEY (pk)
);
SQL
    cat <<DELIM > data.csv
pk,c4,c1,c3
0,4,1,3
DELIM

    dolt sql < schema.sql
    dolt sql -q "insert into t values (1000, 100, 1000, 10000)"

    run dolt table import -r t data.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Warning: The import file's schema does not match the table's schema" ]] || false
    [[ "$output" =~ "If unintentional, check for any typos in the import file's header" ]] || false
    [[ "$output" =~ "Missing columns in t:" ]] || false
    [[ "$output" =~ "	c2" ]] || false
    [[ "$output" =~ "Extra columns in import file:" ]] || false
    [[ "$output" =~ "	c4" ]] || false
}

@test "import-replace-tables: Replace that breaks fk constraints correctly errors" {
    dolt sql <<SQL
CREATE TABLE colors (
    id INT NOT NULL,
    color VARCHAR(32) NOT NULL,

    PRIMARY KEY (id),
    INDEX color_index(color)
);
CREATE TABLE objects (
    id INT NOT NULL,
    name VARCHAR(64) NOT NULL,
    color VARCHAR(32),

    PRIMARY KEY(id),
    FOREIGN KEY (color) REFERENCES colors(color)
);
INSERT INTO colors (id,color) VALUES (1,'red'),(2,'green'),(3,'blue'),(4,'purple');
INSERT INTO objects (id,name,color) VALUES (1,'truck','red'),(2,'ball','green'),(3,'shoe','blue');
SQL

    cat <<DELIM > colors-bad.csv
id,name
1,'red'
DELIM

    run dolt table import -r colors colors-bad.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot truncate table colors as it is referenced in foreign key" ]] || false
}

@test "import-replace-tables: can't use --all-text with -r" {
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
    run dolt table import -r --all-text test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: --all-text is only supported for create operations" ]] || false
}
