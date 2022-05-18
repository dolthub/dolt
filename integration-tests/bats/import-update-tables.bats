#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    cat <<SQL > 1pk5col-ints-sch.sql
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

    cat <<SQL > 1pk1col-char-sch.sql
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c CHAR(5) COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL

    cat <<DELIM > 1pk5col-ints.csv
pk,c1,c2,c3,c4,c5
0,1,2,3,4,5
1,1,2,3,4,5
DELIM

    cat <<DELIM > 1pk5col-ints-updt.csv
pk,c1,c2,c3,c4,c5
0,1,2,3,4,6
DELIM

    cat <<SQL > employees-sch.sql
CREATE TABLE employees (
  \`id\` LONGTEXT NOT NULL COMMENT 'tag:0',
  \`first name\` LONGTEXT COMMENT 'tag:1',
  \`last name\` LONGTEXT COMMENT 'tag:2',
  \`title\` LONGTEXT COMMENT 'tag:3',
  \`start date\` LONGTEXT COMMENT 'tag:4',
  \`end date\` LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (id)
);
SQL

  cat <<SQL > check-constraint-sch.sql
CREATE TABLE persons (
    ID int PRIMARY KEY,
    LastName varchar(255) NOT NULL,
    FirstName varchar(255),
    Age int CHECK (Age>=18)
);
SQL

    cat <<SQL > nibrs_month_sch.sql
CREATE TABLE \`test\` (
  \`STATE_ID\` smallint NOT NULL,
  \`NIBRS_MONTH_ID\` bigint NOT NULL,
  \`AGENCY_ID\` bigint NOT NULL,
  \`AGENCY_TABLE_TYPE_ID\` smallint NOT NULL,
  \`MONTH_NUM\` smallint NOT NULL,
  \`DATA_YEAR\` smallint NOT NULL,
  \`REPORTED_STATUS\` varchar(1) collate utf8mb4_0900_ai_ci,
  \`REPORT_DATE\` timestamp,
  \`UPDATE_FLAG\` char(1) collate utf8mb4_0900_ai_ci,
  \`ORIG_FORMAT\` char(1) collate utf8mb4_0900_ai_ci,
  \`DATA_HOME\` varchar(1) collate utf8mb4_0900_ai_ci,
  \`DDOCNAME\` varchar(50) collate utf8mb4_0900_ai_ci,
  \`DID\` bigint,
  \`MONTH_PUB_STATUS\` int,
  \`INC_DATA_YEAR\` int,
  PRIMARY KEY (\`STATE_ID\`,\`NIBRS_MONTH_ID\`,\`DATA_YEAR\`),
  KEY \`AGENCY_TABLE_TYPE_ID\` (\`AGENCY_TABLE_TYPE_ID\`),
  KEY \`DATA_YEAR_INDEX\` (\`DATA_YEAR\`),
  KEY \`NIBRS_MONTH_ID_INDEX\` (\`NIBRS_MONTH_ID\`),
  KEY \`STATE_ID_INDEX\` (\`STATE_ID\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SQL

    cat <<CSV > nibrs_month_csv.csv
INC_DATA_YEAR,NIBRS_MONTH_ID,AGENCY_ID,MONTH_NUM,DATA_YEAR,REPORTED_STATUS,REPORT_DATE,UPDATE_FLAG,ORIG_FORMAT,DATA_HOME,DDOCNAME,DID,MONTH_PUB_STATUS,STATE_ID,AGENCY_TABLE_TYPE_ID
2019,9128595,9305,3,2019,I,2019-07-18,Y,F,C,2019_03_MN0510000_NIBRS,49502383,0,27,2
CSV

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
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "import-update-tables: update table using csv" {
    dolt sql < 1pk5col-ints-sch.sql
    run dolt table import -u test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 2, Additions: 2, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    # Validate that a successful import with no bad rows does not print the following
    ! [[ "$output" =~ "The following rows were skipped:" ]] || false

    # Run again to get correct Had No Effect amount
    run dolt table import -u test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 2, Additions: 0, Modifications: 0, Had No Effect: 2" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    # Run another update for the correct modification amount
    run dolt table import -u test 1pk5col-ints-updt.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 1, Additions: 0, Modifications: 1, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "import-update-tables: update table using csv with null (blank) values" {
    skip "nulls from csv not working correctly on update"
    dolt sql < 1pk5col-ints-sch.sql
    run dolt table import -u test `batshelper 1pk5col-nulls.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false

    # Validate that a successful import with no bad rows does not print the following
    ! [[ "$output" =~ "The following rows were skipped:" ]] || false
}

@test "import-update-tables: update table using schema with csv" {
    dolt sql < 1pk5col-ints-sch.sql
    run dolt table import -u -s `batshelper 1pk5col-ints-schema.json` test `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: schema is not supported for update or replace operations" ]] || false
}

@test "import-update-tables: update table using csv with newlines" {
    dolt sql <<SQL
CREATE TABLE test (
  pk LONGTEXT NOT NULL COMMENT 'tag:0',
  c1 LONGTEXT COMMENT 'tag:1',
  c2 LONGTEXT COMMENT 'tag:2',
  c3 LONGTEXT COMMENT 'tag:3',
  c4 LONGTEXT COMMENT 'tag:4',
  c5 LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    run dolt table import -u test `batshelper 1pk5col-strings-newlines.csv`
    [ "$status" -eq 0 ]
}

@test "import-update-tables: update table using json" {
    dolt sql < employees-sch.sql
    run dolt table import -u employees `batshelper employees-tbl.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "import-update-tables: update table using wrong json" {
    dolt sql <<SQL
CREATE TABLE employees (
  \`idz\` LONGTEXT NOT NULL COMMENT 'tag:0',
  \`first namez\` LONGTEXT COMMENT 'tag:1',
  \`last namez\` LONGTEXT COMMENT 'tag:2',
  \`titlez\` LONGTEXT COMMENT 'tag:3',
  \`start datez\` LONGTEXT COMMENT 'tag:4',
  \`end datez\` LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (idz)
);
SQL
    run dolt table import -u employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "not found in schema" ]] || false
}

@test "import-update-tables: update table using schema with json" {
    dolt sql < employees-sch.sql
    run dolt table import -u -s employees-sch.sql employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fatal: schema is not supported for update or replace operations" ]] || false
}

@test "import-update-tables: update table with existing imported data with different schema" {
  run dolt table import -c -s employees-sch.sql employees `batshelper employees-tbl.json`
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Import completed successfully." ]] || false
  run dolt table import -u employees `batshelper employees-tbl-schema-wrong.json`
  [ "$status" -eq 1 ]
  [[ "$output" =~ "not found in schema" ]] || false
}

@test "import-update-tables: update table with json when table does not exist" {
    run dolt table import -u employees `batshelper employees-tbl.json`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "The following table could not be found:" ]] || false
}

@test "import-update-tables: update table with a json with columns in different order" {
    dolt sql <<SQL
CREATE TABLE employees (
  \`id\` LONGTEXT NOT NULL COMMENT 'tag:0',
  \`first name\` LONGTEXT COMMENT 'tag:1',
  \`last name\` LONGTEXT COMMENT 'tag:2',
  \`title\` LONGTEXT COMMENT 'tag:3',
  \`start date\` LONGTEXT COMMENT 'tag:4',
  \`end date\` LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (id)
);
SQL
    run dolt table import -u employees `batshelper employees-tbl-schema-unordered.json`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt schema export employees
    [[ "$status" -eq 0 ]]
    [[ "${lines[1]}" =~ "id" ]]         || false
    [[ "${lines[2]}" =~ "first name" ]] || false
    [[ "${lines[3]}" =~ "last name" ]]  || false
    [[ "${lines[4]}" =~ "title" ]]      || false
    [[ "${lines[5]}" =~ "start date" ]] || false
    [[ "${lines[6]}" =~ "end date" ]]   || false
}

@test "import-update-tables: update table with a csv with columns in different order" {
    dolt sql <<SQL
CREATE TABLE employees (
  \`id\` LONGTEXT NOT NULL COMMENT 'tag:0',
  \`first name\` LONGTEXT COMMENT 'tag:1',
  \`last name\` LONGTEXT COMMENT 'tag:2',
  \`title\` LONGTEXT COMMENT 'tag:3',
  \`start date\` LONGTEXT COMMENT 'tag:4',
  \`end date\` LONGTEXT COMMENT 'tag:5',
  PRIMARY KEY (id)
);
SQL
    run dolt table import -u employees `batshelper employees-tbl-schema-unordered.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt schema export employees
    [[ "$status" -eq 0 ]]
    [[ "${lines[1]}" =~ "id" ]]         || false
    [[ "${lines[2]}" =~ "first name" ]] || false
    [[ "${lines[3]}" =~ "last name" ]]  || false
    [[ "${lines[4]}" =~ "title" ]]      || false
    [[ "${lines[5]}" =~ "start date" ]] || false
    [[ "${lines[6]}" =~ "end date" ]]   || false
}

@test "import-update-tables: updating table by inputting string longer than char column throws an error" {
    cat <<DELIM > 1pk1col-rpt-chars.csv
pk,c
1,"123456"
DELIM

    dolt sql < 1pk1col-char-sch.sql
    run dolt table import -u test 1pk1col-rpt-chars.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "A bad row was encountered while moving data" ]] || false
    [[ "$output" =~ "Bad Row: [1,123456]" ]] || false
    [[ "$output" =~ 'too large for column' ]] || false
}

@test "import-update-tables: update table with repeat pk in csv does not throw an error" {
    cat <<DELIM > 1pk5col-rpt-ints.csv
pk,c1,c2,c3,c4,c5
1,1,2,3,4,5
1,1,2,3,4,5
DELIM

    dolt sql < 1pk5col-ints-sch.sql
    run dolt table import -u test 1pk5col-rpt-ints.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -r csv -q "select * from test"
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[1]}" =~ "1,1,2,3,4,5" ]] || false
}

@test "import-update-tables: importing into new table renders bad rows" {
     cat <<DELIM > persons.csv
ID,LastName,FirstName,Age
1,"jon","doe", 20
2,"little","doe", 10
3,"little","doe",4
4,"little","doe",1
DELIM

    dolt sql < check-constraint-sch.sql
    run dolt table import -u persons persons.csv
    [ "$status" -eq 1 ]

    [[ "$output" =~ "A bad row was encountered while moving data" ]] || false
    [[ "$output" =~ "Bad Row:" ]] || false
    [[ "$output" =~ "[2,little,doe,10]" ]] || false

    run dolt table import -u --continue persons persons.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "The following rows were skipped:" ]] || false
    [[ "$output" =~ "[2,little,doe,10]" ]] || false
    [[ "$output" =~ "[3,little,doe,4]" ]] || false
    [[ "$output" =~ "[4,little,doe,1]" ]] || false
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -r csv -q "select * from persons"
    skip "this only worked b/c no rollback on keyless tables; this also fails on primary key tables"
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "ID,LastName,FirstName,Age" ]] || false
    [[ "$output" =~ "1,jon,doe,20" ]] || false
}

@test "import-update-tables: subsequent runs of same import with duplicate keys produces no difference in final data" {
    cat <<DELIM > 1pk5col-rpt-ints.csv
pk,c1,c2,c3,c4,c5
1,1,2,3,4,5
1,1,2,3,4,7
1,1,2,3,4,8
DELIM

    dolt sql < 1pk5col-ints-sch.sql
    dolt table import -u --continue test 1pk5col-rpt-ints.csv
    dolt commit -am "cm1"

    run dolt table import -u --continue test 1pk5col-rpt-ints.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Modifications: 3" ]] || falsa

    skip_nbf_dolt_1
    run dolt diff
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

@test "import-update-tables: importing some columns does not overwrite columns not part of the import" {
  dolt sql <1pk5col-ints-sch.sql
  echo -e 'pk,c1\n1,1\n2,6'|dolt table import -u test
  echo -e 'pk,c2\n1,2\n2,7'|dolt table import -u test
  echo -e 'pk,c3,c4,c5\n1,3,4,5\n2,8,9,10'|dolt table import -u test

  EXPECTED=$(echo -e "pk,c1,c2,c3,c4,c5\n1,1,2,3,4,5\n2,6,7,8,9,10")
  run dolt sql -r csv -q 'SELECT * FROM test'
  [ "$status" -eq 0 ]
  [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "import-update-tables: poorly written file correctly errors" {
   cat <<DELIM > bad-updates.csv
pk,v1
5,5,
6,6,
DELIM

    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 2 NOT NULL, v2 int)"
    dolt sql -q "INSERT INTO test (pk, v1, v2) VALUES (1, 2, 3), (2, 3, 4)"

    run dolt table import -u test bad-updates.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "A bad row was encountered while moving data" ]] || false
    [[ "$output" =~ "csv reader's schema expects 2 fields, but line only has 3 values" ]] || false

    run dolt table import -u --continue test bad-updates.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Lines skipped: 2" ]] || false
}

@test "import-update-tables: error during primary key table just skips" {
   cat <<DELIM > bad-updates.csv
pk
1
2
100
3
DELIM

    dolt sql -q "CREATE TABLE test(pk int PRIMARY KEY CHECK (pk < 10))"

    run dolt table import -u test bad-updates.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "A bad row was encountered while moving data" ]] || false
    [[ "$output" =~ "[100]" ]] || false

    run dolt table import -u --continue test bad-updates.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Lines skipped: 1" ]] || false

    run dolt sql -r csv -q "select * from test"
    skip "table editors need to handle continue flag"
    [ "${#lines[@]}" -eq 4 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
}

@test "import-update-tables: compare tables in database with table imported from parquet file" {
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
    dolt table import -u testTypes test.parquet
    dolt add .
    dolt commit --allow-empty -m "update table from parquet file"

    skip_nbf_dolt_1
    run dolt diff --summary main new_branch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "import-update-tables: Subsequent updates with --continue correctly work" {
   dolt sql -q "create table t (pk int primary key, val varchar(1))"
   cat <<DELIM > file1.csv
pk,val
1,a
2,b
DELIM
   cat <<DELIM > file2.csv
pk,val
1,c
2,gps
3,v
DELIM
  cat <<DELIM > file3.csv
pk,val
1,d
4,fg
dsadas,de
DELIM

    run dolt table import -u --continue t file1.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 2, Additions: 2, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -r csv -q "select * from t"
    [ "${lines[1]}" = "1,a" ]
    [ "${lines[2]}" = "2,b" ]

    run dolt table import -u --continue t file2.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 1, Modifications: 2, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    ! [[ "$output" =~ "The following rows were skipped:" ]] || false

    run dolt sql -r csv -q "select * from t"
    [ "${lines[1]}" = "1,c" ]
    [ "${lines[2]}" = "2,g" ]
    [ "${lines[3]}" = "3,v" ]

    run dolt table import -u --continue t file3.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 2, Modifications: 1, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
    ! [[ "$output" =~ "The following rows were skipped:" ]] || false

    run dolt sql -r csv -q "select * from t order by pk"
    [ "${lines[1]}" = "0,d" ]
    [ "${lines[2]}" = "1,d" ]
    [ "${lines[3]}" = "2,g" ]
    [ "${lines[4]}" = "3,v" ]
    [ "${lines[5]}" = "4,f" ]

    run dolt sql -q "select count(*) from t"
    [[ "$output" =~ "5" ]] || false
}

@test "import-update-tables: string too large for column regression" {
    dolt sql < nibrs_month_sch.sql
    run dolt table import -u test nibrs_month_csv.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false
}

@test "import-update-tables: csv subsetting throws error with not null column" {
    cat <<SQL > 1pk5col-ints-def-sch.sql
CREATE TABLE test (
  pk int NOT NULL COMMENT 'tag:0',
  c1 int,
  c2 int,
  c3 int,
  c4 int NOT NULL,
  c5 int,
  PRIMARY KEY (pk)
);
SQL
     cat <<DELIM > 1pk5col-ints-updt.csv
pk,c1,c2,c5,c3
0,1,2,6,3
DELIM

    dolt sql < 1pk5col-ints-def-sch.sql

    run dolt table import -u test 1pk5col-ints-updt.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Field 'c4' doesn't have a default value" ]] || false
}

@test "import-update-tables: csv subsetting but with defaults" {
   cat <<SQL > 1pk5col-ints-def-sch.sql
CREATE TABLE test (
  pk int NOT NULL COMMENT 'tag:0',
  c1 int,
  c2 int,
  c3 int,
  c4 int DEFAULT 42,
  c5 int,
  PRIMARY KEY (pk)
);
SQL
     cat <<DELIM > 1pk5col-ints-updt.csv
pk,c1,c2,c5,c3
0,1,2,6,3
DELIM

    dolt sql < 1pk5col-ints-def-sch.sql

    run dolt table import -u test 1pk5col-ints-updt.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -r csv -q "select * from test"
    [ "${lines[1]}" = "0,1,2,3,42,6" ]

    run dolt sql -q "select count(*) from test"
    [[ "$output" =~ "1" ]] || false
}

@test "import-update-tables: csv files has less columns that schema -u" {
    cat <<DELIM > 1pk5col-ints-updt.csv
pk,c1,c2,c5,c3
0,1,2,6,3
DELIM

    dolt sql < 1pk5col-ints-sch.sql

    run dolt table import -u test 1pk5col-ints-updt.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -r csv -q "select * from test"
    [ "${lines[1]}" = "0,1,2,3,,6" ]

    run dolt sql -q "select count(*) from test"
    [[ "$output" =~ "1" ]] || false
}

@test "import-update-tables: csv files has same number of columns but different order than schema" {
    cat <<DELIM > 1pk5col-ints-updt.csv
pk,c2,c4,c5,c1,c3
0,2,4,6,1,3
DELIM

    dolt sql < 1pk5col-ints-sch.sql

    run dolt table import -u test 1pk5col-ints-updt.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -r csv -q "select * from test"
    [ "${lines[1]}" = "0,1,2,3,4,6" ]

    run dolt sql -q "select count(*) from test"
    [[ "$output" =~ "1" ]] || false
}

@test "import-update-tables: csv files has more column than schema and different order" {
    cat <<DELIM > 1pk5col-ints-updt.csv
pk,c2,c4,c5,c1,c3,c7
0,2,4,6,1,3,100
DELIM

    dolt sql < 1pk5col-ints-sch.sql

    run dolt table import -u test 1pk5col-ints-updt.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -r csv -q "select * from test"
    [ "${lines[1]}" = "0,1,2,3,4,6" ]

    run dolt sql -q "select count(*) from test"
    [[ "$output" =~ "1" ]] || false
}

@test "import-update-tables: just update one column in a big table" {
    cat <<DELIM > 1pk5col-ints-updt.csv
pk,c2
0,7
DELIM

    dolt sql < 1pk5col-ints-sch.sql

    dolt sql -q "insert into test values (0,1,2,3,4,5)"

    run dolt table import -u test 1pk5col-ints-updt.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 1, Additions: 0, Modifications: 1, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -r csv -q "select * from test"
    [ "${lines[1]}" = "0,1,7,3,4,5" ]
}

@test "import-update-tables: updating a table with no primary keys complains" {
    cat <<DELIM > 1pk5col-ints-updt.csv
c2
70
DELIM

    dolt sql < 1pk5col-ints-sch.sql

    run dolt table import -u test 1pk5col-ints-updt.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error determining the output schema." ]] || false
}

@test "import-update-tables: partial update on keyless table" {
     cat <<SQL > schema.sql
CREATE TABLE keyless (
    c0 INT,
    c1 INT DEFAULT 42,
    c2 INT
);
SQL

    dolt sql < schema.sql
    dolt sql -q "insert into keyless values (0,1,0)"

    cat <<DELIM > data.csv
c0,c2
0,2
DELIM

    run dolt table import -u keyless data.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -r csv -q "select * from keyless order by c0, c1 DESC"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [ "${lines[1]}" = "0,42,2" ]
    [ "${lines[2]}" = "0,1,0" ]
}

@test "import-update-tables: --ignore-skipped-rows correctly prevents skipped rows from printing" {
   cat <<DELIM > persons.csv
ID,LastName,FirstName,Age
1,"jon","doe", 20
2,"little","doe", 10
3,"little","doe",4
4,"little","doe",1
DELIM

    dolt sql < check-constraint-sch.sql

    run dolt table import -u --continue --ignore-skipped-rows persons persons.csv
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "The following rows were skipped:" ]] || false
    ! [[ "$output" =~ "[2,little,doe,10]" ]] || false
    ! [[ "$output" =~ "[3,little,doe,4]" ]] || false
    ! [[ "$output" =~ "[4,little,doe,1]" ]] || false
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt sql -r csv -q "select * from persons"
    [[ "$output" =~ "1,jon,doe,20" ]] || false
}

@test "import-update-tables: large amounts of no effect rows" {
    dolt sql -q "create table t(pk int primary key)"
    dolt sql -q "alter table t add constraint cx CHECK (pk < 10)"
    dolt sql -q "Insert into t values (1),(2),(3),(4),(5),(6),(7),(8),(9) "

    cat <<DELIM > file.csv
pk
1
2
3
4
5
6
10000
DELIM

    run dolt table import -u --continue t file.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 6, Additions: 0, Modifications: 0, Had No Effect: 6" ]] || false
    [[ "$output" =~ "The following rows were skipped:" ]] || false
    [[ "$output" =~ "[10000]" ]] || false

    run dolt sql -r csv -q "select * from t"
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "4" ]] || false
    [[ "$output" =~ "5" ]] || false
    [[ "$output" =~ "6" ]] || false
}

@test "import-update-tables: import supports tables with dashes in the name" {
    cat <<DELIM > file.csv
pk,c1
0,0
DELIM

    run dolt table import -c this-is-a-table file.csv
    [ $status -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false

    run dolt table import -u this-is-a-table file.csv
    [ $status -eq 0 ]

    run dolt sql -r csv -q "SELECT * FROM \`this-is-a-table\`"
    [ $status -eq 0 ]
    [[ "$output" =~ "pk,c1" ]] || false
    [[ "$output" =~ "0,0" ]] || false
}

@test "import-update-tables: successfully update child table in fk relationship" {
    cat <<DELIM > objects-good.csv
id,name,color
4,laptop,blue
5,dollar,green
6,bottle,red
DELIM

    run dolt table import -u objects objects-good.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false

    run dolt sql -r csv -q "SELECT * FROM objects where id >= 4"
    [ $status -eq 0 ]
    [[ "$output" =~ "id,name,color" ]] || false
    [[ "$output" =~ "4,laptop,blue" ]] || false
    [[ "$output" =~ "5,dollar,green" ]] || false
    [[ "$output" =~ "6,bottle,red" ]] || false
}

@test "import-update-tables: unsuccessfully update child table in fk relationship" {
    cat <<DELIM > objects-bad.csv
id,name,color
4,laptop,blue
5,dollar,green
6,bottle,gray
DELIM

    run dolt table import -u objects objects-bad.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "A bad row was encountered while moving data" ]] || false
    [[ "$output" =~ "Bad Row: [6,bottle,gray]" ]] || false
    [[ "$output" =~ "cannot add or update a child row - Foreign key violation" ]] || false

    run dolt table import -u objects objects-bad.csv --continue
    [ "$status" -eq 0 ]
    [[ "$output" =~ "The following rows were skipped:" ]] || false
    [[ "$output" =~ "[6,bottle,gray]" ]] || false
    [[ "$output" =~ "Rows Processed: 2, Additions: 2, Modifications: 0, Had No Effect: 0" ]] || false

    run dolt sql -r csv -q "SELECT * FROM objects where id >= 4"
    [ $status -eq 0 ]
    [[ "$output" =~ "id,name,color" ]] || false
    [[ "$output" =~ "4,laptop,blue" ]] || false
    [[ "$output" =~ "5,dollar,green" ]] || false
    ! [[ "$output" =~ "6,bottle,red" ]] || false
}

@test "import-update-tables: successfully update child table in multi-key fk relationship " {
    skip_nbf_dolt_1
    dolt sql -q "drop table objects"
    dolt sql -q "drop table colors"

    dolt sql <<SQL
CREATE TABLE colors (
    id INT NOT NULL,
    color VARCHAR(32) NOT NULL,

    PRIMARY KEY (id),
    INDEX color_index(color)
);
CREATE TABLE materials (
    id INT NOT NULL,
    material VARCHAR(32) NOT NULL,
    color VARCHAR(32),

    PRIMARY KEY(id),
    FOREIGN KEY (color) REFERENCES colors(color),
    INDEX color_mat_index(color, material)
);
CREATE TABLE objects (
    id INT NOT NULL,
    name VARCHAR(64) NOT NULL,
    color VARCHAR(32),
    material VARCHAR(32),

    PRIMARY KEY(id),
    FOREIGN KEY (color,material) REFERENCES materials(color,material)
);
INSERT INTO colors (id,color) VALUES (1,'red'),(2,'green'),(3,'blue'),(4,'purple'),(10,'brown');
INSERT INTO materials (id,material,color) VALUES (1,'steel','red'),(2,'rubber','green'),(3,'leather','blue'),(10,'dirt','brown'),(11,'air',NULL);
INSERT INTO objects (id,name,color,material) VALUES (1,'truck','red','steel'),(2,'ball','green','rubber'),(3,'shoe','blue','leather'),(11,'tornado',NULL,'air');
SQL

    cat <<DELIM > multi-key-good.csv
id,name,color,material
4,laptop,red,steel
5,dollar,green,rubber
6,bottle,blue,leather
DELIM

    run dolt table import -u objects multi-key-good.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false

    run dolt sql -r csv -q "SELECT * FROM objects where id >= 4 ORDER BY id"
    [ $status -eq 0 ]
    [[ "$output" =~ "id,name,color,material" ]] || false
    [[ "$output" =~ "4,laptop,red,steel" ]] || false
    [[ "$output" =~ "5,dollar,green,rubber" ]] || false
    [[ "$output" =~ "6,bottle,blue,leather" ]] || false

    cat <<DELIM > multi-key-bad.csv
id,name,color,material
4,laptop,red,steel
5,dollar,green,rubber
6,bottle,blue,steel
DELIM

    run dolt table import -u objects multi-key-bad.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "A bad row was encountered while moving data" ]] || false
    [[ "$output" =~ "Bad Row: [6,bottle,blue,steel]" ]] || false
    [[ "$output" =~ "cannot add or update a child row - Foreign key violation" ]] || false

    run dolt table import -u objects multi-key-bad.csv --continue
    [ "$status" -eq 0 ]
    [[ "$output" =~ "The following rows were skipped:" ]] || false
    [[ "$output" =~ "[6,bottle,blue,steel]" ]] || false
    [[ "$output" =~ "Rows Processed: 2, Additions: 0, Modifications: 0, Had No Effect: 2" ]] || false

    run dolt sql -r csv -q "SELECT * FROM objects where id >= 4 ORDER BY id"
    [ $status -eq 0 ]
    [[ "$output" =~ "id,name,color,material" ]] || false
    [[ "$output" =~ "4,laptop,red,steel" ]] || false
    [[ "$output" =~ "5,dollar,green,rubber" ]] || false
    ! [[ "$output" =~ "6,bottle,blue,steel" ]] || false
}

@test "import-update-tables: import update with CASCADE ON UPDATE" {
   skip_nbf_dolt_1
   dolt sql <<SQL
CREATE TABLE one (
  pk int PRIMARY KEY,
  v1 int,
  v2 int
);
ALTER TABLE one ADD INDEX v1 (v1);
CREATE TABLE two (
  pk int PRIMARY KEY,
  v1 int,
  v2 int,
  CONSTRAINT fk_name_1 FOREIGN KEY (v1)
    REFERENCES one(v1)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);
ALTER TABLE two ADD INDEX v1v2 (v1, v2);
CREATE TABLE three (
  pk int PRIMARY KEY,
  v1 int,
  v2 int,
  CONSTRAINT fk_name_2 FOREIGN KEY (v1, v2)
    REFERENCES two(v1, v2)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);
INSERT INTO one VALUES (1, 1, 4), (2, 2, 5), (3, 3, 6), (4, 4, 5);
INSERT INTO two VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3), (5, 4, 4);
INSERT INTO three VALUES (3, 1, 1), (4, 2, 2), (5, 3, 3), (6, 4, 4);
SQL

    cat <<DELIM > table-one.csv
pk,v1,v2
1,2,2
DELIM

    run dolt table import -u one table-one.csv
    [ $status -eq 0 ]
    [[ "$output" =~ "Rows Processed: 1, Additions: 0, Modifications: 1, Had No Effect: 0" ]] || false

    run dolt sql -r csv -q "select * from two where pk = 2"
    [ $status -eq 0 ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "2,2,1" ]] || false

    run dolt sql -r csv -q "select * from three where pk = 3"
    [ $status -eq 0 ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "3,2,1" ]] || false
}

@test "import-update-tables: unsuccessfully update parent table in fk relationship" {
    cat <<DELIM > colors-bad.csv
id,color
3,dsadasda
5,yellow
DELIM

    run dolt table import -u colors colors-bad.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "A bad row was encountered while moving data" ]] || false
    [[ "$output" =~ "cannot delete or update a parent row" ]] || false

    run dolt table import -u colors colors-bad.csv --continue
    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "SELECT * from colors where id in (3,5)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "id,color" ]] || false
    [[ "$output" =~ "3,blue" ]] || false
    [[ "$output" =~ "5,yellow" ]] || false
}

@test "import-update-tables: circular foreign keys" {
    dolt sql <<SQL
CREATE TABLE tbl (
    id int PRIMARY KEY,
    v1 int,
    v2 int,
    INDEX v1 (v1),
    INDEX v2 (v2)
);
ALTER TABLE tbl ADD CONSTRAINT fk_named FOREIGN KEY (v2) REFERENCES tbl(v1) ON UPDATE CASCADE ON DELETE CASCADE;
INSERT INTO tbl VALUES (1,1,1), (2, 2, 1), (3, 3, NULL);
SQL

    cat <<DELIM > circular-keys-good.csv
id,v1,v2
4,4,2
DELIM

    run dolt table import -u tbl circular-keys-good.csv
    [ $status -eq 0 ]
    [[ "$output" =~ "Rows Processed: 1, Additions: 1, Modifications: 0, Had No Effect: 0" ]] || false

    cat <<DELIM > circular-keys-bad.csv
id,v1,v2
5,5,1
6,6,1000
DELIM

    run dolt table import -u tbl circular-keys-bad.csv
    [ $status -eq 1 ]
    [[ "$output" =~ "A bad row was encountered while moving data" ]] || false
    [[ "$output" =~ "cannot add or update a child row" ]] || false
}

@test "import-update-tables: disable foreign key checks" {
    skip_nbf_dolt_1
    cat <<DELIM > objects-bad.csv
id,name,color
4,laptop,blue
5,dollar,green
6,bottle,gray
DELIM

    run dolt table import -u objects objects-bad.csv --disable-fk-checks
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows Processed: 3, Additions: 3, Modifications: 0, Had No Effect: 0" ]] || false

    run dolt sql -r csv -q "select * from objects where id = 6"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6,bottle,gray" ]] || false

    run dolt constraints verify objects
    [ "$status" -eq 1 ]
    [[ "$output" =~ "All constraints are not satisfied" ]] || false
}
