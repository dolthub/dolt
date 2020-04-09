#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE test_int (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
CREATE TABLE test_string (
  pk LONGTEXT NOT NULL COMMENT 'tag:6',
  c1 LONGTEXT COMMENT 'tag:7',
  c2 LONGTEXT COMMENT 'tag:8',
  c3 LONGTEXT COMMENT 'tag:9',
  c4 LONGTEXT COMMENT 'tag:10',
  c5 LONGTEXT COMMENT 'tag:11',
  PRIMARY KEY (pk)
);
SQL
}

teardown() {
    teardown_common
}

@test "table export sql datetime" {
    skip "dates should be quoted"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT PRIMARY KEY,
  v DATETIME
);
SQL
    dolt sql -q "INSERT INTO test VALUES (1, '2020-04-08 11:11:11'), (2, '2020-04-08 12:12:12')"
    dolt table export test test.sql
    run cat test.sql
    [[ "$output" =~ 'INSERT INTO `test` (`pk`,`v`) VALUES (1,"2020-04-08 11:11:11");' ]] || false
    [[ "$output" =~ 'INSERT INTO `test` (`pk`,`v`) VALUES (2,"2020-04-08 12:12:12");' ]] || false
}

@test "dolt table import from stdin export to stdout" {
    skiponwindows "Need to install python before this test will work."
    echo 'pk,c1,c2,c3,c4,c5
0,1,2,3,4,5
9,8,7,6,5,4
'|dolt table import -u test_int
    dolt table export --file-type=csv test_int|python -c '
import sys
rows = []
for line in sys.stdin:
    line = line.strip()

    if line != "":
        rows.append(line.strip().split(","))

if len(rows) != 3:
    sys.exit(1)

if rows[0] != "pk,c1,c2,c3,c4,c5".split(","):
    sys.exit(1)

if rows[1] != "0,1,2,3,4,5".split(","):
    sys.exit(1)

if rows[2] != "9,8,7,6,5,4".split(","):
    sys.exit(1)
'
}

@test "dolt table export" {
    dolt sql -q "insert into test_int values (0, 1, 2, 3, 4, 5)"
    run dolt table export test_int export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f export.csv ]
    run grep 5 export.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    run dolt table export test_int export.csv
    [ "$status" -ne 0 ]
    [[ "$output" =~ "export.csv already exists" ]] || false
    run dolt table export -f test_int export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f export.csv ]
}

@test "dolt table SQL export" {
    dolt sql -q "insert into test_int values (0, 1, 2, 3, 4, 5)"
    run dolt table export test_int export.sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f export.sql ]
    diff --strip-trailing-cr $BATS_TEST_DIRNAME/helper/1pk5col-ints.sql export.sql
}

@test "export a table with a string with commas to csv" {
    run dolt sql -q "insert into test_string values ('tim', 'is', 'super', 'duper', 'rad', 'a,b,c,d,e')"
    [ "$status" -eq 0 ]
    run dolt table export test_string export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] ||  false
    grep -E \"a,b,c,d,e\" export.csv
}

@test "export a table with a string with double quotes to csv" {
    run dolt sql -q 'insert into test_string (pk,c1,c5) values ("this", "is", "a ""quotation""");'
    [ "$status" -eq 0 ]
    run dolt table export test_string export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] ||  false
    grep '"a ""quotation"""' export.csv
}

@test "export a table with a string with new lines to csv" {
    run dolt sql -q 'insert into test_string (pk,c1,c5) values ("this", "is", "a new \n line");'
    [ "$status" -eq 0 ]
    run dolt table export test_string export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] ||  false

    # output will be slit over two lines
    grep 'this,is,,,,"a new ' export.csv
    grep ' line"' export.csv
}

@test "table with column with not null constraint can be exported and reimported" {
    dolt sql -q "CREATE TABLE person_info(name VARCHAR(255) NOT NULL,location VARCHAR(255) NOT NULL,age BIGINT NOT NULL,PRIMARY KEY (name));"
    dolt add .
    dolt commit -m 'add person_info table'
    dolt sql -q "INSERT INTO person_info (name, location, age) VALUES ('kevern smith', 'los angeles', 21);"
    dolt sql -q "INSERT INTO person_info (name, location, age) VALUES ('barbara smith', 'los angeles', 24);"

    # insert empty value in not null column
    dolt sql -q "INSERT INTO person_info (name, location, age) VALUES ('gary busy', '', 900);"
    dolt sql -q "INSERT INTO person_info (name, location, age) VALUES ('the tampa bay buccs', 'florida', 123);"
    dolt sql -q "INSERT INTO person_info (name, location, age) VALUES ('michael fakeperson', 'fake city', 39);"

    # create csvs
    dolt sql -r csv -q 'select * from person_info' > sql-csv.csv
    dolt table export person_info export-csv.csv
    dolt checkout person_info

    skip "Exported csv should handle not null contrained empty values so csv can be reimported" run dolt table import -u person_info sql-csv.csv
    [ "$status" -eq 0 ]

    run dolt table import -u person_info export-csv.csv
    [ "$status" -eq 0 ]
}
