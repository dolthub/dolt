#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
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
}

teardown() {
    teardown_common
}

@test "export a table with a string with commas to csv" {
    run dolt sql -q "insert into test values ('tim', 'is', 'super', 'duper', 'rad', 'a,b,c,d,e')"
    [ "$status" -eq 0 ]
    run dolt table export test export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] ||  false
    grep -E \"a,b,c,d,e\" export.csv
}

@test "export a table with a string with double quotes to csv" {
    run dolt sql -q 'insert into test (pk,c1,c5) values ("this", "is", "a ""quotation""");'
    [ "$status" -eq 0 ]
    run dolt table export test export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] ||  false
    grep '"a ""quotation"""' export.csv
}

@test "export a table with a string with new lines to csv" {
    run dolt sql -q 'insert into test (pk,c1,c5) values ("this", "is", "a new \n line");'
    [ "$status" -eq 0 ]
    run dolt table export test export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] ||  false

    # output will be slit over two lines
    grep 'this,is,,,,"a new ' export.csv
    grep ' line"' export.csv
}

@test "dolt sql with string comparison operators" {
    dolt sql -q "insert into test values ('tim', 'is', 'super', 'duper', 'rad', 'fo sho')"
    dolt sql -q "insert into test values ('zach', 'is', 'super', 'duper', 'not', 'rad')"
    dolt sql -q "insert into test values ('this', 'test', 'is', 'a', 'good', 'test')"
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    # All row counts are offset by 4 to account for table printing
    [ "${#lines[@]}" -eq 7 ]
    run dolt sql -q "select * from test where pk='tim'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select * from test where pk>'tim'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select * from test where pk>='tim'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    run dolt sql -q "select * from test where pk<>'tim'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    run dolt sql -q "select * from test where pk='bob'"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
}

@test "interact with a strings type table with sql" {
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values ('tim','is','super','duper','rad','fo sho')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 1       |" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "c5" ]] || false
    [[ "$output" =~ "tim" ]] || false
    run dolt sql -q "select pk,c1,c4 from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "c4" ]] || false
    [[ "$output" =~ "tim" ]] || false
    [[ ! "$output" =~ "super" ]] || false
}

@test "insert must use quoted strings" {
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (tim,is,super,duper,rad,'fo sho')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error parsing SQL" ]] || false
}

@test "create and view a table with NULL and empty string values" {
    dolt sql -q "insert into test values ('tim', '', '', '', '', '')"
    dolt sql -q "insert into test (pk) values ('aaron')"
    dolt sql -q "insert into test (pk) values ('brian')"
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    # select orders by primary key right now so aaron, brian, tim
    [[ "${lines[4]}" =~ "<NULL>" ]] || false
    [[ ! "${lines[5]}" =~ "<NULL>" ]] || false
    doltselectoutput=$output
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "<NULL>" ]] || false
    [ "$output" = "$doltselectoutput" ]
    # Make sure we don't get a table with no spaces because that bug was
    # generated when making changes to NULL printing
    [[ ! "$output" =~ "|||||" ]] || false
}

@test "semicolons in quoted sql statements" {
    run dolt sql -q "insert into test (pk,c1) values ('test', 'this; should; work')"
    [ "$status" -eq 0 ]
    run dolt sql <<< "insert into test (pk,c1) values ('test2', 'this; should; work')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows inserted: 1" ]]
    run dolt sql <<< "insert into test (pk,c1) values ('test3', 'this \\\\'' should \\\\'' work')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows inserted: 1" ]]
}

@test "table with column with not null constraint can be exported and reimported" {
    dolt sql -q "CREATE TABLE person_info(name VARCHAR(255) NOT NULL COMMENT 'tag:6',location VARCHAR(255) NOT NULL COMMENT 'tag:7',age BIGINT NOT NULL COMMENT 'tag:8',PRIMARY KEY (name));"
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
