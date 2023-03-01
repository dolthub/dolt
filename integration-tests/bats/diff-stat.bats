#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

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
}

teardown() {
    assert_feature_version
    teardown_common
}



@test "diff-stat: stat/summary comparing working table to last commit" {
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "table created"
    dolt sql -q "insert into test values (2, 11, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (3, 11, 0, 0, 0, 0)"

    run dolt diff --stat
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2 Rows Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "2 Rows Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "12 Cells Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(2 Row Entries vs 4 Row Entries)" ]] || false

    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| Table name | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| test       | modified  | true        | false         |" ]] || false

    dolt add test
    dolt commit -m "added two rows"
    dolt sql -q "replace into test values (0, 11, 0, 0, 0, 6)"
    run dolt diff --stat
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified (75.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (25.00%)" ]] || false
    [[ "$output" =~ "0 Cells Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "2 Cells Modified (8.33%)" ]] || false
    [[ "$output" =~ "(4 Row Entries vs 4 Row Entries)" ]] || false


    run dolt diff --summary
    [ "$status" -eq 0 ]
    echo "$output"
    [[ "$output" =~ "| Table name | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| test       | modified  | true        | false         |" ]] || false

    dolt add test
    dolt commit -m "modified first row"
    dolt sql -q "delete from test where pk = 0"
    run dolt diff --stat
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified (75.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Deleted (25.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Added (0.00%)" ]] || false
    [[ "$output" =~ "6 Cells Deleted (25.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(4 Row Entries vs 3 Row Entries)" ]] || false


    run dolt diff --summary
    [ "$status" -eq 0 ]
    echo "$output"
    [[ "$output" =~ "| Table name | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| test       | modified  | true        | false         |" ]] || false
}

@test "diff-stat: stat/summary comparing row with a deleted cell and an added cell" {
    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| Table name | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| test       | added     | false       | true          |" ]] || false

    dolt add test
    dolt commit -m "create table"

    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "put row"
    dolt sql -q "replace into test (pk, c1, c3, c4, c5) values (0, 1, 3, 4, 5)"

    run dolt diff --stat
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0 Rows Unmodified (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (100.00%)" ]] || false
    [[ "$output" =~ "0 Cells Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Cell Modified (16.67%)" ]] || false
    [[ "$output" =~ "(1 Row Entry vs 1 Row Entry)" ]] || false

    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| Table name | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| test       | modified  | true        | false         |" ]] || false

    dolt add test
    dolt commit -m "row modified"
    dolt sql -q "replace into test values (0, 1, 2, 3, 4, 5)"
    run dolt diff --stat
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0 Rows Unmodified (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (100.00%)" ]] || false
    [[ "$output" =~ "0 Cells Added (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Cell Modified (16.67%)" ]] || false
    [[ "$output" =~ "(1 Row Entry vs 1 Row Entry)" ]] || false

    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| Table name | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| test       | modified  | true        | false         |" ]] || false
}

@test "diff-stat: stat/summary comparing two branches" {
    dolt checkout -b firstbranch
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "Added one row"
    dolt checkout -b newbranch
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "Added another row"

    run dolt diff --stat firstbranch newbranch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1 Row Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "1 Row Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "6 Cells Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(1 Row Entry vs 2 Row Entries)" ]] || false

    run dolt diff --summary firstbranch newbranch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| Table name | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| test       | modified  | true        | false         |" ]] || false

    run dolt diff --stat firstbranch..newbranch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1 Row Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "1 Row Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "6 Cells Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(1 Row Entry vs 2 Row Entries)" ]] || false

    run dolt diff --summary firstbranch..newbranch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| Table name | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| test       | modified  | true        | false         |" ]] || false
}

@test "diff-stat: stat/summary shows correct changes after schema change" {
    
    cat <<DELIM > employees.csv
"id","first name","last name","title","start date","end date"
0,tim,sehn,ceo,"",""
1,aaron,son,founder,"",""
2,brian,hendricks,founder,"",""
DELIM
    dolt table import -c -pk=id employees employees.csv
    dolt add employees
    dolt commit -m "Added employees table with data"
    
    dolt sql -q "alter table employees add city longtext"
    dolt sql -q "insert into employees values (3, 'taylor', 'bantle', 'software engineer', '', '', 'Santa Monica')"

    run dolt diff --stat
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3 Rows Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "1 Row Added (33.33%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "10 Cells Added (55.56%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(3 Row Entries vs 4 Row Entries)" ]] || false

    run dolt diff --summary 
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| Table name | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| employees  | modified  | true        | true          |" ]] || false

    dolt sql -q "replace into employees values (0, 'tim', 'sehn', 'ceo', '2 years ago', '', 'Santa Monica')"
    
    dolt diff --stat
    run dolt diff --stat
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2 Rows Unmodified (66.67%)" ]] || false
    [[ "$output" =~ "1 Row Added (33.33%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "1 Row Modified (33.33%)" ]] || false
    [[ "$output" =~ "10 Cells Added (55.56%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "2 Cells Modified (11.11%)" ]] || false
    [[ "$output" =~ "(3 Row Entries vs 4 Row Entries)" ]] || false

    run dolt diff --summary 
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| Table name | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| employees  | modified  | true        | true          |" ]] || false
}

@test "diff-stat: stat/summary gets summaries for all tables with changes" {
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt sql <<SQL
CREATE TABLE employees (
  \`id\` varchar(20) NOT NULL,
  \`first name\` LONGTEXT,
  \`last name\` LONGTEXT,
  \`title\` LONGTEXT,
  \`start date\` LONGTEXT,
  \`end date\` LONGTEXT,
  PRIMARY KEY (id)
);
SQL
    dolt sql -q "insert into employees values (0, 'tim', 'sehn', 'ceo', '', '')"
    dolt add test employees
    dolt commit -m "test tables created"
    dolt sql -q "insert into test values (2, 11, 0, 0, 0, 0)"
    dolt sql -q "insert into employees values (1, 'brian', 'hendriks', 'founder', '', '')"

    run dolt diff --stat
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/test b/test" ]] || false
    [[ "$output" =~ "--- a/test @" ]] || false
    [[ "$output" =~ "+++ b/test @" ]] || false
    [[ "$output" =~ "diff --dolt a/employees b/employees" ]] || false
    [[ "$output" =~ "--- a/employees @" ]] || false
    [[ "$output" =~ "+++ b/employees @" ]] || false

    run dolt diff --summary 
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| Table name | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| test       | modified  | true        | false         |" ]] || false
    [[ "$output" =~ "| employees  | modified  | true        | false         |" ]] || false

    run dolt diff --stat employees
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/employees b/employees" ]] || false
    [[ "$output" =~ "--- a/employees @" ]] || false
    [[ "$output" =~ "+++ b/employees @" ]] || false

    run dolt diff --summary employees
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| Table name | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| employees  | modified  | true        | false         |" ]] || false
}

@test "diff-stat: two and three dot diff stat/summary" {
    dolt checkout main
    dolt sql -q 'insert into test values (0,0,0,0,0,0)'
    dolt add .
    dolt commit -m table
    dolt checkout -b branch1
    dolt sql -q 'insert into test values (1,1,1,1,1,1)'
    dolt add .
    dolt commit -m row
    dolt checkout main
    dolt sql -q 'insert into test values (2,2,2,2,2,2)'
    dolt add .
    dolt commit -m newrow

    run dolt diff main..branch1 --stat
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1 Row Unmodified (50.00%)" ]] || false
    [[ "$output" =~ "1 Row Added (50.00%)" ]] || false
    [[ "$output" =~ "1 Row Deleted (50.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "6 Cells Added (50.00%)" ]] || false
    [[ "$output" =~ "6 Cells Deleted (50.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(2 Row Entries vs 2 Row Entries)" ]] || false

    run dolt diff main..branch1 --summary 
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| Table name | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| test       | modified  | true        | false         |" ]] || false

    run dolt diff main...branch1 --stat
    echo $output
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1 Row Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "1 Row Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "6 Cells Added (100.00%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(1 Row Entry vs 2 Row Entries)" ]] || false

    run dolt diff main...branch1 --summary 
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| Table name | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| test       | modified  | true        | false         |" ]] || false
}

@test "diff-stat: diff stat incorrect primary key set change regression test" {
    dolt sql -q "create table testdrop (col1 varchar(20), id int primary key, col2 varchar(20))"
    dolt add .
    dolt sql -q "insert into testdrop values ('test1', 1, 'test2')"
    dolt commit -am "Add testdrop table"

    dolt sql -q "alter table testdrop drop column col1"
    run dolt diff --stat
    [ $status -eq 0 ]
    [[ $output =~ "1 Row Modified (100.00%)" ]]
}

@test "diff-stat: stat/summary for renamed table" {
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "table created"

    dolt sql -q "alter table test rename to test2"
    run dolt diff --stat
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No data changes. See schema changes by using -s or --schema." ]] || false

    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| Table name    | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| test -> test2 | renamed   | false       | true          |" ]] || false

    dolt sql -q "insert into test2 values (2, 2, 2, 2, 2, 2)"
    run dolt diff --stat
    [ "$status" -eq 0 ]
    echo "$output"
    [[ "$output" =~ "2 Rows Unmodified (100.00%)" ]] || false
    [[ "$output" =~ "1 Row Added (50.00%)" ]] || false
    [[ "$output" =~ "0 Rows Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Rows Modified (0.00%)" ]] || false
    [[ "$output" =~ "6 Cells Added (50.00%)" ]] || false
    [[ "$output" =~ "0 Cells Deleted (0.00%)" ]] || false
    [[ "$output" =~ "0 Cells Modified (0.00%)" ]] || false
    [[ "$output" =~ "(2 Row Entries vs 3 Row Entries)" ]] || false

    run dolt diff --summary
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| Table name    | Diff type | Data change | Schema change |" ]] || false
    [[ "$output" =~ "| test -> test2 | renamed   | true        | true          |" ]] || false
}