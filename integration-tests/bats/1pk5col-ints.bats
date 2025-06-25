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

@test "1pk5col-ints: empty table" {
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "test" ]] || false
    run dolt ls --verbose
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0 rows" ]] || false

    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
    [[ ! "$output" =~ 'pk' ]] || false

    run dolt diff
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "diff --dolt a/test b/test" ]
    [ "${lines[1]}" = "added table" ]
    
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked tables" ]] || false
    [[ "$output" =~ new[[:space:]]table:[[:space:]]+test ]] || false
}

@test "1pk5col-ints: create a table, dolt add, dolt reset, and dolt commit" {
    run dolt add test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed" ]] || false
    [[ "$output" =~ "new table:" ]] || false
    run dolt reset test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked tables" ]] || false
    [[ "$output" =~ "new table:" ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed" ]] || false
    [[ "$output" =~ "new table:" ]] || false
    run dolt commit -m "test commit"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false

}

@test "1pk5col-ints: add a row to a created table using dolt table put-row" {
    dolt add test
    dolt commit -m "create table"
    run dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    [ "$status" -eq 0 ]
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+0[[:space:]]+\|[[:space:]]+1 ]] || false
}

@test "1pk5col-ints: dolt sql all manner of inserts" {
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 1 row affected" ]] || false
    run dolt sql -q "select * from test"
    [[ "$output" =~ "6" ]] || false
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (1,7,7,7,7,7),(2,8,8,8,8,8)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 2 rows affected" ]] || false
    run dolt sql -q "select * from test"
    [[ "$output" =~ "7" ]] || false
    [[ "$output" =~ "8" ]] || false
    run dolt sql -q "insert into test (pk,c1,c3,c5) values (3,9,9,9)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 1 row affected" ]] || false
    run dolt sql -q "select * from test"
    [[ "$output" =~ "9" ]] || false
    run dolt sql -q "insert into test (c1,c3,c5) values (50,55,60)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Field 'pk' doesn't have a default value" ]] || false
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5,c6) values (10,1,1,1,1,1,1)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Unknown column 'c6' in 'test'" ]] || false
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "duplicate primary key" ]] || false
}

@test "1pk5col-ints: dolt sql insert same column twice" {
    run dolt sql -q "insert into test (pk,c1,c1) values (3,1,2)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "column 'c1' specified twice" ]] || false
}

@test "1pk5col-ints: dolt sql insert no columns specified" {
    run dolt sql -q "insert into test values (0,0,0,0,0,0)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 1 row affected" ]] || false
    run dolt sql -q "select * from test"
    [[ "$output" =~ "0" ]] || false
    run dolt sql -q "insert into test values (4,1,2)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "number of values does not match number of columns provided" ]] || false
}

@test "1pk5col-ints: dolt sql with insert ignore" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6)"
    run dolt sql -q "insert ignore into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6),(11,111,111,111,111,111)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 1 row affected" ]] || false
    run dolt sql -q "select * from test"
    [[ "$output" =~ "111" ]] || false
}

@test "1pk5col-ints: dolt sql replace into" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6)"
    run dolt sql -q "replace into test (pk,c1,c2,c3,c4,c5) values (0,7,7,7,7,7),(1,8,8,8,8,8)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 3 rows affected" ]] || false
    run dolt sql -q "select * from test"
    [[ "$output" =~ "7" ]] || false
    [[ "$output" =~ "8" ]] || false
    [[ ! "$output" =~ "6" ]] || false
}

@test "1pk5col-ints: dolt sql insert and dolt sql select" {
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 1 row affected" ]] || false
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (101,102,103,104,105,106),(1,6,7,8,9,10)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 2 rows affected" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ \|[[:space:]]+c5 ]] || false
    [[ "$output" =~ \|[[:space:]]+5 ]] || false
    [[ "$output" =~ \|[[:space:]]+10 ]] || false
    [[ "$output" =~ \|[[:space:]]+106 ]] || false
    run dolt sql -q "select * from test where pk=1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ \|[[:space:]]+c5 ]] || false
    [[ "$output" =~ \|[[:space:]]+10 ]] || false
    run dolt sql -q "select c5 from test where pk=1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ \|[[:space:]]+c5 ]] || false
    [[ "$output" =~ \|[[:space:]]+10 ]] || false
    run dolt sql -q "select * from test limit 1"
    [ "$status" -eq 0 ]
    # All line number assertions are offset by 3 to allow for table separator lines
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select * from test where c2 > 7"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select * from test where c2 >= 7"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    run dolt sql -q "select * from test where c2 <> 7"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    run dolt sql -q "select * from test where c2 > 3 and c1 < 10"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select c10 from test where pk=1"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "column \"c10\" could not be found in any table in scope" ]] || false
    run dolt sql -q "select * from test where c2=147"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

@test "1pk5col-ints: dolt sql select as" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "select c1 as column1, c2 as column2 from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "column1" ]] || false
    [[ "$output" =~ "column2" ]] || false
    [[ ! "$output" =~ "c1" ]] || false
    [[ ! "$output" =~ "c2" ]] || false
    run dolt sql -q "select c1 as column1 from test where c1=1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ column1 ]] || false
    [ "${#lines[@]}" -eq 5 ]
}

@test "1pk5col-ints: dolt sql select csv output" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "select c1 as column1, c2 as column2 from test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "column1,column2" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "11,12" ]] || false

    run dolt sql -q "select c1 as column1 from test where c1=1" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'column1' ]] || false
    [ "${#lines[@]}" -eq 2 ]

    # Test that null values are properly output
    dolt sql -q "insert into test (pk,c1) values (40,1)"
    run dolt sql -q "select c1 as column1, c2 as column2, c3 as column3 from test where pk = 40" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "column1,column2,column3" ]] || false
    [[ "$output" =~ "1,," ]] || false
}

@test "1pk5col-ints: dolt sql select json output" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "select c1 as column1, c2 as column2 from test" -r json
    [ "$status" -eq 0 ]
    [ "$output" == '{"rows": [{"column1":1,"column2":2},{"column1":11,"column2":12},{"column1":21,"column2":22}]}' ]

    run dolt sql -q "select c1 as column1 from test where c1=1" -r json
    [ "$status" -eq 0 ]
    [ "$output" == '{"rows": [{"column1":1}]}' ]
 
    # Test that null values are properly handled
    dolt sql -q "insert into test (pk,c1) values (40,1)"
    run dolt sql -q "select c1 as column1, c2 as column2, c3 as column3 from test where pk = 40" -r json
    [ "$status" -eq 0 ]
    [ "$output" == '{"rows": [{"column1":1}]}' ]
}

@test "1pk5col-ints: dolt sql select with inverted where clause" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "select * from test where 5 > c1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
}

@test "1pk5col-ints: dolt sql update queries" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "update test set c1=6,c2=7,c3=8,c4=9,c5=10 where pk=0"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 1 row affected" ]] || false
    [[ "$output" =~ "Rows matched: 1  Changed: 1  Warnings: 0" ]] || false
    run dolt sql -q "select * from test where pk=0"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "10" ]] || false
    [[ ! "$output" =~ "|5" ]] || false
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (4,11,12,13,14,15)"
    run dolt sql -q "update test set c2=11,c3=11,c4=11,c5=11 where c1=11"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 2 rows affected" ]] || false
    [[ "$output" =~ "Rows matched: 2  Changed: 2  Warnings: 0" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "11" ]] || false
    [[ ! "$output" =~ "12" ]] || false
    run dolt sql -q "update test set c2=50,c3=50,c4=50,c5=50 where c1=50"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 0 rows affected" ]] || false
    [[ "$output" =~ "Rows matched: 0  Changed: 0  Warnings: 0" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "50" ]] || false
    run dolt sql -q "update test set c12=11 where pk=0"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "column \"c12\" could not be found in any table in scope" ]] || false
    run dolt sql -q "update test set c1='foo' where pk=0"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: 'foo' is not a valid value for 'bigint'" ]] || false
    run dolt sql -q "update test set c1=100,c2=100,c3=100,c4=100,c5=100 where pk>0"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 3 rows affected" ]] || false
    [[ "$output" =~ "Rows matched: 3  Changed: 3  Warnings: 0" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "100" ]] || false
    [[ "$output" =~ "10" ]] || false
    [[ ! "$output" =~ "11" ]] || false
}

@test "1pk5col-ints: dolt sql delete queries" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "delete from test where pk=2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 1 row affected" ]] || false
    run dolt sql -q "delete from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 2 rows affected" ]] || false
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "delete from test where pk>0"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 2 rows affected" ]] || false    
    run dolt sql -q "delete from test where c1=1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 1 row affected" ]] || false    
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "delete from test where c10=1"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "column \"c10\" could not be found in any table in scope" ]] || false
    run dolt sql -q "delete from test where c1='foo'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK, 0 rows affected" ]] || false    
}

@test "1pk5col-ints: dolt checkout to put a table back to its checked in state" {
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "Added table and test row"
    dolt sql -q "replace into test values (0, 1, 2, 3, 4, 10)"
    run dolt checkout test
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "5" ]] || false
    [[ ! "$output" =~ "10" ]] || false
}

@test "1pk5col-ints: dolt checkout branch and table name collision" {
    dolt branch test
    run dolt checkout test
    [ "$status" -eq 0 ]
    # Checks out branch "test" table "test" unaltered.  Matches git behavior for:
    #
    # git init
    # git commit --allow-empty -m "create"
    # touch test
    # git branch test
    # git checkout test
}

@test "1pk5col-ints: make a change on a different branch, commit, and merge to main" {
    dolt branch test-branch
    dolt checkout test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "added test row"
    dolt checkout main
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added test row" ]] || false
}

@test "1pk5col-ints: create a branch off an older commit than HEAD" {
    dolt add test
    dolt commit -m "first commit"
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "added test row"
    run dolt checkout -b older-branch HEAD^
    [ "$status" -eq 0 ]
    [ "$output" = "Switched to branch 'older-branch'" ]
    run dolt log
    [[ ! "$output" =~ "added test row" ]] || false
    [[ "$output" =~ "first commit" ]] || false
}

@test "1pk5col-ints: delete an unmerged branch" {
    dolt checkout -b test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "added test row"
    run dolt branch -d test-branch
    [ "$status" -ne 0 ]
    [[ "$output" =~ "error: Cannot delete checked out branch 'test-branch'" ]] || false
    dolt checkout main
    run dolt branch -d test-branch
    [ "$status" -ne 0 ]
    run dolt branch -d -f test-branch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "1pk5col-ints: generate a merge conflict and resolve with ours" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 6)"
    dolt add test
    dolt commit -m "added conflicting test row"
    dolt checkout main
    run dolt merge test-branch --no-commit
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT (content)" ]] || false
    run dolt conflicts cat test
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+ours[[:space:]] ]] || false
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+theirs[[:space:]] ]] || false

    EXPECTED=$(echo -e "table,num_conflicts\ntest,1")
    run dolt sql -r csv -q 'SELECT * FROM dolt_conflicts'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    run dolt conflicts resolve --ours test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ \|[[:space:]]+5 ]] || false
    [[ ! "$output" =~ \|[[:space:]]+6 ]] || false
    run dolt conflicts cat test
    [[ ! "$output" =~ "ours" ]] || false
    [[ ! "$output" =~ "theirs" ]] || false
    dolt add test
    dolt commit -m "merged and resolved conflict"
    run dolt log
    [[ "$output" =~ "added test row" ]] || false
    [[ "$output" =~ "added conflicting test row" ]] || false
    [[ "$output" =~ "merged and resolved conflict" ]] || false
    [[ "$output" =~ "Merge:" ]] || false
}

@test "1pk5col-ints: generate a merge conflict and resolve with ours using stored procedure" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 6)"
    dolt add test
    dolt commit -m "added conflicting test row"
    dolt checkout main
    run dolt merge test-branch --no-commit
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT (content)" ]] || false
    run dolt conflicts cat test
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+ours[[:space:]] ]] || false
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+theirs[[:space:]] ]] || false

    EXPECTED=$(echo -e "table,num_conflicts\ntest,1")
    run dolt sql -r csv -q 'SELECT * FROM dolt_conflicts'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    run dolt sql -q "call dolt_conflicts_resolve('--ours', 'test')"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ \|[[:space:]]+5 ]] || false
    [[ ! "$output" =~ \|[[:space:]]+6 ]] || false
    run dolt conflicts cat test
    [[ ! "$output" =~ "ours" ]] || false
    [[ ! "$output" =~ "theirs" ]] || false
    dolt add test
    dolt commit -m "merged and resolved conflict"
    run dolt log
    [[ "$output" =~ "added test row" ]] || false
    [[ "$output" =~ "added conflicting test row" ]] || false
    [[ "$output" =~ "merged and resolved conflict" ]] || false
    [[ "$output" =~ "Merge:" ]] || false
}

@test "1pk5col-ints: generate a merge conflict and try to roll back using dolt merge --abort" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 6)"
    dolt add test
    dolt commit -m "added conflicting test row"
    dolt checkout main
    run dolt merge test-branch
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT (content)" ]] || false
    run dolt sql -q "select * from test"
    [[ "$output" =~ \|[[:space:]]+5 ]] || false
    run dolt conflicts cat test
    [[ "$output" =~ "ours" ]] || false
    [[ "$output" =~ "theirs" ]] || false
    run dolt status
    [[ "$output" =~ "You have unmerged tables" ]] || false
    run dolt merge --abort
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "1pk5col-ints: generate a merge conflict and resolve with theirs" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 6)"
    dolt add test
    dolt commit -m "added conflicting test row"
    dolt checkout main
    run dolt merge test-branch
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT (content)" ]] || false
    run dolt conflicts resolve --theirs test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt sql -q "select * from test"
    [[ "$output" =~ \|[[:space:]]+6 ]] || false
    [[ ! "$output" =~ "|5" ]] || false
}

@test "1pk5col-ints: generate a merge conflict and resolve with theirs using stored procedure" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 6)"
    dolt add test
    dolt commit -m "added conflicting test row"
    dolt checkout main
    run dolt merge test-branch
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT (content)" ]] || false
    run dolt sql -q "call dolt_conflicts_resolve('--theirs', 'test')"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from test"
    [[ "$output" =~ \|[[:space:]]+6 ]] || false
    [[ ! "$output" =~ "|5" ]] || false
}

@test "1pk5col-ints: put a row that violates the schema" {
    run dolt sql -q "insert into test values (0, 1, 2, 3, 4, 'foo')"
    [ "$status" -ne 0 ]
}

@test "1pk5col-ints: put a row that has a column not in the schema" {
    run dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5, 10)"
    [ "$status" -ne 0 ]
}

@test "1pk5col-ints: import data from a csv file after table created" {
    run dolt table import test -u `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    # Number of lines offset by 3 for table printing style
    [ "${#lines[@]}" -eq 6 ]
}

@test "1pk5col-ints: import data from a csv file with a bad line" {
    cat <<DELIM > badline.csv
pk,c1,c2,c3,c4,c5
0,1,2,3,4,5
1,1,2,3,4,5
2
DELIM

    run dolt table import test -u badline.csv
    [ "$status" -eq 1 ]
    echo $output
    [[ "${lines[0]}" =~ "Additions: 2" ]] || false
    [[ "$output" =~ "row values" ]] || false
    [[ "$output" =~ "CSV reader expected 6 values, but saw 1" ]] || false
}

@test "1pk5col-ints: import data from a csv file with a bad header" {
cat <<DELIM > bad.csv
,c1,c2,c3,c4,c5
0,1,2,3,4,5
DELIM
    run dolt table import test -u bad.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "bad header line: column cannot be NULL or empty string" ]] || false
    [[ ! "$output" =~ "panic" ]] || false

cat <<DELIM > bad.csv
pk,c1, ,c3,c4,c5
0,1,2,3,4,5
DELIM
    run dolt table import test -u bad.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "bad header line: column cannot be NULL or empty string" ]] || false
    [[ ! "$output" =~ "panic" ]] || false

cat <<DELIM > bad.csv
pk,c1,"",c3,c4,c5
0,1,2,3,4,5
DELIM
    run dolt table import test -u bad.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "bad header line: column cannot be NULL or empty string" ]] || false
    [[ ! "$output" =~ "panic" ]] || false

cat <<DELIM > bad.csv
pk,c1," ",c3,c4,c5
0,1,2,3,4,5
DELIM
    run dolt table import test -u bad.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "bad header line: column cannot be NULL or empty string" ]] || false
    [[ ! "$output" =~ "panic" ]] || false
}

@test "1pk5col-ints: import data from a psv file after table created" {
    cat <<DELIM > 1pk5col-ints.psv
pk|c1|c2|c3|c4|c5
0|1|2|3|4|5
1|1|2|3|4|5
DELIM

    run dolt table import test -u 1pk5col-ints.psv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    # Number of lines offset by 3 for table printing style
    [ "${#lines[@]}" -eq 6 ]
}

@test "1pk5col-ints: overwrite a row. make sure it updates not inserts" {
    dolt table import test -u `batshelper 1pk5col-ints.csv`
    run dolt sql -q "replace into test values (1, 2, 4, 6, 8, 10)"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    # Number of lines offset by 3 for table printing style
    [ "${#lines[@]}" -eq 6 ]
}

@test "1pk5col-ints: dolt schema show" {
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]] || false
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` bigint NOT NULL" ]] || false
    [[ "$output" =~ "\`c1\` bigint" ]] || false
    [[ "$output" =~ "\`c2\` bigint" ]] || false
    [[ "$output" =~ "\`c3\` bigint" ]] || false
    [[ "$output" =~ "\`c4\` bigint" ]] || false
    [[ "$output" =~ "\`c5\` bigint" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]] || false
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` bigint NOT NULL" ]] || false
    [[ "$output" =~ "\`c1\` bigint" ]] || false
    [[ "$output" =~ "\`c2\` bigint" ]] || false
    [[ "$output" =~ "\`c3\` bigint" ]] || false
    [[ "$output" =~ "\`c4\` bigint" ]] || false
    [[ "$output" =~ "\`c5\` bigint" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "1pk5col-ints: dolt schema show on non existent table" {
    run dolt schema show foo
    [ "$status" -eq 0 ]
    [ "$output" = "foo not found" ]
}

@test "1pk5col-ints: rm a staged but uncommitted table" {
    run dolt add test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table rm test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt add test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "${lines[0]}" = "On branch main" ]
    [ "${lines[1]}" = "nothing to commit, working tree clean" ]
}

@test "1pk5col-ints: create and view a table with NULL values" {
    dolt sql -q "insert into test (pk) values (0)"
    dolt sql -q "insert into test (pk) values (1)"
    dolt sql -q "insert into test (pk) values (2)"
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "NULL" ]] || false
    doltsqloutput=$output
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "NULL" ]] || false
    [ "$output" = "$doltsqloutput" ]
    # Make sure we don't get a table with no spaces because that bug was
    # generated when making changes to NULL printing
    [[ ! "$output" =~ "|||||" ]] || false
}

@test "1pk5col-ints: using dolt sql to select rows with NULL values" {
    dolt sql -q "insert into test (pk) values (0)"
    dolt sql -q "insert into test (pk) values (1)"
    dolt sql -q "insert into test values (2, 0, 0, 0, 0, 0)"
    run dolt sql -q "select * from test where c1 is null"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "NULL" ]] || false
}

@test "1pk5col-ints: display correct merge stats" {
  dolt checkout -b test-branch
  dolt add test
  dolt commit -m "added test table"
  dolt checkout main
  dolt branch test-branch-m
  dolt branch test-branch-alt
  dolt checkout test-branch-m
  dolt merge test-branch --no-commit
  dolt checkout test-branch-alt
  dolt sql -q "CREATE TABLE test_alt (pk BIGINT NOT NULL, c1 BIGINT, PRIMARY KEY (pk));"
  dolt add test_alt
  dolt commit -m 'add test_alt'
  dolt checkout test-branch-m
  dolt merge test-branch-alt --no-commit
  dolt add test_alt
  dolt commit -m 'merge test_alt'
  dolt checkout test-branch
  dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
  dolt add test
  dolt commit -m "added row to test"
  dolt checkout test-branch-m
  run dolt merge test-branch -m "merge"
  [ "$status" -eq 0 ]
  [ "${lines[6]}" = "test | 1 +" ]
  [ "${lines[7]}" = "1 tables changed, 1 rows added(+), 0 rows modified(*), 0 rows deleted(-)" ]
}
