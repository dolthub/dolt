#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE test1 (
  pk int NOT NULL,
  c1 int,
  c2 int,
  PRIMARY KEY (pk)
);
CREATE TABLE test2 (
  pk int NOT NULL,
  c1 int,
  c2 int,
  PRIMARY KEY (pk)
);
SQL

    dolt add .
    dolt commit -m "added tables"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "merge: 3way merge doesn't stomp working changes" {
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,2)"
    dolt add test1
    dolt commit -m "add pk 0 to test1"

    dolt checkout main
    dolt SQL -q "INSERT INTO test1 values (1,2,3)"
    dolt add test1
    dolt commit -m "add pk 1 to test1"

    dolt SQL -q "INSERT INTO test2 values (0,1,2)"
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false

    run dolt merge merge_branch --no-commit
    log_status_eq 0
    [[ ! "$output" =~ "Fast-forward" ]] || false

    run dolt status
    echo -e "\n\noutput: " $output "\n\n"
    log_status_eq 0
    [[ "$output" =~ "test2" ]] || false
    [[ "$output" =~ "test1" ]] || false

   run dolt sql -q "SELECT * from dolt_merge_status"
   [[ "$output" =~ "true" ]] || false
   [[ "$output" =~ "merge_branch" ]] || false
   [[ "$output" =~ "refs/heads/main" ]] || false

    # make sure all the commits make it into the log
    dolt add .
    dolt commit -m "squash merge"

    run dolt log
    log_status_eq 0
    [[ "$output" =~ "add pk 0 to test1" ]] || false
    [[ "$output" =~ "add pk 1 to test1" ]] || false
}

@test "merge: --abort restores working changes" {
    dolt branch other

    dolt sql -q "INSERT INTO test1 VALUES (0,10,10),(1,11,11);"
    dolt commit -am "added rows to test1 on main"

    dolt checkout other
    dolt sql -q "INSERT INTO test1 VALUES (0,20,20),(1,21,21);"
    dolt commit -am "added rows to test1 on other"

    dolt checkout main
    # dirty the working set with changes to test2
    dolt sql -q "INSERT INTO test2 VALUES (9,9,9);"

    dolt merge other --no-commit
    dolt merge --abort

    run dolt sql -q "SELECT * from dolt_merge_status"
    [[ "$output" =~ "false" ]] || false

    # per Git, working set changes to test2 should remain
    dolt sql -q "SELECT * FROM test2" -r csv
    run dolt sql -q "SELECT * FROM test2" -r csv
    log_status_eq 0
    [[ "${lines[1]}" =~ "9,9,9" ]] || false
}

@test "merge: --abort leaves clean working, staging roots" {
    dolt branch other

    dolt sql -q "INSERT INTO test1 VALUES (1,10,10);"
    dolt commit -am "added rows to test1 on main"

    dolt checkout other
    dolt sql -q "INSERT INTO test1 VALUES (2,20,20);"
    dolt commit -am "added rows to test1 on other"

    dolt checkout main
    dolt merge other --no-commit
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "still merging" ]] || false
    [[ "$output" =~ "modified:       test" ]] || false

    dolt merge --abort
    run dolt status
    log_status_eq 0
    [[ "${lines[0]}" =~ "On branch main" ]] || false
    [[ "${lines[1]}" =~ "nothing to commit, working tree clean" ]] || false

    run dolt sql -q "SELECT * from dolt_merge_status"
    [[ "$output" =~ "false" ]] || false
}

@test "merge: squash merge" {
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,2)"
    dolt add test1
    dolt commit -m "add pk 0 to test1"

    dolt checkout main
    dolt SQL -q "INSERT INTO test1 values (1,2,3)"
    dolt add test1
    dolt commit -m "add pk 1 to test1"

    dolt SQL -q "INSERT INTO test2 values (0,1,2)"
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false

    run dolt merge --squash merge_branch --no-commit
    log_status_eq 0
    [[ "$output" =~ "Squash" ]] || false
    [[ ! "$output" =~ "Fast-forward" ]] || false

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "test2" ]] || false
    [[ "$output" =~ "test1" ]] || false

    # make sure the squashed commit is not in the log.
    dolt add .
    dolt commit -m "squash merge"

    run dolt log
    log_status_eq 0
    [[ "$output" =~ "add pk 1 to test1" ]] || false
    [[ ! "$output" =~ "add pk 0 to test1" ]] || false
}

@test "merge: can merge commit spec with ancestor spec" {
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,2)"
    dolt add test1
    dolt commit -m "add pk 0 to test1"

    dolt SQL -q "INSERT INTO test1 values (1,2,3)"
    dolt add test1
    dolt commit -m "add pk 1 to test1"

    dolt checkout main

    run dolt merge merge_branch~ --no-commit
    log_status_eq 0
    [[ "$output" =~ "Fast-forward" ]] || false
    run dolt sql -q 'select count(*) from test1 where pk = 1'
    log_status_eq 0
    [[ "$output" =~ "| 0 " ]] || false

    run dolt sql -q "SELECT * from dolt_merge_status"
    [[ "$output" =~ "false" ]] || false
}

@test "merge: dolt commit fails on table with conflict" {
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,1)"
    dolt add test1
    dolt commit -m "add pk 0 = 1,1 to test1"

    dolt checkout main
    dolt SQL -q "INSERT INTO test1 values (0,2,2)"
    dolt add test1
    dolt commit -m "add pk 0 = 2,2 to test1"

    run dolt merge merge_branch -m "merge_branch"
    log_status_eq 0
    [[ "$output" =~ "test1" ]] || false

    dolt add test1
    run dolt commit -am "can't commit with conflicts"
    [ "$status" -ne 0 ]
    [[ "$output" =~ " unresolved conflicts from the merge" ]] || false
    [[ "$output" =~ "test1" ]] || false

    run dolt sql -q "SELECT * from dolt_merge_status"
    [[ "$output" =~ "true" ]] || false
    [[ "$output" =~ "merge_branch" ]] || false
    [[ "$output" =~ "refs/heads/main" ]] || false

    dolt commit --force -am "force commit with conflicts"
}

@test "merge: dolt commit fails with unmerged tables in working set" {
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,1)"
    dolt add test1
    dolt commit -m "add pk 0 = 1,1 to test1"

    dolt checkout main
    dolt SQL -q "INSERT INTO test1 values (0,2,2)"
    dolt add test1
    dolt commit -m "add pk 0 = 2,2 to test1"

    run dolt merge merge_branch --no-commit
    log_status_eq 0
    [[ "$output" =~ "test1" ]] || false

    run dolt commit -m 'create a merge commit'
    [ "$status" -ne 0 ]
    [[ "$output" =~ "unresolved conflicts" ]] || false
    [[ "$output" =~ "test1" ]] || false
}

@test "merge: ff merge doesn't stomp working changes" {
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,2)"
    dolt add test1
    dolt commit -m "modify test1"

    dolt checkout main
    dolt SQL -q "INSERT INTO test2 values (0,1,2)"
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false

    run dolt merge merge_branch
    log_status_eq 0
    [[ "$output" =~ "Fast-forward" ]] || false

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false
}

@test "merge: no-ff merge" {
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,2)"
    dolt add test1
    dolt commit -m "modify test1"

    dolt checkout main
    run dolt merge merge_branch --no-ff -m "no-ff merge"
    log_status_eq 0
    [[ ! "$output" =~ "Fast-forward" ]] || false

    run dolt log
    log_status_eq 0
    [[ "$output" =~ "no-ff merge" ]] || false
}

@test "merge: no-ff merge doesn't stomp working changes and doesn't fast forward" {
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,2)"
    dolt add test1
    dolt commit -m "modify test1"

    dolt checkout main
    dolt SQL -q "INSERT INTO test2 values (0,1,2)"
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false

    run dolt merge merge_branch --no-ff -m "no-ff merge"
    log_status_eq 0
    [[ ! "$output" =~ "Fast-forward" ]] || false

    run dolt status
    log_status_eq 0
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false

    run dolt log
    log_status_eq 0
    [[ "$output" =~ "no-ff merge" ]] || false
}

@test "merge: 3way merge rejected when working changes touch same tables" {
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,2)"
    dolt add test1
    dolt commit -m "add pk 0 to test1"

    dolt checkout main
    dolt SQL -q "INSERT INTO test2 values (0,1,2)"
    dolt add test2
    dolt commit -m "add pk 0 to test2"

    dolt SQL -q "INSERT INTO test1 values (1,2,3)"
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "test2" ]] || false

    run dolt merge merge_branch
    log_status_eq 1
}

@test "merge: ff merge rejected when working changes touch same tables" {
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,2)"
    dolt add test1
    dolt commit -m "modify test1"

    dolt checkout main
    dolt ls
    dolt SQL -q "INSERT INTO test1 values (1,2,3)"
    run dolt status
    log_status_eq 0
    [[ "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "test2" ]] || false

    run dolt merge merge_branch
    log_status_eq 1
}

@test "merge: Add tables with same schema on two branches, merge" {
    dolt branch other
    dolt sql <<SQL
CREATE TABLE quiz (pk int PRIMARY KEY);
INSERT INTO quiz VALUES (10),(11),(12);
SQL
    dolt add . && dolt commit -m "added table quiz on main";

    dolt checkout other
    dolt sql <<SQL
CREATE TABLE quiz (pk int PRIMARY KEY);
INSERT INTO quiz VALUES (20),(21),(22);
SQL
    dolt add . && dolt commit -m "added table quiz on other"

    dolt checkout main
    run dolt merge other -m "merge other"
    log_status_eq 0
    run dolt sql -q "SELECT * FROM quiz ORDER BY pk;" -r csv
    [[ "${lines[0]}" =~ "pk" ]] || false
    [[ "${lines[1]}" =~ "10" ]] || false
    [[ "${lines[2]}" =~ "11" ]] || false
    [[ "${lines[3]}" =~ "12" ]] || false
    [[ "${lines[4]}" =~ "20" ]] || false
    [[ "${lines[5]}" =~ "21" ]] || false
    [[ "${lines[6]}" =~ "22" ]] || false
}

@test "merge: Add views on two branches, merge" {
    dolt branch other
    dolt sql -q "CREATE VIEW pkpk AS SELECT pk*pk FROM test1;"
    dolt add . && dolt commit -m "added view on table test1"

    dolt checkout other
    dolt sql -q "CREATE VIEW c1c1 AS SELECT c1*c1 FROM test2;"
    dolt add . && dolt commit -m "added view on table test2"

    dolt checkout main
    run dolt merge other --no-commit
    log_status_eq 0
    [[ "$output" =~ "CONFLICT" ]] || false
    run dolt conflicts resolve --theirs dolt_schemas
    log_status_eq 0
    run dolt sql -q "select name from dolt_schemas" -r csv
    log_status_eq 0
    [[ "$output" =~ "c1c1" ]] || false
}

@test "merge: Add views on two branches, merge with stored procedure" {
    dolt branch other
    dolt sql -q "CREATE VIEW pkpk AS SELECT pk*pk FROM test1;"
    dolt add . && dolt commit -m "added view on table test1"

    dolt checkout other
    dolt sql -q "CREATE VIEW c1c1 AS SELECT c1*c1 FROM test2;"
    dolt add . && dolt commit -m "added view on table test2"

    dolt checkout main
    run dolt merge other --no-commit
    log_status_eq 0
    [[ "$output" =~ "CONFLICT" ]] || false
    run dolt sql -q "call dolt_conflicts_resolve('--theirs', 'dolt_schemas')"
    log_status_eq 0
    run dolt sql -q "select name from dolt_schemas" -r csv
    log_status_eq 0
    [[ "$output" =~ "c1c1" ]] || false
}

@test "merge: Add views on two branches, merge without conflicts" {
    dolt branch other
    dolt sql -q "CREATE VIEW pkpk AS SELECT pk*pk FROM test1;"
    dolt add . && dolt commit -m "added view on table test1"

    dolt checkout other
    dolt sql -q "CREATE VIEW c1c1 AS SELECT c1*c1 FROM test2;"
    dolt add . && dolt commit -m "added view on table test2"

    dolt checkout main
    run dolt merge other
    skip "key collision in dolt_schemas"
    log_status_eq 0
    run dolt sql -q "select name from dolt_schemas" -r csv
    log_status_eq 0
    [[ "$output" =~ "pkpk" ]] || false
    [[ "$output" =~ "c1c1" ]] || false
}

@test "merge: unique index conflict" {
    dolt sql <<SQL
CREATE TABLE test (
    pk int PRIMARY KEY,
    c0 int,
    UNIQUE KEY(c0)
);
INSERT INTO test VALUES (0,0);
SQL
    dolt add -A && dolt commit -am "setup"

    dolt checkout -b other
    dolt sql -q "INSERT INTO test VALUES (2,19);"
    dolt commit -am "added row"

    dolt checkout main
    dolt sql -q "INSERT INTO test VALUES (1,19);"
    dolt commit -am "added row"

    skip "merge fails on unique index violation, should log conflict"
    dolt merge other
}

@test "merge: composite unique index conflict" {
    dolt sql <<SQL
CREATE TABLE test (
    pk int PRIMARY KEY,
    c0 int,
    c1 int,
    UNIQUE KEY(c0,c1)
);
INSERT INTO test VALUES (0, 0,  0);
INSERT INTO test VALUES (1, 11, 2);
INSERT INTO test VALUES (2, 1,  22);
SQL
    dolt add -A && dolt commit -am "setup"

    dolt checkout -b other
    dolt sql -q "UPDATE test SET c0 = 1 where c0 = 11"
    dolt commit -am "added row"

    dolt checkout main
    dolt sql -q "UPDATE test SET c1 = 2 where c1 = 22"
    dolt commit -am "added row"

    run dolt merge other
    log_status_eq 0

    run dolt sql -q "select * from dolt_constraint_violations" -r=csv
    [[ "$output" =~ "test,2" ]] || false
}

@test "merge: composite indexes should be consistent post-merge" {
    dolt sql <<SQL
CREATE TABLE test (
    id int PRIMARY KEY,
    c0 int,
    c1 int,
    INDEX idx_c0_c1 (c0, c1)
);
INSERT INTO test VALUES (1, 0, 0);
SQL
    dolt add .
    dolt commit -am "initial data"
    dolt branch right

    dolt sql -q "UPDATE test SET c0 = 1;"
    dolt commit -am "left commit"

    dolt checkout right
    dolt sql -q "UPDATE test SET c1 = 1;"
    dolt commit -am "right commit"

    dolt checkout main
    dolt merge right -m "merge"

    # left composite index left-over
    run dolt sql -r csv -q "SELECT count(*) from test WHERE c0 = 1 AND c1 = 0;"
    log_status_eq 0
    [ ${lines[1]} -eq 0 ]

    # right composite index left-over
    run dolt sql -r csv -q "SELECT count(*) from test WHERE c0 = 0 AND c1 = 1;"
    log_status_eq 0
    [ ${lines[1]} -eq 0 ]

    run dolt sql -r csv -q "SELECT count(*) from test WHERE c0 = 1 AND c1 = 1;"
    log_status_eq 0
    [ ${lines[1]} -eq 1 ]
}

@test "merge: merge a branch with a new table" {
    dolt branch feature-branch

    dolt sql << SQL
INSERT INTO test2 VALUES (0, 0, 0);
INSERT INTO test2 VALUES (1, 1, 1);
SQL
    dolt add -A && dolt commit -am "add data to test 2"

    dolt checkout feature-branch
    dolt sql << SQL
create table test3 (a int primary key);
INSERT INTO test3 VALUES (0), (1);
SQL
    dolt add .
    dolt commit -am "new table test3"

    dolt checkout main

    run dolt merge feature-branch -m "merge feature-branch"
    log_status_eq 0

    run dolt sql -q "select * from test3"
    log_status_eq 0
    [[ "$output" =~ 1 ]] || false
}

@test "merge: merge a branch that deletes a table" {
    dolt branch feature-branch

    dolt sql << SQL
INSERT INTO test1 VALUES (0, 0, 0);
INSERT INTO test1 VALUES (1, 1, 1);
SQL
    dolt commit -am "add data to test1"

    dolt checkout feature-branch
    dolt sql << SQL
INSERT INTO test1 VALUES (2, 2, 2);
drop table test2;
SQL
    dolt commit -am "add data to test1, drop test2"

    dolt checkout main
    run dolt merge feature-branch -m "merge feature-branch"
    log_status_eq 0

    run dolt sql -q "show tables"
    log_status_eq 0
    [[ ! "$output" =~ "test2" ]] || false

    run dolt sql -q "select * from test1" -r csv
    log_status_eq 0
    [[ "$output" =~ "1,1,1" ]] || false
    [[ "$output" =~ "2,2,2" ]] || false
}

@test "merge: merge branch with table that was deleted" {
    dolt sql << SQL
INSERT INTO test2 VALUES (0, 0, 0);
INSERT INTO test2 VALUES (1, 1, 1);
SQL
    dolt add -A && dolt commit -am "add data to test2"

    dolt branch feature-branch
    dolt sql -q "drop table test2"
    dolt commit -am "drop table test2"

    dolt sql << SQL
INSERT INTO test1 VALUES (2, 2, 2);
INSERT INTO test1 VALUES (3, 3, 3);
SQL
    dolt commit -am "add data to test1"

    dolt checkout feature-branch
    dolt sql << SQL
INSERT INTO test1 VALUES (0, 0, 0);
INSERT INTO test1 VALUES (1, 1, 1);
SQL
    dolt commit -am "add data to test1"

    dolt checkout main
    run dolt merge feature-branch -m "merge feature-branch"
    log_status_eq 0

    run dolt sql -q "show tables"
    log_status_eq 0
    [[ ! "$output" =~ "test2" ]] || false

    run dolt sql -q "select * from test1" -r csv
    log_status_eq 0
    [[ "$output" =~ "1,1,1" ]] || false
    [[ "$output" =~ "2,2,2" ]] || false
}

@test "merge: merge a branch that edits a deleted table" {
    dolt sql << SQL
INSERT INTO test2 VALUES (0, 0, 0);
INSERT INTO test2 VALUES (1, 1, 1);
SQL
    dolt add -A && dolt commit -am "add data to test2"

    dolt branch feature-branch
    dolt sql -q "drop table test2"
    dolt commit -am "drop table test2"

    dolt checkout feature-branch
    dolt sql << SQL
INSERT INTO test2 VALUES (2, 2, 2);
SQL
    dolt commit -am "add data to test2"

    dolt checkout main
    run dolt merge feature-branch

    log_status_eq 1
    [[ "$output" =~ "conflict" ]] || false
}

@test "merge: merge a branch that deletes an edited table" {
    dolt sql << SQL
INSERT INTO test2 VALUES (0, 0, 0);
INSERT INTO test2 VALUES (1, 1, 1);
SQL
    dolt add -A && dolt commit -am "add data to test2"

    dolt branch feature-branch
    dolt sql << SQL
INSERT INTO test2 VALUES (2, 2, 2);
SQL
    dolt commit -am "add data to test2"

    dolt checkout feature-branch
    dolt sql -q "drop table test2"
    dolt commit -am "drop table test2"

    dolt checkout main
    run dolt merge feature-branch

    log_status_eq 1
    [[ "$output" =~ "conflict" ]] || false
}

@test "merge: merge a branch that deletes a deleted table" {
    dolt sql << SQL
INSERT INTO test2 VALUES (0, 0, 0);
INSERT INTO test2 VALUES (1, 1, 1);
SQL
    dolt add -A && dolt commit -am "add data to test2"

    dolt branch feature-branch
    dolt sql << SQL
INSERT INTO test2 VALUES (2, 2, 2);
SQL
    dolt commit -am "add data to test2"
    dolt sql << SQL
drop table test2;
SQL
    dolt commit -am "drop test2"

    dolt checkout feature-branch
    dolt sql -q "drop table test2"
    dolt commit -am "drop table test2"

    dolt checkout main
    run dolt merge feature-branch -m "merge feature-branch"
    log_status_eq 0

    run dolt sql -q "show tables"
    log_status_eq 0
    [[ "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "test2" ]] || false
}

@test "merge: non-violating merge succeeds when violations already exist" {
    dolt sql <<SQL
CREATE table parent (pk int PRIMARY KEY, col1 int);
CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));
CREATE table other (pk int);
INSERT INTO parent VALUES (1, 1);
SQL
    dolt add .
    dolt commit -am "create table with data";
    dolt branch right
    dolt sql -q "DELETE FROM parent where pk = 1;"
    dolt commit -am "delete pk = 1 from left";

    dolt checkout right
    dolt sql -q "INSERT INTO child VALUES (1, 1);"
    dolt commit -am "add child of 1 to right"
    dolt branch other

    dolt checkout other
    dolt sql -q "INSERT INTO other VALUES (1);"
    dolt commit -am "non-fk insert"

    dolt checkout main
    dolt merge right
    dolt commit -afm "commit constraint violations"

    dolt merge other --no-commit

    run dolt sql -r csv -q "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;";
    [[ "${lines[1]}" = "foreign key,1,1" ]]
}

@test "merge: non-conflicting / non-violating merge succeeds when conflicts and violations already exist" {
    dolt sql <<SQL
CREATE table parent (pk int PRIMARY KEY, col1 int);
CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));
INSERT INTO parent VALUES (1, 1), (2, 1);
SQL
    dolt add .
    dolt commit -am "create table with data";
    dolt branch other
    dolt branch other2

    dolt sql -q "UPDATE parent SET col1 = 2 where pk = 1;"
    dolt sql -q "DELETE FROM parent where pk = 2;"
    dolt commit -am "updating col1 to 2 and remove pk = 2";

    dolt checkout other
    dolt sql -q "UPDATE parent SET col1 = 3 where pk = 1;"
    dolt sql -q "INSERT INTO child VALUES (1, 2);"
    dolt commit -am "updating col1 to 3 and adding child of pk 2";

    dolt checkout other2
    dolt sql -q "INSERT INTO parent values (3, 1);"
    dolt commit -am "insert parent with pk 3"

    dolt checkout main
    # Create a conflicted state by merging other into main
    run dolt merge other
    [[ "$output" =~ "CONFLICT" ]]

    run dolt sql -r csv -q "SELECT * FROM parent;"
    [[ "${lines[1]}" = "1,2" ]]

    run dolt sql -r csv -q "SELECT * from child;"
    [[ "${lines[1]}" = "1,2" ]]

    run dolt sql -r csv -q "SELECT base_col1, base_pk, our_col1, our_pk, their_col1, their_pk from dolt_conflicts_parent;"
    [[ "$output" =~ "1,1,2,1,3,1" ]]

    run dolt sql -r csv -q "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;"
    [[ "$output" =~ "foreign key,1,2" ]]

    # commit it so we can merge again
    dolt commit -afm "committing merge conflicts"

    # merge should be allowed and previous conflicts and violations should be retained
    dolt merge other2 --no-commit
    run dolt sql -r csv -q "SELECT * FROM parent;"
    [[ "${lines[1]}" = "1,2" ]]
    [[ "${lines[2]}" = "3,1" ]]

    run dolt sql -r csv -q "SELECT * from child;"
    [[ "${lines[1]}" = "1,2" ]]

    run dolt sql -r csv -q "SELECT base_col1, base_pk, our_col1, our_pk, their_col1, their_pk from dolt_conflicts_parent;"
    [[ "${lines[1]}" =~ "1,1,2,1,3,1" ]]

    run dolt sql -r csv -q "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;"
    [[ "${lines[1]}" =~ "foreign key,1,2" ]]
}

@test "merge: conflicting merge should retain previous conflicts and constraint violations" {
    dolt sql <<SQL
CREATE table parent (pk int PRIMARY KEY, col1 int);
CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));
INSERT INTO parent VALUES (1, 1), (2, 1);
SQL
    dolt add .
    dolt commit -am "create table with data";
    dolt branch other
    dolt branch other2
    dolt sql -q "UPDATE parent SET col1 = 2 where pk = 1;"
    dolt sql -q "DELETE FROM parent where pk = 2;"
    dolt commit -am "updating col1 to 2 and remove pk = 2";

    dolt checkout other
    dolt sql -q "UPDATE parent SET col1 = 3 where pk = 1;"
    dolt sql -q "INSERT INTO child VALUES (1, 2);"
    dolt commit -am "updating col1 to 3 and adding child of pk 2";

    dolt checkout other2
    dolt sql -q "UPDATE parent SET col1 = 4 where pk = 1;"
    dolt commit -am "updating col1 to 4"

    dolt checkout main

    # Create a conflicted state by merging other into main
    run dolt merge other
    log_status_eq 0
    [[ "$output" =~ "CONFLICT" ]]

    run dolt sql -r csv -q "SELECT * FROM parent;"
    [[ "${lines[1]}" = "1,2" ]]

    run dolt sql -r csv -q "SELECT * from child;"
    [[ "${lines[1]}" = "1,2" ]]

    run dolt sql -r csv -q "SELECT base_col1, base_pk, our_col1, our_pk, their_col1, their_pk from dolt_conflicts_parent;"
    [[ "${lines[1]}" = "1,1,2,1,3,1" ]]

    run dolt sql -r csv -q "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;"
    [[ "${lines[1]}" = "foreign key,1,2" ]]

    # commit it so we can merge again
    dolt commit -afm "committing merge conflicts"

    skip_nbf_dolt "behavior in new format diverges"

    # Merge should fail due to conflict and previous conflict and violation state should be retained
    run dolt merge other2
    [[ "$output" =~ "existing unresolved conflicts would be overridden by new conflicts produced by merge" ]]

    run dolt sql -r csv -q "SELECT * FROM parent;"
    [[ "${lines[1]}" = "1,2" ]]

    run dolt sql -r csv -q "SELECT * from child;"
    [[ "${lines[1]}" = "1,2" ]]

    run dolt sql -r csv -q "SELECT base_col1, base_pk, our_col1, our_pk, their_col1, their_pk from dolt_conflicts_parent;"
    [[ "${lines[1]}" = "1,1,2,1,3,1" ]]

    run dolt sql -r csv -q "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;"
    [[ "${lines[1]}" = "foreign key,1,2" ]]
}

@test "merge: violated check constraint" {
    skip "merge doesn't respect check constraints"
    dolt sql -q "CREATE table t (pk int PRIMARY KEY, col1 int);"
    dolt commit -am "create table"
    dolt branch other

    dolt sql -q "ALTER TABLE t ADD CHECK (col1 % 2 = 0);"
    dolt commit -am "add check constraint"

    dolt checkout other
    dolt sql -q "INSERT into t values (1, 1);"
    dolt commit -am "add row"

    dolt checkout main

    dolt merge other
    run dolt sql -r csv -q "SELECT * from dolt_constraint_violations";
    [[ "$output" =~ "t,1" ]]

    run dolt status
    [[ ! "$output" =~ "nothing to commit, working tree clean" ]]
}

@test "merge: ourRoot renames, theirRoot modifies" {
    dolt checkout -b merge_branch
    dolt sql -q "INSERT INTO test1 VALUES (0,1,2)"
    dolt commit -am "add pk 0 to test1"

    dolt checkout main
    dolt sql -q "ALTER TABLE test1 RENAME TO new_name"
    dolt add .
    dolt commit -am "rename test1"

    run dolt merge merge_branch
    log_status_eq 1
    [[ "$output" =~ "table with same name deleted and modified" ]] || false
}

@test "merge: ourRoot modifies, theirRoot renames" {
    dolt checkout -b merge_branch
    dolt sql -q "ALTER TABLE test1 RENAME TO new_name"
    dolt add .
    dolt commit -am "rename test1"

    dolt checkout main
    dolt sql -q "INSERT INTO test1 VALUES (0,1,2)"
    dolt commit -am "add pk 0 to test1"

    run dolt merge merge_branch
    log_status_eq 1
    [[ "$output" =~ "table with same name deleted and modified" ]] || false
}

@test "merge: dolt merge commits successful non-fast-forward merge" {
    dolt branch other
    dolt sql -q "INSERT INTO test1 VALUES (1,2,3)"
    dolt commit -am "add (1,2,3) to test1";

    dolt checkout other
    dolt sql -q "INSERT INTO test1 VALUES (2,3,4)"
    dolt commit -am "add (2,3,4) to test1";

    dolt checkout main
    run dolt merge other --no-commit --commit
    log_status_eq 1
    [[ "$output" =~ "cannot define both 'commit' and 'no-commit' flags at the same time" ]] || false

    run dolt merge other --no-commit
    log_status_eq 0

    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "merge other" ]] || false

    dolt reset --hard
    run dolt merge other -m "merge other"
    log_status_eq 0

    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "merge other" ]] || false
    [[ ! "$output" =~ "add (1,2) to t1" ]] || false
    [[ ! "$output" =~ "add (2,3) to t1" ]] || false
}

@test "merge: specify ---author for merge that's used for creating commit" {
    dolt branch other
    dolt sql -q "INSERT INTO test1 VALUES (1,2,3)"
    dolt commit -am "add (1,2,3) to test1";

    dolt checkout other
    dolt sql -q "INSERT INTO test1 VALUES (2,3,4)"
    dolt commit -am "add (2,3,4) to test1";

    run dolt config --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "user.email = bats@email.fake" ]] || false
    [[ "$output" =~ "user.name = Bats Tests" ]] || false

    dolt checkout main
    run dolt merge other --author "John Doe <john@doe.com>" -m "merge other"
    log_status_eq 0

    run dolt log -n 1
    [ "$status" -eq 0 ]
    regex='John Doe <john@doe.com>'
    [[ "$output" =~ "$regex" ]] || false
}

@test "merge: prints merge stats" {
    dolt sql -q "CREATE table t (pk int primary key, col1 int);"
    dolt sql -q "CREATE table t2 (pk int primary key);"
    dolt sql -q "INSERT INTO t VALUES (1, 1), (2, 2);"
    dolt commit -Am "add table t"

    dolt checkout -b right
    dolt sql -q "insert into t values (3, 3), (4, 4);"
    dolt sql -q "delete from t where pk = 1;"
    dolt sql -q "update t set col1 = 200 where pk = 2;"
    dolt sql -q "insert into t2 values (1);"
    dolt commit -Am "right"

    dolt checkout main
    dolt sql -q "insert into t values (5, 5);"
    dolt commit -Am "left"

    run dolt merge right -m "merge right into main"
    [ $status -eq 0 ]
    [[ "$output" =~ "2 tables changed, 3 rows added(+), 1 rows modified(*), 1 rows deleted(-)" ]] || false
}
