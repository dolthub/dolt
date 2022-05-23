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
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false

    run dolt merge merge_branch
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Fast-forward" ]] || false

    run dolt status
    echo -e "\n\noutput: " $output "\n\n"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test2" ]] || false
    [[ "$output" =~ "test1" ]] || false

    # make sure all the commits make it into the log
    dolt add .
    dolt commit -m "squash merge"

    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "add pk 0 to test1" ]] || false
    [[ "$output" =~ "add pk 1 to test1" ]] || false
}

@test "merge: --abort restores working changes" {
    skip_nbf_dolt_1
    dolt branch other

    dolt sql -q "INSERT INTO test1 VALUES (0,10,10),(1,11,11);"
    dolt commit -am "added rows to test1 on main"

    dolt checkout other
    dolt sql -q "INSERT INTO test1 VALUES (0,20,20),(1,21,21);"
    dolt commit -am "added rows to test1 on other"

    dolt checkout main
    # dirty the working set with changes to test2
    dolt sql -q "INSERT INTO test2 VALUES (9,9,9);"

    dolt merge other
    dolt merge --abort

    # per Git, working set changes to test2 should remain
    dolt sql -q "SELECT * FROM test2" -r csv
    run dolt sql -q "SELECT * FROM test2" -r csv
    [ "$status" -eq 0 ]
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
    dolt merge other
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "still merging" ]] || false
    [[ "$output" =~ "modified:       test" ]] || false

    dolt merge --abort
    run dolt status
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "On branch main" ]] || false
    [[ "${lines[1]}" =~ "nothing to commit, working tree clean" ]] || false
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
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false

    run dolt merge --squash merge_branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Squash" ]] || false
    [[ ! "$output" =~ "Fast-forward" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test2" ]] || false
    [[ "$output" =~ "test1" ]] || false

    # make sure the squashed commit is not in the log.
    dolt add .
    dolt commit -m "squash merge"

    run dolt log
    [ "$status" -eq 0 ]
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

    run dolt merge merge_branch~
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]] || false
    run dolt sql -q 'select count(*) from test1 where pk = 1'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 0 " ]] || false
}

@test "merge: dolt commit fails on table with conflict" {
    skip_nbf_dolt_1
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,1)"
    dolt add test1
    dolt commit -m "add pk 0 = 1,1 to test1"

    dolt checkout main
    dolt SQL -q "INSERT INTO test1 values (0,2,2)"
    dolt add test1
    dolt commit -m "add pk 0 = 2,2 to test1"

    run dolt merge merge_branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false

    dolt add test1
    run dolt commit -am "can't commit with conflicts"
    [ "$status" -ne 0 ]
    [[ "$output" =~ " unresolved conflicts from the merge" ]] || false
    [[ "$output" =~ "test1" ]] || false
    dolt commit --force -am "force commit with conflicts"
}

@test "merge: dolt commit fails with unmerged tables in working set" {
    skip_nbf_dolt_1
    dolt checkout -b merge_branch
    dolt SQL -q "INSERT INTO test1 values (0,1,1)"
    dolt add test1
    dolt commit -m "add pk 0 = 1,1 to test1"

    dolt checkout main
    dolt SQL -q "INSERT INTO test1 values (0,2,2)"
    dolt add test1
    dolt commit -m "add pk 0 = 2,2 to test1"

    run dolt merge merge_branch
    [ "$status" -eq 0 ]
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
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false

    run dolt merge merge_branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
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
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Fast-forward" ]] || false

    run dolt log
    [ "$status" -eq 0 ]
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
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false

    run dolt merge merge_branch --no-ff -m "no-ff merge"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Fast-forward" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test2" ]] || false
    [[ ! "$output" =~ "test1" ]] || false

    run dolt log
    [ "$status" -eq 0 ]
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
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "test2" ]] || false

    run dolt merge merge_branch
    [ "$status" -eq 1 ]
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
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "test2" ]] || false

    run dolt merge merge_branch
    [ "$status" -eq 1 ]
}

@test "merge: Add tables with same schema on two branches, merge" {
    skip_nbf_dolt_1
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
    run dolt merge other
    [ "$status" -eq 0 ]
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
    skip_nbf_dolt_1
    dolt branch other
    dolt sql -q "CREATE VIEW pkpk AS SELECT pk*pk FROM test1;"
    dolt add . && dolt commit -m "added view on table test1"

    dolt checkout other
    dolt sql -q "CREATE VIEW c1c1 AS SELECT c1*c1 FROM test2;"
    dolt add . && dolt commit -m "added view on table test2"

    dolt checkout main
    run dolt merge other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
    run dolt conflicts resolve --theirs dolt_schemas
    [ "$status" -eq 0 ]
    run dolt sql -q "select name from dolt_schemas" -r csv
    [ "$status" -eq 0 ]
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
    [ "$status" -eq 0 ]
    run dolt sql -q "select name from dolt_schemas" -r csv
    [ "$status" -eq 0 ]
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

    skip "merge fails on unique index violation, should log conflict"
    dolt merge other
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
    dolt commit -am "initial data"
    dolt branch right

    dolt sql -q "UPDATE test SET c0 = 1;"
    dolt commit -am "left commit"

    dolt checkout right
    dolt sql -q "UPDATE test SET c1 = 1;"
    dolt commit -am "right commit"

    dolt checkout main
    dolt merge right && dolt commit -am "merge"

    # left composite index left-over
    run dolt sql -r csv -q "SELECT count(*) from test WHERE c0 = 1 AND c1 = 0;"
    [ "$status" -eq 0 ]
    [ ${lines[1]} -eq 0 ]

    # right composite index left-over
    run dolt sql -r csv -q "SELECT count(*) from test WHERE c0 = 0 AND c1 = 1;"
    [ "$status" -eq 0 ]
    [ ${lines[1]} -eq 0 ]

    run dolt sql -r csv -q "SELECT count(*) from test WHERE c0 = 1 AND c1 = 1;"
    [ "$status" -eq 0 ]
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
    dolt commit -am "new table test3"

    dolt checkout main
    
    run dolt merge feature-branch
    [ "$status" -eq 0 ]

    run dolt sql -q "select * from test3"
    [ "$status" -eq 0 ]
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
    run dolt merge feature-branch
    [ "$status" -eq 0 ]

    dolt commit -m "merged feature-branch"

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test2" ]] || false

    run dolt sql -q "select * from test1" -r csv
    [ "$status" -eq 0 ]
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
    run dolt merge feature-branch
    [ "$status" -eq 0 ]

    dolt commit -m "merged feature-branch"

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test2" ]] || false

    run dolt sql -q "select * from test1" -r csv
    [ "$status" -eq 0 ]
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

    [ "$status" -eq 1 ]
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

    [ "$status" -eq 1 ]
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
    run dolt merge feature-branch
    [ "$status" -eq 0 ]
    dolt commit -m "merged feature-branch"

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "test2" ]] || false
}

@test "merge: non-violating merge succeeds when violations already exist" {
    skip_nbf_dolt_1
    dolt sql <<SQL
CREATE table parent (pk int PRIMARY KEY, col1 int);
CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));
CREATE table other (pk int);
INSERT INTO parent VALUES (1, 1);
SQL
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
    run dolt merge right
    dolt commit -afm "commit constraint violations"

    dolt merge other

    run dolt sql -r csv -q "SELECT violation_type, pk, parent_fk from dolt_constraint_violations_child;";
    [[ "${lines[1]}" = "foreign key,1,1" ]]
}

@test "merge: non-conflicting / non-violating merge succeeds when conflicts and violations already exist" {
    skip_nbf_dolt_1
    dolt sql <<SQL
CREATE table parent (pk int PRIMARY KEY, col1 int);
CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));
INSERT INTO parent VALUES (1, 1), (2, 1);
SQL
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
    dolt merge other2
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
    skip_nbf_dolt_1
    dolt sql <<SQL
CREATE table parent (pk int PRIMARY KEY, col1 int);
CREATE table child (pk int PRIMARY KEY, parent_fk int, FOREIGN KEY (parent_fk) REFERENCES parent(pk));
INSERT INTO parent VALUES (1, 1), (2, 1);
SQL
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
    [ "$status" -eq 0 ]
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
}
