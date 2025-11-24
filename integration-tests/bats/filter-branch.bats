#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
  pk int NOT NULL PRIMARY KEY,
  c0 int
);
INSERT INTO test VALUES
    (0,0),(1,1),(2,2);
CREATE TABLE to_drop (
    pk int PRIMARY KEY
);
SQL
    dolt add -A
    dolt commit -m "added table test"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "filter-branch: smoke-test" {
    dolt sql -q "INSERT INTO test VALUES (7,7),(8,8),(9,9);"
    dolt add -A && dolt commit -m "added more rows"

    dolt filter-branch -q "DELETE FROM test WHERE pk > 1;"
    run dolt sql -q "SELECT count(*) FROM test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    run dolt sql -q "SELECT max(pk), max(c0) FROM dolt_history_test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
}

@test "filter-branch: verbose mode" {
    dolt sql -q "INSERT INTO test VALUES (7,7),(8,8),(9,9);"
    dolt commit -Am "added more rows"

    run dolt filter-branch -v -q "DELETE FROM test WHERE pk = 8;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "processing commit" ]] || false
    [[ "$output" =~ "updated commit" ]] || false
}


@test "filter-branch: filter multiple branches" {
    dolt branch other

    dolt sql -q "INSERT INTO test VALUES (7,7),(8,8),(9,9);"
    dolt add -A && dolt commit -m "added more rows"

    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (4,4),(5,5),(6,6);"
    dolt add -A && dolt commit -m "added more rows"

    dolt checkout main
    dolt filter-branch --all -q "DELETE FROM test WHERE pk > 4;"

    run dolt sql -q "SELECT pk,c0 FROM dolt_history_test ORDER BY pk" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "2,2" ]] || false

    dolt checkout other
    run dolt sql -q "SELECT pk,c0 FROM dolt_history_test ORDER BY pk" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "4,4" ]] || false
}

@test "filter-branch: filter tags" {
    dolt sql <<SQL
create table t (pk int primary key);
insert into t values (1),(2);
call dolt_commit('-Am', 'msg');
insert into t values (3);
call dolt_commit('-Am', 'three');
call dolt_tag('myTag');
insert into t values (4);
call dolt_commit('-Am', 'four');
SQL
    run dolt sql -q "select * from t as of 'myTag'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false

    dolt filter-branch --all --continue -q "delete from t where pk >= 3"

    run dolt sql -q "select * from t as of 'myTag'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ ! "$output" =~ "3" ]] || false
}

@test "filter-branch: filter branches only" {
    dolt sql <<SQL
create table t (pk int primary key);
insert into t values (1),(2);
call dolt_commit('-Am', 'msg');
insert into t values (3);
call dolt_commit('-Am', 'three');
call dolt_tag('myTag');
insert into t values (4);
call dolt_commit('-Am', 'four');
SQL
    run dolt sql -q "select * from t" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
    [[ "$output" =~ "4" ]] || false

    run dolt sql -q "select * from t as of 'myTag'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false

    dolt filter-branch --branches --continue -q "delete from t where pk >= 3"

    run dolt sql -q "select * from t" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ ! "$output" =~ "3" ]] || false
    [[ ! "$output" =~ "4" ]] || false

    run dolt sql -q "select * from t as of 'myTag'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "$output" =~ "3" ]] || false
}

@test "filter-branch: with missing table" {
    dolt sql -q "DROP TABLE test;"
    dolt add -A && dolt commit -m "dropped test"

    # filter-branch warns about missing table but doesn't error
    run dolt filter-branch --continue --verbose -q "DELETE FROM test WHERE pk > 1;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "table not found: test" ]] || false

    run dolt sql -q "SELECT count(*) FROM test AS OF 'HEAD~1';" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false
}

@test "filter-branch: forks history" {
    dolt branch other

    dolt sql -q "INSERT INTO test VALUES (7,7),(8,8),(9,9);"
    dolt add -A && dolt commit -m "added more rows"

    dolt filter-branch -q "DELETE FROM test WHERE pk > 1;"

    dolt checkout other
    run dolt sql -q "SELECT * FROM test WHERE pk > 1" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2,2" ]] || false
}

@test "filter-branch: filter until commit" {
    dolt sql -q "INSERT INTO test VALUES (7,7)"
    dolt add -A && dolt commit -m "added (7,7)"
    dolt sql -q "INSERT INTO test VALUES (8,8)"
    dolt add -A && dolt commit -m "added (8,8)"
    dolt sql -q "INSERT INTO test VALUES (9,9)"
    dolt add -A && dolt commit -m "added (9,9)"

    dolt filter-branch -q "DELETE FROM test WHERE pk > 2;" HEAD~2

    run dolt sql -q "SELECT max(pk), max(c0) FROM test AS OF 'HEAD';" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2,2" ]] || false
    run dolt sql -q "SELECT max(pk), max(c0) FROM test AS OF 'HEAD~1';" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2,2" ]] || false
    run dolt sql -q "SELECT max(pk), max(c0) FROM test AS OF 'HEAD~2';" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "7,7" ]] || false
}

function setup_write_test {
    dolt sql -q "INSERT INTO test VALUES (4,4);"
    dolt add -A && dolt commit -m "4"

    dolt sql -q "INSERT INTO test VALUES (5,5);"
    dolt add -A && dolt commit -m "5"
}

@test "filter-branch: INSERT INTO" {
    setup_write_test

    dolt filter-branch -q "INSERT INTO test VALUES (9,9);"

    run dolt sql -q "SELECT pk,c0 FROM dolt_history_test ORDER BY pk DESC LIMIT 4;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "9,9" ]] || false
    [[ "$output" =~ "9,9" ]] || false
    [[ "$output" =~ "9,9" ]] || false
    [[ "$output" =~ "5,5" ]] || false
}

@test "filter-branch: UPDATE" {
    setup_write_test

    dolt filter-branch -q "UPDATE test SET c0 = 9 WHERE pk = 2;"

    run dolt sql -q "SELECT pk,c0 FROM dolt_history_test ORDER BY c0 DESC LIMIT 4;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2,9" ]] || false
    [[ "$output" =~ "2,9" ]] || false
    [[ "$output" =~ "2,9" ]] || false
    [[ "$output" =~ "5,5" ]] || false
}

@test "filter-branch: ADD/DROP column" {
    setup_write_test

    dolt filter-branch -q "ALTER TABLE TEST ADD COLUMN c1 int;"

    for commit in HEAD HEAD~1 HEAD~2; do
        run dolt sql -q "SELECT * FROM test AS OF '$commit';" -r csv
        [ "$status" -eq 0 ]
        [[ "$output" =~ "pk,c0,c1" ]] || false
    done

    dolt filter-branch -q "ALTER TABLE TEST DROP COLUMN c0;"

    for commit in HEAD HEAD~1 HEAD~2; do
        run dolt sql -q "SELECT * FROM test AS OF '$commit';" -r csv
        [ "$status" -eq 0 ]
        [[ "$output" =~ "pk,c1" ]] || false
    done
}

@test "filter-branch: ADD/DROP table" {
    setup_write_test

    for commit in HEAD HEAD~1 HEAD~2; do
        run dolt sql -q "SHOW TABLES AS OF '$commit';" -r csv
        [ "$status" -eq 0 ]
        [[ ! "$output" =~ "added" ]] || false
    done

    dolt filter-branch -q "CREATE TABLE added (pk int PRIMARY KEY);"

    for commit in HEAD HEAD~1 HEAD~2; do
        run dolt sql -q "SHOW TABLES AS OF '$commit';" -r csv
        [ "$status" -eq 0 ]
        [[ "$output" =~ "added" ]] || false
    done

    for commit in HEAD HEAD~1 HEAD~2; do
        run dolt sql -q "SHOW TABLES AS OF '$commit';" -r csv
        [ "$status" -eq 0 ]
        [[ "$output" =~ "to_drop" ]] || false
    done

    dolt filter-branch -q "DROP TABLE to_drop;"

    for commit in HEAD HEAD~1 HEAD~2; do
        run dolt sql -q "SHOW TABLES AS OF '$commit';" -r csv
        [ "$status" -eq 0 ]
        [[ ! "$output" =~ "to_drop" ]] || false
    done
}

@test "filter-branch: error on conflict" {
    setup_write_test

    run dolt filter-branch -q "INSERT INTO test VALUES (1,2);"
    [ "$status" -ne 0 ]
    [[ ! "$output" =~ "panic" ]] || false

    run dolt filter-branch -q "REPLACE INTO test VALUES (1,2);"
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT pk,c0 FROM dolt_history_test WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "1,2" ]] || false
}

@test "filter-branch: error on incorrect schema" {
    setup_write_test

    dolt sql <<SQL
ALTER TABLE test ADD COLUMN c1 int;
INSERT INTO test VALUES (6,6,6);
SQL
    dolt add -A && dolt commit -m "added column c1"

    run dolt filter-branch -q "INSERT INTO test VALUES (9,9);"
    [ "$status" -ne 0 ]
    [[ ! "$output" =~ "panic" ]] || false

    run dolt filter-branch -q "INSERT INTO test (pk,c0) VALUES (9,9);"
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT pk,c0 FROM dolt_history_test WHERE pk=9;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "9,9" ]] || false
    [[ "$output" =~ "9,9" ]] || false
    [[ "$output" =~ "9,9" ]] || false
    [[ "$output" =~ "9,9" ]] || false
}

@test "filter-branch: continue on errors" {
    dolt sql <<SQL
create table t1 (a int primary key, b int);
insert into t1 values (1,1), (2,2);
call dolt_commit('-Am', 'added t1');

create table t2 (c int primary key, d int);
insert into t2 values (1,1), (2,2);
call dolt_commit('-Am', 'added t2');

create table t3 (e int primary key, f int);
insert into t3 values (1,1), (2,2);
call dolt_commit('-Am', 'added t3');

alter table t1 add column x int;
call dolt_commit('-Am', 'altered t1, added x column');

insert into t1 values (3,3,3);
call dolt_commit('-Am', 'more data in t1');
SQL

    # Ignore errors for the table / column not existing
    run dolt filter-branch --continue << SQL
alter table t2 add column z int not null default (c+d);
alter table t1 drop column x;
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT * from t2 order by c" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1,2" ]] || false
    [[ "$output" =~ "2,2,4" ]] || false

    run dolt sql -q "SELECT * from t1 order by a" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ ! "$output" =~ "3,3,3" ]] || false
}

@test "filter-branch: run a stored procedure" {
    dolt sql <<SQL
create table t1 (a int primary key, b int);
insert into t1 values (1,1), (2,2);
call dolt_commit('-Am', 'added t1');

create table t2 (c int primary key, d int);
insert into t2 values (1,1), (2,2);
call dolt_commit('-Am', 'added t2');
SQL

    dolt filter-branch --continue << SQL
delimiter $$
create procedure addrows()
begin
  insert into t2 values (20, 20);
  insert into t2 values (30, 30);
end$$
delimiter ;
call addrows();
drop procedure addrows;
SQL

    run dolt sql -q "SELECT * from t2 order by c" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "20,20" ]] || false
    [[ "$output" =~ "30,30" ]] || false

    run dolt sql -q "show create procedure addrows"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "does not exist" ]] || false
}

@test "filter-branch: fails with working and staged changes in current branch" {
    dolt sql -q "insert into test values (3, 3)"

    run dolt filter-branch -q "alter table test add column filter int"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "local changes detected on branch refs/heads/main, clear uncommitted changes (dolt stash dolt commit) before using filter-branch, or use --apply-to-uncommitted" ]] || false

    dolt add .
    run dolt filter-branch -q "alter table test add column filter int"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "local changes detected on branch refs/heads/main, clear uncommitted changes (dolt stash dolt commit) before using filter-branch, or use --apply-to-uncommitted" ]] || false

    dolt commit -m "added row"
    run dolt filter-branch -q "alter table test add column filter int"
    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "select * from test as of 'HEAD'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk,c0,filter" ]] || false
    [[ "$output" =~ "0,0," ]] || false
    [[ "$output" =~ "1,1," ]] || false
    [[ "$output" =~ "2,2," ]] || false
    [[ "$output" =~ "3,3," ]] || false
}

@test "filter-branch: works with working and staged changes in other branch" {
    dolt sql << SQL
call dolt_checkout('-b', 'other');
insert into test values (3, 3);
SQL
    run dolt filter-branch -q "alter table test add column filter int"
    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "select * from test as of 'HEAD'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk,c0,filter" ]] || false
    [[ "$output" =~ "0,0," ]] || false
    [[ "$output" =~ "1,1," ]] || false
    [[ "$output" =~ "2,2," ]] || false
    [[ ! "$output" =~ "3,3," ]] || false

    dolt checkout other
    run dolt filter-branch -q "alter table test add column filter int"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "local changes detected on branch refs/heads/other, clear uncommitted changes (dolt stash dolt commit) before using filter-branch, or use --apply-to-uncommitted" ]] || false
}

@test "filter-branch: --all fails with working and staged changes on other branch" {
    dolt sql << SQL
call dolt_checkout('-b', 'other');
insert into test values (3, 3);
SQL

    run dolt filter-branch --all -q "alter table test add column filter int"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "local changes detected on branch refs/heads/other, clear uncommitted changes (dolt stash dolt commit) before using filter-branch, or use --apply-to-uncommitted" ]] || false

    dolt checkout other

    dolt add .
    run dolt filter-branch --all -q "alter table test add column filter int"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "local changes detected on branch refs/heads/other, clear uncommitted changes (dolt stash dolt commit) before using filter-branch, or use --apply-to-uncommitted" ]] || false

    dolt commit -m "added row"
    run dolt filter-branch --all -q "alter table test add column filter int"
    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "select * from test as of 'HEAD'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk,c0,filter" ]] || false
    [[ "$output" =~ "0,0," ]] || false
    [[ "$output" =~ "1,1," ]] || false
    [[ "$output" =~ "2,2," ]] || false
    [[ "$output" =~ "3,3," ]] || false
}

@test "filter-branch: --branches fails with working and staged changes" {
    dolt sql << SQL
call dolt_checkout('-b', 'other');
insert into test values (3, 3);
SQL

    run dolt filter-branch --branches -q "alter table test add column filter int"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "local changes detected on branch refs/heads/other, clear uncommitted changes (dolt stash dolt commit) before using filter-branch, or use --apply-to-uncommitted" ]] || false

    dolt checkout other

    dolt add .
    run dolt filter-branch --branches -q "alter table test add column filter int"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "local changes detected on branch refs/heads/other, clear uncommitted changes (dolt stash dolt commit) before using filter-branch, or use --apply-to-uncommitted" ]] || false

    dolt commit -m "added row"
    run dolt filter-branch --branches -q "alter table test add column filter int"
    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "select * from test as of 'HEAD'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk,c0,filter" ]] || false
    [[ "$output" =~ "0,0," ]] || false
    [[ "$output" =~ "1,1," ]] || false
    [[ "$output" =~ "2,2," ]] || false
    [[ "$output" =~ "3,3," ]] || false
}

@test "filter-branch: --continue still fails with working and staged changes" {
    dolt sql -q "insert into test values (3, 3)"

    run dolt filter-branch --continue -q "alter table test add column filter int"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "local changes detected on branch refs/heads/main, clear uncommitted changes (dolt stash dolt commit) before using filter-branch, or use --apply-to-uncommitted" ]] || false

    dolt add .
    run dolt filter-branch --continue -q "alter table test add column filter int"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "local changes detected on branch refs/heads/main, clear uncommitted changes (dolt stash dolt commit) before using filter-branch, or use --apply-to-uncommitted" ]] || false

    dolt commit -m "added row"
    run dolt filter-branch --continue -q "alter table test add column filter int"
    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "select * from test as of 'HEAD'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk,c0,filter" ]] || false
    [[ "$output" =~ "0,0," ]] || false
    [[ "$output" =~ "1,1," ]] || false
    [[ "$output" =~ "2,2," ]] || false
    [[ "$output" =~ "3,3," ]] || false
}

@test "filter-branch: --apply-to-uncommitted applies changes to unstaged tables on current branch" {
    dolt sql -q "insert into test values (3, 3)"

    run dolt sql -r csv -q "select * from dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "table_name,staged,status" ]] || false
    [[ "$output" =~ "test,0,modified" ]] || false

    run dolt filter-branch --apply-to-uncommitted -q "alter table test add column filter int"
    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "select * from dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "table_name,staged,status" ]] || false
    [[ "$output" =~ "test,0,modified" ]] || false

    run dolt sql -r csv -q "select * from test as of 'HEAD'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk,c0,filter" ]] || false
    [[ "$output" =~ "0,0," ]] || false
    [[ "$output" =~ "1,1," ]] || false
    [[ "$output" =~ "2,2," ]] || false
    [[ ! "$output" =~ "3,3," ]] || false
}

@test "filter-branch: --apply-to-uncommitted applies changes to staged tables on current branch" {
    dolt sql -q "insert into test values (3, 3)"
    dolt add .

    run dolt sql -r csv -q "select * from dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "table_name,staged,status" ]] || false
    [[ "$output" =~ "test,1,modified" ]] || false

    run dolt filter-branch --apply-to-uncommitted -q "alter table test add column filter int"
    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "select * from dolt_status"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "table_name,staged,status" ]] || false
    [[ "$output" =~ "test,1,modified" ]] || false

    run dolt sql -r csv -q "select * from test as of 'HEAD'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk,c0,filter" ]] || false
    [[ "$output" =~ "0,0," ]] || false
    [[ "$output" =~ "1,1," ]] || false
    [[ "$output" =~ "2,2," ]] || false
    [[ ! "$output" =~ "3,3," ]] || false
}

@test "filter-branch: --apply-to-uncommitted applies changes to unstaged tables on other branch" {
    dolt sql << SQL
call dolt_checkout('-b', 'other');
insert into test values (3, 3);
SQL

    # no changes on main branch
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    # changes on other branch
    run dolt sql -r csv -q "call dolt_checkout('other'); select * from dolt_status;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "table_name,staged,status" ]] || false
    [[ "$output" =~ "test,0,modified" ]] || false

    run dolt filter-branch --apply-to-uncommitted --branches -q "alter table test add column filter int"
    [ "$status" -eq 0 ]

    # still no changes on main branch
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    # changes on other branch
    run dolt sql -r csv -q "call dolt_checkout('other'); select * from dolt_status;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "table_name,staged,status" ]] || false
    [[ "$output" =~ "test,0,modified" ]] || false

    run dolt sql -r csv -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk,c0,filter" ]] || false
    [[ "$output" =~ "0,0," ]] || false
    [[ "$output" =~ "1,1," ]] || false
    [[ "$output" =~ "2,2," ]] || false
    [[ ! "$output" =~ "3,3," ]] || false

    run dolt sql -r csv -q "call dolt_checkout('other'); select * from test;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk,c0,filter" ]] || false
    [[ "$output" =~ "0,0," ]] || false
    [[ "$output" =~ "1,1," ]] || false
    [[ "$output" =~ "2,2," ]] || false
    [[ "$output" =~ "3,3," ]] || false
}

@test "filter-branch: --apply-to-uncommitted applies changes to staged tables on other branch" {
    dolt sql << SQL
call dolt_checkout('-b', 'other');
insert into test values (3, 3);
call dolt_add('.');
SQL

    # no changes on main branch
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    # changes on other branch
    run dolt sql -r csv -q "call dolt_checkout('other'); select * from dolt_status;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "table_name,staged,status" ]] || false
    [[ "$output" =~ "test,1,modified" ]] || false

    run dolt filter-branch --apply-to-uncommitted --branches -q "alter table test add column filter int"
    [ "$status" -eq 0 ]

    # still no changes on main branch
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    # changes on other branch
    run dolt sql -r csv -q "call dolt_checkout('other'); select * from dolt_status;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "table_name,staged,status" ]] || false
    [[ "$output" =~ "test,1,modified" ]] || false

    run dolt sql -r csv -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk,c0,filter" ]] || false
    [[ "$output" =~ "0,0," ]] || false
    [[ "$output" =~ "1,1," ]] || false
    [[ "$output" =~ "2,2," ]] || false
    [[ ! "$output" =~ "3,3," ]] || false

    run dolt sql -r csv -q "call dolt_checkout('other'); select * from test;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "pk,c0,filter" ]] || false
    [[ "$output" =~ "0,0," ]] || false
    [[ "$output" =~ "1,1," ]] || false
    [[ "$output" =~ "2,2," ]] || false
    [[ "$output" =~ "3,3," ]] || false
}