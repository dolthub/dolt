#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

# Altering types and properties of the schema are not really supported by the
# command line. Have to upload schema files for these next few tests.
@test "conflict-detection-2: two branches change type of same column to same type. merge. no conflict" {
    skip "type changes are not allowed without changing tag"
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
    dolt add test
    dolt commit -m "table created"
    dolt branch change-types
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT UNSIGNED COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "changed c1 to type uint"
    dolt checkout change-types
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT UNSIGNED COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "changed c1 to type uint again"
    dolt checkout master
    run dolt merge change-types
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection-2: two branches change type of same column to different type. merge. conflict" {
    skip "type changes are not allowed without changing tag"
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
    dolt add test
    dolt commit -m "table created"
    dolt branch change-types
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT UNSIGNED COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "changed c1 to type uint"
    dolt checkout change-types
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 DOUBLE COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    skip "I think changing a type to two different types should throw a conflict"
    dolt add test
    dolt commit -m "changed c1 to type float"
    dolt checkout master
    run dolt merge change-types
    [ $status -eq 1 ]
    [[ "$output" =~ "Bad merge" ]] || false
    [ $status -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection-2: two branches make same column primary key. merge. no conflict" {
    skip "cannot resuse tags on table drop/add"
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
    dolt add test
    dolt commit -m "table created"
    dolt branch add-pk
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk,c1)
);
SQL
    dolt add test
    dolt commit -m "made c1 a pk"
    dolt checkout add-pk
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk,c1)
);
SQL
    dolt add test
    dolt commit -m "made c1 a pk again"
    dolt checkout master
    run dolt merge add-pk
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection-2: two branches add same primary key column. merge. no conflict" {
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
    dolt add test
    dolt commit -m "table created"
    dolt branch add-pk
    dolt table rm test
    skip "cannot add change primary keys"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  pk1 BIGINT NOT NULL COMMENT 'tag:6',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk,pk1)
);
SQL
    dolt add test
    dolt commit -m "added pk pk1"
    dolt checkout add-pk
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  pk1 BIGINT NOT NULL COMMENT 'tag:6',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk,pk1)
);
SQL
    dolt add test
    dolt commit -m "added pk pk1 again"
    dolt checkout master
    run dolt merge add-pk
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection-2: two branches make different columns primary key. merge. conflict" {
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
    dolt add test
    dolt commit -m "table created"
    dolt branch add-pk
    dolt table rm test
    skip "cannot change primary keys"
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  pk1 BIGINT NOT NULL COMMENT 'tag:6',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk,pk1)
);
SQL
    dolt add test
    dolt commit -m "added pk pk1"
    dolt checkout add-pk
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  pk2 BIGINT NOT NULL COMMENT 'tag:7',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk,pk2)
);
SQL
    dolt add test
    dolt commit -m "added pk pk2"
    dolt checkout master
    run dolt merge add-pk
    [ $status -eq 0 ]
    skip "This merges fine right now. Should throw conflict."
    [[ "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection-2: two branches both create different tables. merge. no conflict" {
    dolt branch table1
    dolt branch table2
    dolt checkout table1
    dolt sql <<SQL
CREATE TABLE table1 (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt add table1
    dolt commit -m "first table"
    dolt checkout table2
    dolt sql <<SQL
CREATE TABLE table2 (
  pk1 BIGINT NOT NULL,
  pk2 BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk1,pk2)
);
SQL
    dolt add table2
    dolt commit -m "second table"
    dolt checkout master
    run dolt merge table1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]] || false
    run dolt merge table2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection-2: two branches drop different tables. merge. no conflict" {
  dolt sql <<SQL
CREATE TABLE foo (
  pk BIGINT NOT NULL PRIMARY KEY
);
CREATE TABLE bar (
  pk BIGINT NOT NULL PRIMARY KEY
);
SQL
    dolt add .
    dolt commit -m "created two tables"

    dolt branch other

    dolt sql -q 'drop table foo'
    dolt add .
    dolt commit -m "dropped table foo"

    dolt checkout other
    dolt sql -q 'drop table bar'
    dolt add .
    dolt commit -m "dropped table bar"

    dolt checkout master
    skip "test currently panics on merge at doltcore/env/actions/merge.go:79"
    run dolt merge other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection-2: two branch rename different tables. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE foo (
  pk BIGINT NOT NULL PRIMARY KEY
);
CREATE TABLE bar (
  pk BIGINT NOT NULL PRIMARY KEY
);
SQL
    dolt add .
    dolt commit -m "created two tables"

    dolt branch other

    dolt sql -q 'alter table foo rename to foofoo;'
    dolt add .
    dolt commit -m "renamed table foo to foofoo"

    dolt checkout other
    dolt sql -q 'alter table bar rename to barbar'
    dolt add .
    dolt commit -m "renamed table bar to barbar"

    dolt checkout master
    skip "test currently panics on merge at doltcore/env/actions/merge.go:79"
    run dolt merge other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection-2: two branches, one deletes rows, one modifies those same rows. merge. conflict" {
    dolt sql <<SQL
CREATE TABLE foo (
  pk INT PRIMARY KEY,
  val INT
);
INSERT INTO foo VALUES (1, 1), (2, 1), (3, 1), (4, 1), (5, 1);
SQL
    dolt add foo
    dolt commit -m 'initial commit.'

    dolt checkout -b deleter
    dolt sql -q 'delete from foo'
    dolt add foo
    dolt commit -m 'delete commit.'

    dolt checkout -b modifier master
    dolt sql -q 'update foo set val = val + 1 where pk in (1, 3, 5);'
    dolt add foo
    dolt commit -m 'modify commit.'

    dolt checkout -b merge-into-modified modifier
    run dolt merge deleter
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
    dolt merge --abort

    # Accept theirs deletes all rows.
    dolt checkout master
    dolt branch -d -f merge-into-modified
    dolt checkout -b merge-into-modified modifier
    dolt merge deleter

    # Test resolve nonexistant key
    run dolt conflicts resolve foo 999
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no conflicts resolved" ]] || false

    dolt conflicts resolve --theirs foo
    run dolt sql -q 'select count(*) from foo'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 0        |" ]] || false
    dolt merge --abort
    dolt reset --hard

    # Accept ours deletes two rows.
    dolt checkout master
    dolt branch -d -f merge-into-modified
    dolt checkout -b merge-into-modified modifier
    dolt merge deleter
    dolt conflicts resolve --ours foo
    run dolt sql -q 'select count(*) from foo'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 3        |" ]] || false
    dolt merge --abort
    dolt reset --hard

    dolt checkout -b merge-into-deleter deleter
    run dolt merge modifier
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
    dolt merge --abort

    # Accept ours deletes all rows.
    dolt checkout master
    dolt branch -d -f merge-into-deleter
    dolt checkout -b merge-into-deleter deleter
    dolt merge modifier
    dolt conflicts resolve --ours foo
    run dolt sql -q 'select count(*) from foo'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 0        |" ]] || false
    dolt merge --abort
    dolt reset --hard

    # Accept theirs adds modified.
    dolt checkout master
    dolt branch -d -f merge-into-deleter
    dolt checkout -b merge-into-deleter deleter
    dolt merge modifier
    dolt conflicts resolve --theirs foo
    run dolt sql -q 'select count(*) from foo'
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 3        |" ]] || false
    dolt merge --abort
    dolt reset --hard
}
