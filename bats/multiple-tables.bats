#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE test1 (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql <<SQL
CREATE TABLE test2 (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    # L&R must be removed (or added and committed) for
    # cleanliness of the working directory used in these tests
    rm "LICENSE.md"
    rm "README.md"
}

teardown() {
    teardown_common
}

@test "examine a multi table repo" {
      run dolt ls
      [ "$status" -eq 0 ]
      [[ "$output" =~ "test1" ]] || false
      [[ "$output" =~ "test2" ]] || false
      [ "${#lines[@]}" -eq 3 ]
      run dolt schema show
      [ "$status" -eq 0 ]
      [[ "$output" =~ "test1 @ working" ]] || false
      [[ "$output" =~ "test2 @ working" ]] || false
      run dolt status 
      [ "$status" -eq 0 ]
      [[ "$output" =~ "test1" ]] || false
      [[ "$output" =~ "test2" ]] || false
}

@test "modify both tables, commit only one" {
    dolt sql -q "insert into test1 values (0, 1, 2, 3, 4, 5)"
    dolt sql -q "insert into test2 values (0, 1, 2, 3, 4, 5)"
    dolt add test1
    run dolt status
    [[ "$output" =~ "Changes to be committed" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
    run dolt commit -m "added one table"
    run dolt status
    [[ ! "$output" =~ "Changes to be committed" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
    run dolt diff
    [[ "$output" =~ "test2" ]] || false
    run dolt checkout test2
    [ "$output" = "" ]
    run dolt status
    [[ "$output" =~ "nothing to commit" ]] || false
    run dolt ls
    [[ "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "test2" ]] || false
    [ "${#lines[@]}" -eq 2 ]
}

@test "dolt add --all and dolt add . adds all changes" {
    dolt sql -q "insert into test1 values (0, 1, 2, 3, 4, 5)"
    dolt sql -q "insert into test2 values (0, 1, 2, 3, 4, 5)"
    dolt add --all
    run dolt status
    [[ "$output" =~ "Changes to be committed" ]] || false
    [[ ! "$output" =~ "Untracked files" ]] || false
    run dolt reset test1 test2
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status 
    [[ ! "$output" =~ "Changes to be committed" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [[ "$output" =~ "Changes to be committed" ]] || false
    [[ ! "$output" =~ "Untracked files" ]] || false
}

@test "dolt reset . resets all tables" {
    dolt add --all
    run dolt status
    [[ "$output" =~ "Changes to be committed" ]] || false
    [[ ! "$output" =~ "Untracked files" ]] || false
    run dolt reset .
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status 
    [[ ! "$output" =~ "Changes to be committed" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
}

@test "dolt reset --hard" {
    dolt add --all
    run dolt status
    [[ "$output" =~ "Changes to be committed" ]] || false
    [[ ! "$output" =~ "Untracked files" ]] || false
    run dolt reset .
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status 
    [[ ! "$output" =~ "Changes to be committed" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false

    dolt add --all
    dolt commit -m "commit file1 and file2"

    dolt sql -q "insert into test1 values (0, 1, 2, 3, 4, 5)"
    dolt sql -q "insert into test2 values (0, 1, 2, 3, 4, 5)"

    dolt sql <<SQL
CREATE TABLE test3 (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql <<SQL
CREATE TABLE test4 (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL

    run dolt status
    [[ "$output" =~ modified.*test1 ]] || false
    [[ "$output" =~ modified.*test2 ]] || false
    [[ "$output" =~ file.*test3 ]] || false
    [[ "$output" =~ file.*test4 ]] || false

    dolt add test1 test2 test3
    dolt reset --hard

    run dolt status
    [[ ! "$output" =~ "test1" ]] || false
    [[ ! "$output" =~ "test2" ]] || false
    [[ "$output" =~ file.*test3 ]] || false
    [[ "$output" =~ file.*test4 ]] || false
}

@test "dolt reset --hard on new tables" {
    # Per Git, dolt reset --hard on an untracked table should do nothing
    dolt reset --hard
    run dolt status
    [[ "$output" =~ table.*test1 ]] || false
    [[ "$output" =~ table.*test2 ]] || false

    # Per Git, if you add the table and do git reset --hard, the tables
    # should be deleted
    dolt add test1 test2
    dolt reset --hard
    run dolt status
    skip "dolt reset --hard does not delete tracked new tables"
    [[ ! "$output" =~ table.*test1 ]] || false
    [[ ! "$output" =~ table.*test2 ]] || false
    [[ "$output" =~ "nothing to commit" ]] || false
}
