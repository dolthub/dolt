#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test (
    pk int primary key
);

INSERT INTO test VALUES (0),(1),(2);
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-commit: DOLT_COMMIT without a message throws error" {
    run dolt sql -q "SELECT DOLT_ADD('.')"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT DOLT_COMMIT()"
    [ $status -eq 1 ]
    run dolt log
    [ $status -eq 0 ]
    regex='Initialize'
    [[ "$output" =~ "$regex" ]] || false
}

@test "sql-commit: DOLT_COMMIT with just a message reads session parameters" {
    run dolt sql -q "SELECT DOLT_ADD('.')"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT DOLT_COMMIT('-m', 'Commit1')"
    [ $status -eq 0 ]
    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    regex='Bats Tests <bats@email.fake>'
    [[ "$output" =~ "$regex" ]] || false
}

@test "sql-commit: DOLT_COMMIT with the all flag performs properly" {
    run dolt sql -q "SELECT DOLT_COMMIT('-a', '-m', 'Commit1')"

    # Check that everything was added
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]

    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    regex='Bats Tests <bats@email.fake>'
    [[ "$output" =~ "$regex" ]] || false
}

@test "sql-commit: DOLT_COMMIT with all flag, message and author" {
    run dolt sql -r csv -q "SELECT DOLT_COMMIT('-a', '-m', 'Commit1', '--author', 'John Doe <john@doe.com>') as commit_hash"
    [ $status -eq 0 ]
    DCOMMIT=$output

    # Check that everything was added
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]

    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    regex='John Doe <john@doe.com>'
    [[ "$output" =~ "$regex" ]] || false

    # Check that dolt_log has the same hash as the output of DOLT_COMMIT
    run dolt sql -r csv -q "SELECT commit_hash from dolt_log order by date desc LIMIT 1"
    [ $status -eq 0 ]
    [[ "$output" =~ "$DCOMMIT" ]] || false

    run dolt sql -q "SELECT * from dolt_commits ORDER BY Date DESC;"
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
}

@test "sql-commit: DOLT_COMMIT works with --author without config variables set" {
    dolt config --global --unset user.name
    dolt config --global --unset user.email

    run dolt sql -q "SELECT DOLT_ADD('.')"

    run dolt sql -q "SELECT DOLT_COMMIT('-m', 'Commit1', '--author', 'John Doe <john@doe.com>') as commit_hash"
    [ "$status" -eq 0 ]
    DCOMMIT=$output

    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
    regex='John Doe <john@doe.com>'
    [[ "$output" =~ "$regex" ]] || false

    run dolt sql -q "SELECT * from dolt_log"
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false

    # Check that dolt_log has the same hash as the output of DOLT_COMMIT
    run dolt sql -q "SELECT commit_hash from dolt_log LIMIT 1"
    [ $status -eq 0 ]
    [[ "$output" =~ "$DCOMMIT" ]] || false

    run dolt sql -q "SELECT * from dolt_commits ORDER BY Date DESC;"
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
}

@test "sql-commit: DOLT_COMMIT immediately updates dolt log system table." {
    run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Commit1');
SELECT * FROM dolt_log;
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ "Commit1" ]] || false
}

@test "sql-commit: DOLT_COMMIT immediately updates dolt diff system table." {
    original_hash=$(get_head_commit)
    run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Commit1');
SELECT from_commit FROM dolt_diff_test WHERE to_commit = hashof('head');
SQL

    [ $status -eq 0 ]
    # Represents that the diff table marks a change from the recent commit.
    [[ "$output" =~ $original_hash ]] || false
}

@test "sql-commit: DOLT_COMMIT updates session variables" {
    head_variable=@@dolt_repo_$$_head
    head_commit=$(get_head_commit)
    run dolt sql << SQL
SELECT DOLT_COMMIT('-a', '-m', 'Commit1');
SELECT $head_variable = HASHOF('head');
SELECT $head_variable
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    # Verify that the head commit changes.
    [[ ! "$output" =~ $head_commit ]] || false

    # Verify that head on log matches the new session variable.
    head_commit=$(get_head_commit)
    [[ "$output" =~ $head_commit ]] || false
}

@test "sql-commit: DOLT_COMMIT with unstaged tables leaves them in the working set" {
    head_variable=@@dolt_repo_$$_head

    run dolt sql << SQL
CREATE TABLE test2 (
    pk int primary key
);
SELECT DOLT_ADD('test');
SELECT DOLT_COMMIT('-m', '0, 1, 2 in test');
SELECT $head_variable = HASHOF('head');
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    run dolt log -n1
    [ $status -eq 0 ]
    [[ "$output" =~ "0, 1, 2" ]] || false    

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test2) ]] || false

    run dolt sql -r csv -q "show tables"
    [ $status -eq 0 ]
    [[ "$output" =~ 'test2' ]] || false
    
    run dolt sql -r csv -q "select * from dolt_status;"
    [ $status -eq 0 ]
    [[ "$output" =~ 'test2,false,new table' ]] || false

    # Now another partial commit
    run dolt sql << SQL
SELECT DOLT_ADD('test2');
insert into test values (20);
SELECT DOLT_COMMIT('-m', 'added test2 table');
SELECT $head_variable = HASHOF('head');
SQL

    [ $status -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    run dolt log -n1
    [ $status -eq 0 ]
    [[ "$output" =~ "added test2 table" ]] || false    

    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "20" ]] || false
    
    run dolt sql -r csv -q "select * from dolt_status;"
    [ $status -eq 0 ]
    [[ "$output" =~ 'test,false,modified' ]] || false
}

@test "sql-commit: The -f parameter is properly parsed and executes" {
    run dolt sql <<SQL
SET FOREIGN_KEY_CHECKS=0;
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

INSERT INTO objects (id,name,color) VALUES (1,'truck','red'),(2,'ball','green'),(3,'shoe','blue');

SELECT DOLT_COMMIT('-fam', 'Commit1');
SQL

    [ $status -eq 0 ]

    run dolt sql -r csv -q "select COUNT(*) from objects;"
    [ $status -eq 0 ]
    [[ "$output" =~ '3' ]] || false

    run dolt sql -r csv -q "select COUNT(*) from dolt_log;"
    [ $status -eq 0 ]
    [[ "$output" =~ '2' ]] || false
}

@test "sql-commit: missing message does not panic and throws an error" {
    run dolt sql -q "SELECT DOLT_COMMIT('--allow-empty', '-fam')"
    [ $status -eq 1 ]
    ! [[ "$output" =~ 'panic' ]] || false
    [[ "$output" =~ 'error: no value for option `message' ]] || false
}

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | cut -c 13-44
}
