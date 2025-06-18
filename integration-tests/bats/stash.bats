#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))"
    dolt add .
    dolt commit -am "Created table"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "stash: stashing on clean working set" {
    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No local changes to save" ]] || false
}

@test "stash: simple stashing and popping stash" {
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq 0 ]
    result=$output

    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Saved working directory and index state" ]] || false

    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "stash@{0}" ]] || false

    run dolt stash pop
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Dropped refs/stash@{0}" ]] || false

    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq 0 ]
    [ "$output" = "$result" ]
}

@test "stash: clearing stash when stash list is empty" {
    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    run dolt stash clear
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "stash: clearing stash removes all entries in stash list" {
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    dolt stash

    dolt sql -q "INSERT INTO test VALUES (2, 'b')"
    dolt stash

    dolt sql -q "INSERT INTO test VALUES (3, 'c')"
    dolt stash

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]

    run dolt stash clear
    [ "$status" -eq 0 ]

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

@test "stash: clearing stash and stashing again" {
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    dolt stash

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    run dolt stash clear
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO test VALUES (2, 'b')"
    dolt stash
    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
}

@test "stash: clearing stash and popping returns error of no entries found" {
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    dolt stash
    dolt sql -q "INSERT INTO test VALUES (2, 'b')"
    dolt stash

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]

    run dolt stash clear
    [ "$status" -eq 0 ]

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]

    run dolt stash pop
    [ "$status" -eq 1 ]
    [[ "$output" =~ "No stash entries found." ]] || false

    dolt sql -q "INSERT INTO test VALUES (2, 'b')"
    dolt stash
    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
}

@test "stash: popping oldest stash" {
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Saved working directory and index state" ]] || false
    [[ "$output" =~ "Created table" ]] || false

    dolt sql -q "INSERT INTO test VALUES (2, 'b')"
    dolt commit -am "Added row 2 b"

    dolt sql -q "INSERT INTO test VALUES (3, 'c')"
    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Saved working directory and index state" ]] || false
    [[ "$output" =~ "Added row 2 b" ]] || false

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "stash@{0}" ]] || false
    [[ "${lines[1]}" =~ "stash@{1}" ]] || false

    # stash@{1} is older stash than stash@{0}, which is the latest
    run dolt stash pop stash@{1}
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Dropped refs/stash@{1}" ]] || false

    run dolt sql -q "SELECT * FROM test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,a" ]] || false
}

@test "stash: popping neither latest nor oldest stash" {
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Created table" ]] || false

    dolt sql -q "INSERT INTO test VALUES (2, 'b')"
    dolt commit -am "Added row 2 b"

    dolt sql -q "INSERT INTO test VALUES (3, 'c')"
    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Added row 2 b" ]] || false

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]

    dolt sql -q "INSERT INTO test VALUES (4, 'd')"
    dolt commit -am "Added row 4 d"

    dolt sql -q "INSERT INTO test VALUES (5, 'e')"
    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Added row 4 d" ]] || false

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]

    run dolt stash pop stash@{1}
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Dropped refs/stash@{1}" ]] || false

    run dolt sql -q "SELECT * FROM test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3,c" ]] || false

    dolt checkout test
    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]

    run dolt stash pop stash@{1}
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Dropped refs/stash@{1}" ]] || false

    run dolt sql -q "SELECT * FROM test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,a" ]] || false
}

@test "stash: stashing multiple entries on different branches" {
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    run dolt stash
    [ "$status" -eq 0 ]

    dolt checkout -b newbranch
    dolt sql -q "INSERT INTO test VALUES (1, 'b')"
    run dolt stash
    [ "$status" -eq 0 ]

    run dolt stash list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "stash@{0}: WIP on refs/heads/newbranch:" ]] || false
    [[ "$output" =~ "stash@{1}: WIP on refs/heads/main:" ]] || false
}

@test "stash: popping stash on different branch" {
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    run dolt stash
    [ "$status" -eq 0 ]

    run dolt stash list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "stash@{0}: WIP on refs/heads/main:" ]] || false

    dolt checkout -b newbranch
    run dolt stash pop
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "1,a" ]] || false
}

@test "stash: dropping stash removes an entry at given index in stash list" {
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Created table" ]] || false

    dolt sql -q "INSERT INTO test VALUES (2, 'b')"
    dolt commit -am "Added row 2 b"

    dolt sql -q "INSERT INTO test VALUES (3, 'c')"
    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Added row 2 b" ]] || false

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]

    dolt sql -q "INSERT INTO test VALUES (4, 'd')"
    dolt commit -am "Added row 4 d"

    dolt sql -q "INSERT INTO test VALUES (5, 'e')"
    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Added row 4 d" ]] || false

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "${lines[0]}" =~ "Added row 4 d" ]] || false
    [[ "${lines[1]}" =~ "Added row 2 b" ]] || false
    [[ "${lines[2]}" =~ "Created table" ]] || false

    run dolt stash drop stash@{1}
    [ "$status" -eq 0 ]

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "Added row 4 d" ]] || false
    [[ "${lines[1]}" =~ "Created table" ]] || false
    [[ ! "$output" =~ "Added row 2 b" ]] || false
}

@test "stash: popping stash on dirty working set with no conflict" {
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    run dolt stash
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO test VALUES (2, 'b')"
    run dolt sql -q "SELECT * FROM test" -r csv
    [ "$status" -eq 0 ]
    [[ ! "$output" = "1,a" ]] || false

    run dolt stash pop 0
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Dropped refs/stash@{0}" ]] || false

    run dolt sql -q "SELECT * FROM test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,b" ]] || false
}

@test "stash: popping stash on dirty working set with conflict" {
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    run dolt stash
    [ "$status" -eq 0 ]

    dolt sql -q "INSERT INTO test VALUES (1, 'b')"
    run dolt stash pop
    echo "$output"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: Your local changes to the following tables would be overwritten by applying stash" ]] || false
    [[ "$output" =~ "The stash entry is kept in case you need it again." ]] || false

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "1,b" ]] || false
    [[ ! "$output" =~ "1,a" ]] || false

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
}

@test "stash: stashing both modified staged and working set of changes and popping the stash" {
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    dolt add .
    dolt sql -q "INSERT INTO test VALUES (2, 'b')"

    run dolt status
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false

    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq 0 ]
    result=$output

    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Saved working directory and index state" ]] || false

    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]

    # both staged and working set changes will be in current working set only; no staged change
    run dolt stash pop
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "Dropped refs/stash@{0}" ]] || false

    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq 0 ]
    [ "$output" = "$result" ]
}

@test "stash: stashing on working set with untracked tables only should be nothing to stash" {
    dolt sql -q "CREATE TABLE new_table (id INT PRIMARY KEY);"

    run dolt status
    [[ ! "$output" =~ "Changes to be committed:" ]] || false
    [[ ! "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "Untracked tables:" ]] || false

    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No local changes to save" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new_table" ]] || false
}

@test "stash: stashing untracked tables with --include-untracked flag and popping the table should not be staged" {
    dolt sql -q "CREATE TABLE new_table (id INT PRIMARY KEY);"
    dolt add .
    dolt sql -q "CREATE TABLE test_table (id INT);"

    run dolt status
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ ! "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "Untracked tables:" ]] || false

    run dolt stash --include-untracked
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Saved working directory and index state" ]] || false

    run dolt status
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt stash pop
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "Changes to be committed:" ]] || false
    [[ "${lines[3]}" =~ "new table:        new_table" ]] || false
    [[ "${lines[4]}" =~ "Untracked tables:" ]] || false
    [[ "${lines[6]}" =~ "new table:        test_table" ]] || false
}

@test "stash: stashing staged new table changes and popping the stash, the added table should be staged" {
    dolt sql -q "CREATE TABLE new_table (id INT PRIMARY KEY);"
    dolt add .

    run dolt status
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ ! "$output" =~ "Changes not staged for commit:" ]] || false
    [[ ! "$output" =~ "Untracked tables:" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new_table" ]] || false

    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Saved working directory and index state" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "new_table" ]] || false

    # staged new table change should be in the current staged set
    run dolt stash pop
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ ! "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "Dropped refs/stash@{0}" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new_table" ]] || false
}

@test "stash: stashing with staged, working set changes with untracked tables should only stash modified working set and staged changes" {
    dolt sql -q "CREATE TABLE new_table (id INT PRIMARY KEY);"
    dolt add .
    dolt sql -q "INSERT INTO new_table VALUES (1),(2);"
    dolt sql -q "CREATE TABLE test_table (id INT);"
    run dolt status
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "Untracked tables:" ]] || false

    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Saved working directory and index state" ]] || false

    run dolt status
    [[ ! "$output" =~ "Changes to be committed:" ]] || false
    [[ ! "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "Untracked tables:" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test_table" ]] || false

    # popping the stash should result in no modified working set, but with staged new_table
    run dolt stash pop
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ ! "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "Untracked tables:" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new_table" ]] || false
    [[ "$output" =~ "test_table" ]] || false
}

@test "stash: stashing working set with deleted table and popping it" {
    dolt sql -q "CREATE TABLE new_table (id INT PRIMARY KEY);"
    dolt commit -Am "create new table"

    dolt sql -q "DROP TABLE new_table;"
    run dolt status
    [[ ! "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ ! "$output" =~ "Untracked tables:" ]] || false

    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Saved working directory and index state" ]] || false

    run dolt status
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new_table" ]] || false

    # popping the stash should result in no modified working set, but with staged new_table
    run dolt stash pop
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ ! "$output" =~ "Untracked tables:" ]] || false

    run dolt ls
    [[ ! "$output" =~ "new_table" ]] || false
}

@test "stash: simple stashing and popping stash after running GC" {
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    run dolt sql -q "SELECT * FROM test"
    [ "$status" -eq 0 ]
    result=$output

    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Saved working directory and index state" ]] || false

    dolt sql -q "INSERT INTO test VALUES (2, 'b')"
    dolt commit -am "add row of 2b"

    run dolt sql -q "SELECT * FROM test" -r csv
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "1,a" ]] || false

    dolt gc

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "stash@{0}" ]] || false

    run dolt stash pop
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Dropped refs/stash@{0}" ]] || false

    run dolt sql -q "SELECT * FROM test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,a
2,b" ]] || false
}

@test "stash: popping stash with deleted table that is deleted already on current head" {
    dolt branch branch1
    dolt checkout -b branch2
    dolt sql -q "DROP TABLE test;"
    dolt commit -am "table 'test' is dropped"

    dolt checkout branch1
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    dolt sql -q "DROP TABLE test;"
    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Saved working directory and index state" ]] || false

    dolt checkout branch2
    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "stash@{0}" ]] || false

    run dolt stash pop
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
    [[ "$output" =~ "Dropped refs/stash@{0}" ]] || false
}

@test "stash: popping stash with deleted table that the same table exists on current head" {
    dolt branch branch1
    dolt branch branch2

    dolt checkout branch1
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    dolt sql -q "DROP TABLE test;"
    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Saved working directory and index state" ]] || false

    dolt checkout branch2
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "stash@{0}" ]] || false

    # if the table is the same, it's dropped
    run dolt stash pop
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "deleted:          test" ]] || false
    [[ "$output" =~ "Dropped refs/stash@{0}" ]] || false
}

@test "stash: popping stash with deleted table that different table with same name on current head gives conflict" {
    dolt branch branch1
    dolt branch branch2

    dolt checkout branch1
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    dolt sql -q "DROP TABLE test;"
    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Saved working directory and index state" ]] || false

    dolt checkout branch2
    dolt sql -q "DROP TABLE test;"
    dolt sql -q "CREATE TABLE test (id BIGINT PRIMARY KEY);"

    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "stash@{0}" ]] || false

    # if the table is different with the same name, it gives conflict
    run dolt stash pop
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table was modified in one branch and deleted in the other" ]] || false
}

@test "stash: popping stash with added table with PK on current head with the exact same table is added already" {
    dolt branch branch1
    dolt checkout -b branch2
    dolt sql -q "CREATE TABLE new_test(id INT PRIMARY KEY); INSERT INTO new_test VALUES (1);"
    dolt commit -Am "new table 'new_test' is created"

    dolt checkout branch1
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    dolt sql -q "CREATE TABLE new_test(id INT PRIMARY KEY); INSERT INTO new_test VALUES (1);"
    dolt add .
    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Saved working directory and index state" ]] || false

    dolt checkout branch2
    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "stash@{0}" ]] || false

    run dolt stash pop
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
    [[ "$output" =~ "Dropped refs/stash@{0}" ]] || false
}

@test "stash: popping stash with added keyless table on current head with the exact same table is added already" {
    dolt branch branch1
    dolt checkout -b branch2
    dolt sql -q "CREATE TABLE new_test(id INT); INSERT INTO new_test VALUES (1);"
    dolt commit -Am "new table 'new_test' is created"

    dolt checkout branch1
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    dolt sql -q "CREATE TABLE new_test(id INT); INSERT INTO new_test VALUES (1);"
    dolt add .
    run dolt stash
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Saved working directory and index state" ]] || false

    dolt checkout branch2
    run dolt stash list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "stash@{0}" ]] || false

    skip # stash of the exact copy of keyless table causes merge conflict, where it should not
    run dolt stash pop
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
    [[ "$output" =~ "Dropped refs/stash@{0}" ]] || false
}
