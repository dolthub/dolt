#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10))"
    dolt commit -am "Created table"
    dolt checkout -b branch1
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    dolt commit -am "Inserted 1"
    dolt sql -q "INSERT INTO test VALUES (2, 'b')"
    dolt commit -am "Inserted 2"
    dolt sql -q "INSERT INTO test VALUES (3, 'c')"
    dolt commit -am "Inserted 3"

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,b" ]] || false
    [[ "$output" =~ "3,c" ]] || false
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "cherry-pick: simple cherry pick with the latest commit" {
    dolt checkout main

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ ! "$output" =~ "1,a" ]] || false
    [[ ! "$output" =~ "2,b" ]] || false
    [[ "$output" =~ "3,c" ]] || false
}

@test "cherry-pick: multiple simple cherry-picks" {
    dolt sql -q "UPDATE test SET v = 'x' WHERE pk = 2"
    dolt sql -q "INSERT INTO test VALUES (5, 'g'), (8, 'u');"
    dolt commit -am "Updated 2b to 2x and inserted more rows"

    dolt checkout main
    run dolt cherry-pick branch1~2
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "2,b" ]] || false
    [[ ! "$output" =~ "1,a" ]] || false
    [[ ! "$output" =~ "2,x" ]] || false
    [[ ! "$output" =~ "3,c" ]] || false

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ ! "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,x" ]] || false
    [[ ! "$output" =~ "3,c" ]] || false
    [[ "$output" =~ "5,g" ]] || false
    [[ "$output" =~ "8,u" ]] || false
}

@test "cherry-pick: too far back" {
    dolt checkout main
    run dolt cherry-pick branch1~10
    [ "$status" -eq "1" ]
    [[ "$output" =~ "ancestor" ]] || false
}

@test "cherry-pick: no changes" {
    dolt commit --allow-empty -am "empty commit"
    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "No changes were made" ]] || false
}

@test "cherry-pick: invalid hash" {
    run dolt cherry-pick aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    [ "$status" -eq "1" ]
    [[ "$output" =~ "target commit not found" ]] || false
}

@test "cherry-pick: has changes in the working set" {
    dolt checkout main
    dolt sql -q "INSERT INTO test VALUES (4, 'f')"
    run dolt cherry-pick branch1~2
    [ "$status" -eq "1" ]
    [[ "$output" =~ "changes" ]] || false
}

@test "cherry-pick: staged changes" {
    dolt checkout main
    dolt sql -q "INSERT INTO test VALUES (4, 'f')"
    dolt add -A
    run dolt cherry-pick branch1~2
    [ "$status" -eq "1" ]
    [[ "$output" =~ "You must commit any changes before using cherry-pick." ]] || false
}

@test "cherry-pick: insert, update, delete rows and schema changes on non existent table in working set" {
    dolt sql -q "CREATE TABLE branch1table (id int primary key, col1 int)"
    dolt sql -q "INSERT INTO branch1table VALUES (9,8),(7,6),(5,4)"
    dolt commit -am "create table with rows"

    dolt sql -q "INSERT INTO branch1table VALUES (1,2)"
    dolt commit -am "Insert a row"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "conflict: table with same name deleted and modified" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false

    dolt checkout branch1
    dolt sql -q "UPDATE branch1table SET col1 = 0 WHERE id > 6"
    dolt commit -am "Update a rows"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "conflict: table with same name deleted and modified" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false

    dolt checkout branch1
    dolt sql -q "DELETE FROM branch1table WHERE id > 8"
    dolt commit -am "Update and delete rows"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "conflict: table with same name deleted and modified" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false

    dolt checkout branch1
    dolt sql -q "ALTER TABLE branch1table ADD COLUMN col2 int"
    dolt commit -am "Alter table add column"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "conflict: table with same name deleted and modified" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false
}

@test "cherry-pick: commit with CREATE TABLE" {
    dolt sql -q "CREATE TABLE table_a (pk BIGINT PRIMARY KEY, v varchar(10))"
    dolt sql -q "INSERT INTO table_a VALUES (11, 'aa'), (22, 'ab'), (33, 'ac')"
    dolt sql -q "DELETE FROM test WHERE pk = 2"
    dolt commit -am "Added table_a with rows and delete pk=2 from test"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW TABLES" -r csv
    [[ "$output" =~ "table_a" ]] || false

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ ! "$output" =~ "1,a" ]] || false
    [[ ! "$output" =~ "2,b" ]] || false
    [[ ! "$output" =~ "3,c" ]] || false

    run dolt sql -q "SELECT * FROM table_a" -r csv
    [[ "$output" =~ "11,aa" ]] || false
    [[ "$output" =~ "22,ab" ]] || false
    [[ "$output" =~ "33,ac" ]] || false
}

@test "cherry-pick: commit with DROP TABLE" {
    dolt sql -q "DROP TABLE test"
    dolt commit -am "Drop table test"

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "test" ]] || false

    dolt checkout main
    run dolt sql -q "SHOW TABLES" -r csv
    [[ "$output" =~ "test" ]] || false

    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "conflict: table with same name deleted and modified" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ "$output" =~ "test" ]] || false
}

@test "cherry-pick: cherry-pick commit is a merge commit" {
    dolt checkout -b branch2
    dolt sql -q "INSERT INTO test VALUES (4, 'd'), (5, 'e')"
    dolt commit -am "add more rows in branch2"

    dolt checkout branch1
    dolt sql -q "INSERT INTO test VALUES (6, 'f'), (7, 'g')"
    dolt commit -am "add more rows in branch1"
    dolt merge branch2
    dolt commit -am "merge branch2"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "cherry-picking a merge commit is not supported." ]] || false
}

@test "cherry-pick: cherry-pick commit is a cherry-picked commit" {
    dolt checkout -b branch2
    dolt sql -q "INSERT INTO test VALUES (4, 'd'), (5, 'e')"
    dolt commit -am "add more rows in branch2"

    dolt checkout branch1
    dolt sql -q "INSERT INTO test VALUES (6, 'f'), (7, 'g')"
    dolt commit -am "add more rows in branch1"
    run dolt cherry-pick branch2
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "4,d" ]] || false
    [[ "$output" =~ "5,e" ]] || false

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "4,d" ]] || false
    [[ "$output" =~ "5,e" ]] || false
}

@test "cherry-pick: ALTER TABLE rename table name" {
    dolt sql -q "INSERT INTO test VALUES (4, 'd')"
    dolt sql -q "ALTER TABLE test RENAME TO new_name"
    dolt commit -am "rename table name"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "conflict: table with same name deleted and modified" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE add column" {
    dolt sql -q "INSERT INTO test VALUES (4, 'd')"
    dolt sql -q "ALTER TABLE test ADD COLUMN c int"
    dolt commit -am "alter table test add column c"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table schema does not match in current HEAD and cherry-pick commit" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE change column" {
    dolt sql -q "INSERT INTO test VALUES (4, 'd')"
    dolt sql -q "ALTER TABLE test CHANGE COLUMN v c varchar(100)"
    dolt commit -am "alter table test change column v"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table schema does not match in current HEAD and cherry-pick commit" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE modify column" {
    dolt sql -q "UPDATE test SET v = '1' WHERE pk < 4"
    dolt sql -q "ALTER TABLE test MODIFY COLUMN v int"
    dolt commit -am "alter table test modify column v"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table schema does not match in current HEAD and cherry-pick commit" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE drop column" {
    dolt sql -q "ALTER TABLE test DROP COLUMN v"
    dolt commit -am "alter table test drop column v"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table schema does not match in current HEAD and cherry-pick commit" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE rename column" {
    dolt sql -q "ALTER TABLE test RENAME COLUMN v TO c"
    dolt commit -am "alter table test rename column v"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table schema does not match in current HEAD and cherry-pick commit" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE drop and add primary key" {
    dolt sql -q "ALTER TABLE test DROP PRIMARY KEY, ADD PRIMARY KEY (pk, v)"
    dolt commit -am "alter table test drop and add primary key"

    dolt checkout main
    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table schema does not match in current HEAD and cherry-pick commit" ]] || false
}
