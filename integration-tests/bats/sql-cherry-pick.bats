#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10));
INSERT INTO dolt_ignore VALUES ('generated_*', 1);
SQL
    dolt add .
    dolt commit -am "Created table"
    dolt checkout -b branch1
    dolt sql -q "INSERT INTO test VALUES (1, 'a')"
    dolt commit -am "Inserted 1"
    dolt sql -q "INSERT INTO test VALUES (2, 'b')"
    dolt commit -am "Inserted 2"
    dolt sql -q "INSERT INTO test VALUES (3, 'c')"
    dolt commit -am "Inserted 3"

    dolt sql -q "CREATE TABLE generated_foo (pk int PRIMARY KEY);"

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,b" ]] || false
    [[ "$output" =~ "3,c" ]] || false
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-cherry-pick: simple cherry pick with the latest commit" {
    dolt checkout main

    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ ! "$output" =~ "1,a" ]] || false
    [[ ! "$output" =~ "2,b" ]] || false
    [[ "$output" =~ "3,c" ]] || false
}

@test "sql-cherry-pick: multiple simple cherry-picks" {
    dolt sql <<SQL
UPDATE test SET v = 'x' WHERE pk = 2;
INSERT INTO test VALUES (5, 'g'), (8, 'u');
CALL DOLT_COMMIT('-am','Updated 2b to 2x and inserted more rows');
CALL DOLT_CHECKOUT('main');
CALL DOLT_CHERRY_PICK('branch1~2');
SQL

    # we are still on branch1
    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,x" ]] || false
    [[ "$output" =~ "3,c" ]] || false
    [[ "$output" =~ "5,g" ]] || false
    [[ "$output" =~ "8,u" ]] || false

    dolt checkout main
    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "2,b" ]] || false
    [[ ! "$output" =~ "1,a" ]] || false
    [[ ! "$output" =~ "2,x" ]] || false
    [[ ! "$output" =~ "3,c" ]] || false

    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ ! "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,x" ]] || false
    [[ ! "$output" =~ "3,c" ]] || false
    [[ "$output" =~ "5,g" ]] || false
    [[ "$output" =~ "8,u" ]] || false
}

@test "sql-cherry-pick: too far back" {
    run dolt sql<<SQL
CALL DOLT_CHECKOUT('main');
CALL DOLT_CHERRY_PICK('branch1~10');
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "ancestor" ]] || false
}

@test "sql-cherry-pick: empty commit handling" {
    run dolt sql<<SQL
CALL DOLT_COMMIT('--allow-empty', '-m', 'empty commit');
CALL DOLT_CHECKOUT('main');
CALL DOLT_CHERRY_PICK('branch1');
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "The previous cherry-pick commit is empty. Use --allow-empty to cherry-pick empty commits." ]] || false

    # Calling dolt_cherry_pick with --allow-empty creates the empty commit
    run dolt sql<<SQL
CALL DOLT_CHECKOUT('main');
CALL DOLT_CHERRY_PICK('--allow-empty', 'branch1');
SQL
    [ "$status" -eq "0" ]
}

@test "sql-cherry-pick: invalid hash" {
    run dolt sql -q "CALL DOLT_CHERRY_PICK('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "target commit not found" ]] || false
}

@test "sql-cherry-pick: has changes in the working set" {
    dolt checkout main
    dolt sql -q "INSERT INTO test VALUES (4, 'f')"
    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1~2')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "cannot cherry-pick with uncommitted changes" ]] || false
}

@test "sql-cherry-pick: staged changes" {
    dolt checkout main
    dolt sql -q "INSERT INTO test VALUES (4, 'f')"
    dolt add -A
    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1~2')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "cannot cherry-pick with uncommitted changes" ]] || false
}

@test "sql-cherry-pick: insert, update, delete rows and schema changes on non existent table in working set" {
    dolt sql -q "CREATE TABLE branch1table (id int primary key, col1 int)"
    dolt add .
    dolt sql -q "INSERT INTO branch1table VALUES (9,8),(7,6),(5,4)"
    dolt commit -am "create table with rows"

    dolt sql -q "INSERT INTO branch1table VALUES (1,2)"
    dolt commit -am "Insert a row"

    dolt checkout main
    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table was modified in one branch and deleted in the other" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false

    dolt checkout branch1
    dolt sql -q "UPDATE branch1table SET col1 = 0 WHERE id > 6"
    dolt commit -am "Update a rows"

    dolt checkout main
    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table was modified in one branch and deleted in the other" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false

    dolt checkout branch1
    dolt sql -q "DELETE FROM branch1table WHERE id > 8"
    dolt commit -am "Update and delete rows"

    dolt checkout main
    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table was modified in one branch and deleted in the other" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false

    dolt checkout branch1
    dolt sql -q "ALTER TABLE branch1table ADD COLUMN col2 int"
    dolt commit -am "Alter table add column"

    dolt checkout main
    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "merge aborted: schema conflict found for table branch1table" ]] || false
    [[ "$output" =~ "table was modified in one branch and deleted in the other" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false
}

@test "sql-cherry-pick: error when using --abort with no in-progress cherry-pick" {
    run dolt sql -q "call DOLT_CHERRY_PICK('--abort');"
    [ $status -eq 1 ]
    [[ $output =~ "error: There is no cherry-pick merge to abort" ]] || false
    [[ ! $output =~ "usage: dolt cherry-pick" ]] || false
}

@test "sql-cherry-pick: conflict resolution" {
    dolt sql -q "CREATE TABLE other (pk int primary key, v int)"
    dolt add .
    dolt sql -q "INSERT INTO other VALUES (1, 2)"
    dolt sql -q "INSERT INTO test VALUES (4,'f')"
    dolt commit -am "add other table"

    dolt checkout main
    dolt sql -q "CREATE TABLE other (pk int primary key, v int)"
    dolt add .
    dolt sql -q "INSERT INTO other VALUES (1, 3)"
    dolt sql -q "INSERT INTO test VALUES (4,'k')"
    dolt commit -am "add other table with conflict and test with conflict"

    run dolt sql <<SQL
set @@dolt_allow_commit_conflicts = 1;
set @@dolt_force_transaction_commit = 1;
call DOLT_CHERRY_PICK('branch1')
SQL
    [ $status -eq 0 ]
    [[ $output =~ "|      | 2              | 0                | 0                     |" ]] || false

    run dolt conflicts cat .
    [ $status -eq 0 ]
    [[ $output =~ "|  +  | ours   | 1  | 3 |" ]] || false
    [[ $output =~ "|  +  | theirs | 1  | 2 |" ]] || false
    [[ $output =~ "|  +  | ours   | 4  | k |" ]] || false
    [[ $output =~ "|  +  | theirs | 4  | f |" ]] || false

    dolt sql -q "call DOLT_CHERRY_PICK('--abort');"

    run dolt status
    [ $status -eq 0 ]
    [[ ! $output =~ "Unmerged paths" ]] || false
    [[ ! $output =~ "generated_foo" ]] || false

    run dolt sql <<SQL
set @@dolt_allow_commit_conflicts = 1;
set @@dolt_force_transaction_commit = 1;
call DOLT_CHERRY_PICK('branch1');
SQL
    [ $status -eq 0 ]
    [[ $output =~ "|      | 2              | 0                | 0                     |" ]] || false

    run dolt status
    echo "pavel 2 >>> $output"
    [ $status -eq 0 ]
    [[ ! $output =~ "Changes to be committed" ]] || false
    [[ $output =~ "Unmerged paths" ]] || false
    [[ $output =~ "both modified:    other" ]] || false
    [[ $output =~ "both modified:    test" ]] || false

    dolt conflicts resolve --theirs .

    run dolt sql -q "select * from other"
    [ $status -eq 0 ]
    [[ $output =~ "1  | 2" ]] || false

    run dolt sql -q "select * from test"
    [ $status -eq 0 ]
    [[ $output =~ "4  | f" ]] || false

    dolt commit -m "committing cherry-picked change"
}


@test "sql-cherry-pick: commit with CREATE TABLE" {
    dolt sql -q "CREATE TABLE table_a (pk BIGINT PRIMARY KEY, v varchar(10))"
    dolt add .
    dolt sql -q "INSERT INTO table_a VALUES (11, 'aa'), (22, 'ab'), (33, 'ac')"
    dolt sql -q "DELETE FROM test WHERE pk = 2"
    dolt commit -am "Added table_a with rows and delete pk=2 from test"

    dolt checkout main
    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
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

@test "sql-cherry-pick: commit with DROP TABLE" {
    dolt sql -q "DROP TABLE test"
    dolt commit -am "Drop table test"

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "test" ]] || false

    dolt checkout main
    run dolt sql -q "SHOW TABLES" -r csv
    [[ "$output" =~ "test" ]] || false

    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table was modified in one branch and deleted in the other" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ "$output" =~ "test" ]] || false
}

@test "sql-cherry-pick: commit with ALTER TABLE rename table name" {
    # get main and branch1 in sync, so that the data on branch1 doesn't
    # cause a conflict when we cherry pick the table rename statement
    dolt checkout main
    dolt merge branch1

    dolt checkout branch1
    dolt sql -q "ALTER TABLE test RENAME TO new_name"
    dolt commit -Am "rename table name"

    dolt checkout main
    dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"

    run dolt sql -q "show tables;"
    [ $status -eq 0 ]
    [[ ! $output =~ "test" ]] || false
    [[ $output =~ "new_name" ]] || false
}

@test "sql-cherry-pick: cherry-pick commit is a merge commit" {
    dolt checkout -b branch2
    dolt sql -q "INSERT INTO test VALUES (4, 'd'), (5, 'e')"
    dolt commit -am "add more rows in branch2"

    dolt checkout branch1
    dolt sql -q "INSERT INTO test VALUES (6, 'f'), (7, 'g')"
    dolt commit -am "add more rows in branch1"
    dolt merge branch2 -m "merge branch2"

    dolt checkout main
    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ $status -eq 1 ]
    [[ $output =~ "cherry-picking a merge commit is not supported" ]] || false
}

@test "sql-cherry-pick: cherry-pick commit is a cherry-picked commit" {
    dolt checkout -b branch2
    dolt sql -q "INSERT INTO test VALUES (4, 'd'), (5, 'e')"
    dolt commit -am "add more rows in branch2"

    dolt checkout branch1
    dolt sql -q "INSERT INTO test VALUES (6, 'f'), (7, 'g')"
    dolt commit -am "add more rows in branch1"
    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch2')"
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "4,d" ]] || false
    [[ "$output" =~ "5,e" ]] || false

    dolt checkout main
    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "4,d" ]] || false
    [[ "$output" =~ "5,e" ]] || false
}

@test "sql-cherry-pick: add triggers" {
    dolt sql -q "CREATE TRIGGER trigger1 BEFORE INSERT ON test FOR EACH ROW SET new.v = concat(new.v, ' inserted')"
    dolt sql -q "CREATE view two as select 1+1"
    dolt sql -q "INSERT INTO test VALUES (4,'z')"
    run dolt sql -q "SELECT * FROM test"
    [[ "$output" =~ "z inserted" ]] || false

    dolt add .
    dolt commit -am "add trigger"

    dolt checkout main
    run dolt sql -q "SHOW TRIGGERS"
    [[ ! "$output" =~ "trigger1" ]] || false

    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test"
    [[ "$output" =~ "z inserted" ]] || false

    run dolt sql -q "SHOW TRIGGERS"
    [[ "$output" =~ "trigger1" ]] || false

    dolt checkout branch1
    dolt sql -q "DROP TRIGGER trigger1"
    dolt commit -am "drop trigger"
    dolt checkout main
    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW TRIGGERS"
    [[ ! "$output" =~ "trigger1" ]] || false
}

@test "sql-cherry-pick: add procedures" {
    dolt sql -q "CREATE PROCEDURE proc1 (in x int) select x from dual"
    run dolt sql -q "CALL proc1(434)"
    [[ "$output" =~ "434" ]] || false

    dolt add .
    dolt commit -am "add procedure"

    dolt checkout main
    run dolt sql -q "SHOW PROCEDURE STATUS"
    [[ ! "$output" =~ "proc1" ]] || false

    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW PROCEDURE STATUS"
    [[ "$output" =~ "proc1" ]] || false

    run dolt sql -q "CALL proc1(434)"
    [[ "$output" =~ "434" ]] || false
}

@test "sql-cherry-pick: keyless table" {
    dolt checkout main
    dolt sql -q "CREATE TABLE keyless (id int, name varchar(10))"
    dolt add .
    dolt commit -am "add keyless table"

    dolt checkout -b branch2
    dolt sql -q "INSERT INTO keyless VALUES (1,'1'), (2,'3')"
    dolt commit -am "insert into keyless table"

    dolt checkout main
    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ "$status" -eq "0" ]

    dolt sql -q "SELECT * FROM keyless" -r csv
    [[ ! "$output" =~ "1,1" ]] || false
    [[ ! "$output" =~ "2,3" ]] || false
}

@test "sql-cherry-pick: commit with ALTER TABLE add column" {
    dolt sql -q "ALTER TABLE test ADD COLUMN c int"
    dolt commit -am "alter table test add column c"

    dolt checkout main
    dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"

    run dolt sql -q "SHOW CREATE TABLE test;"
    [ $status -eq 0 ]
    [[ $output =~ '`c` int' ]] || false
}

@test "sql-cherry-pick: commit with ALTER TABLE change column" {
    dolt sql -q "ALTER TABLE test CHANGE COLUMN v c varchar(100)"
    dolt commit -am "alter table test change column v"

    dolt checkout main
    dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"

    run dolt sql -q "SHOW CREATE TABLE test;"
    [ $status -eq 0 ]
    [[ $output =~ '`c` varchar(100)' ]] || false
    [[ ! $output =~ '`v` varchar(10)' ]] || false
}

@test "sql-cherry-pick: commit with ALTER TABLE modify column" {
    dolt sql -q "UPDATE test SET v = '1' WHERE pk < 4"
    dolt sql -q "ALTER TABLE test MODIFY COLUMN v int"
    dolt commit -am "alter table test modify column v"

    # TODO: Incompatible type changes currently trigger an error response, instead of
    #       being tracked as a schema conflict artifact. Once we fix that, update this test.
    dolt checkout main
    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ $status -eq 1 ]
    [[ $output =~ "merge aborted: schema conflict found for table test" ]] || false
}

@test "sql-cherry-pick: commit with ALTER TABLE drop column" {
    # get main and branch1 in sync, so that the data on branch1 doesn't
    # cause a conflict when we cherry pick the alter table statement
    dolt checkout main
    dolt merge branch1

    # Drop column v on branch1
    dolt checkout branch1
    dolt sql -q "ALTER TABLE test DROP COLUMN v"
    dolt commit -am "alter table test drop column v"

    # Cherry pick the drop column change to main
    dolt checkout main
    dolt sql -q "CALL DOLT_CHERRY_PICK('branch1');"

    # Assert that column v is gone
    run dolt sql -q "SHOW CREATE TABLE test;"
    [ $status -eq 0 ]
    [[ ! $output =~ '`v` varchar(10)' ]] || false
}

@test "sql-cherry-pick: commit with ALTER TABLE rename column" {
    dolt sql -q "ALTER TABLE test RENAME COLUMN v TO c"
    dolt commit -am "alter table test rename column v"

    dolt checkout main
    dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"

    run dolt sql -q "SHOW CREATE TABLE test;"
    [ $status -eq 0 ]
    [[ ! $output =~ '`v` varchar(10)' ]] || false
    [[ $output =~ '`c` varchar(10)' ]] || false
}

@test "sql-cherry-pick: commit with ALTER TABLE drop and add primary key" {
    dolt sql -q "ALTER TABLE test DROP PRIMARY KEY, ADD PRIMARY KEY (pk, v)"
    dolt commit -am "alter table test drop and add primary key"

    dolt checkout main
    run dolt sql -q "CALL DOLT_CHERRY_PICK('branch1')"
    [ $status -eq 1 ]
    [[ $output =~ "error: cannot merge because table test has different primary keys" ]] || false
}
