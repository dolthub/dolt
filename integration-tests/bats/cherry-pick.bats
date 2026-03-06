#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10), index(v));
INSERT INTO dolt_ignore VALUES ('generated_*', 1);
SQL
    dolt add .
    dolt commit -am "Created table"
    dolt branch branch1
    dolt --branch branch1 sql -q "INSERT INTO test VALUES (1, 'a')"
    dolt --branch branch1 commit -am "Inserted 1"
    dolt --branch branch1 sql -q "INSERT INTO test VALUES (2, 'b')"
    dolt --branch branch1 commit -am "Inserted 2"
    dolt --branch branch1 sql -q "INSERT INTO test VALUES (3, 'c')"
    dolt --branch branch1 commit -am "Inserted 3"
    dolt --branch branch1 sql -q "CREATE TABLE generated_foo (pk int PRIMARY KEY);"

    run dolt --branch branch1 sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "1,a" ]] || false
    [[ "$output" =~ "2,b" ]] || false
    [[ "$output" =~ "3,c" ]] || false
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "cherry-pick: simple cherry pick with the latest commit" {
    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ ! "$output" =~ "1,a" ]] || false
    [[ ! "$output" =~ "2,b" ]] || false
    [[ "$output" =~ "3,c" ]] || false
}

@test "cherry-pick: multiple simple cherry-picks" {
    dolt --branch branch1 sql -q "UPDATE test SET v = 'x' WHERE pk = 2"
    dolt --branch branch1 sql -q "INSERT INTO test VALUES (5, 'g'), (8, 'u');"
    dolt --branch branch1 commit -am "Updated 2b to 2x and inserted more rows"

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
    run dolt cherry-pick branch1~10
    [ "$status" -eq "1" ]
    [[ "$output" =~ "ancestor" ]] || false
}

@test "cherry-pick: empty commit handling" {
    dolt --branch branch1 commit --allow-empty -am "empty commit"

    # If an empty commit is cherry-picked, Git will stop the cherry-pick and allow you to manually commit it
    # with the --allow-empty flag. We don't support that yet, so instead, empty commits generate an error.
    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "The previous cherry-pick commit is empty. Use --allow-empty to cherry-pick empty commits." ]] || false

    # If the --allow-empty flag is specified, then empty commits can be automatically cherry-picked.
    run dolt cherry-pick --allow-empty branch1
    [ "$status" -eq "0" ]
}

@test "cherry-pick: invalid hash" {
    run dolt cherry-pick aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    [ "$status" -eq "1" ]
    [[ "$output" =~ "target commit not found" ]] || false
}

@test "cherry-pick: has changes in the working set" {
    dolt sql -q "INSERT INTO test VALUES (4, 'f')"
    run dolt cherry-pick branch1~2
    [ "$status" -eq "1" ]
    [[ "$output" =~ "your local changes would be overwritten by cherry-pick" ]] || false
}

@test "cherry-pick: staged changes" {
    dolt sql -q "INSERT INTO test VALUES (4, 'f')"
    dolt add -A
    run dolt cherry-pick branch1~2
    [ "$status" -eq "1" ]
    [[ "$output" =~ "Please commit your staged changes before using cherry-pick." ]] || false
}

@test "cherry-pick: insert, update, delete rows and schema changes on non existent table in working set" {
    dolt --branch branch1 sql -q "CREATE TABLE branch1table (id int primary key, col1 int)"
    dolt --branch branch1 add .
    dolt --branch branch1 sql -q "INSERT INTO branch1table VALUES (9,8),(7,6),(5,4)"
    dolt --branch branch1 commit -am "create table with rows"

    dolt --branch branch1 sql -q "INSERT INTO branch1table VALUES (1,2)"
    dolt --branch branch1 commit -am "Insert a row"

    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table was modified in one branch and deleted in the other" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false

    dolt --branch branch1 sql -q "UPDATE branch1table SET col1 = 0 WHERE id > 6"
    dolt --branch branch1 commit -am "Update a rows"

    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table was modified in one branch and deleted in the other" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false

    dolt --branch branch1 sql -q "DELETE FROM branch1table WHERE id > 8"
    dolt --branch branch1 commit -am "Update and delete rows"

    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table was modified in one branch and deleted in the other" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false

    dolt --branch branch1 sql -q "ALTER TABLE branch1table ADD COLUMN col2 int"
    dolt --branch branch1 commit -am "Alter table add column"

    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "merge aborted: schema conflict found for table branch1table" ]] || false
    [[ "$output" =~ "table was modified in one branch and deleted in the other" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "branch1table" ]] || false
}

@test "cherry-pick: error when using --abort with no in-progress cherry-pick" {
    run dolt cherry-pick --abort
    [ $status -eq 1 ]
    [[ $output =~ "error: There is no cherry-pick merge to abort" ]] || false
    [[ ! $output =~ "usage: dolt cherry-pick" ]] || false
}

# NM4 - breaky break.
@test "cherry-pick: schema change, with data conflict" {
    dolt sql -q "CREATE TABLE other (pk int primary key, c1 int, c2 int)"
    dolt sql -q "INSERT INTO other VALUES (1, 2, 3)"
    dolt commit -Am "add other table (on main)"

    # Create two commits on branch2: one to assert does NOT get included, and one to cherry pick
    dolt branch branch2
    dolt --branch branch2 sql -q "INSERT INTO other VALUES (100, 200, 300);"
    dolt --branch branch2 commit -am "add row 100 to other (on branch2)"

    # This ALTER TABLE statement modifies other rows that aren't included in the cherry-picked
    # commit – row (100, 200, 300) is modified to (100, 400). This shows up as a conflict
    # in the cherry-pick (modified row on one side, row doesn't exist on the other side).
    dolt --branch branch2 sql -q "ALTER TABLE other DROP COLUMN c1;"
    dolt --branch branch2 sql -q "UPDATE other SET c2 = 400 WHERE pk = 100"
    dolt --branch branch2 sql -q "INSERT INTO other VALUES (10, 30);"
    dolt --branch branch2 sql -q "INSERT INTO test VALUES (100, 'q');"
    dolt --branch branch2 commit -am "alter table, add row 10 to other, add row 100 to test (on branch2)"

    run dolt cherry-pick branch2
    [ $status -eq 1 ]
    [[ $output =~ "Unable to apply commit cleanly due to conflicts or constraint violations" ]] || false

    # Assert that table 'test' is staged, but table 'other' is not staged, since it had conflicts
    run dolt sql -r csv -q "SELECT * from dolt_status;"
    [ $status -eq 0 ]

    [[ $output =~ "test,1,modified" ]] || false
    [[ $output =~ "other,0,modified" ]] || false
    [[ $output =~ "other,0,conflict" ]] || false

    # Make sure the data conflict shows up correctly
    run dolt conflicts cat .
    [ $status -eq 0 ]
    [[ $output =~ "|  -  | ours   | 100 | 200  | 300 |" ]] || false
    [[ $output =~ "|  *  | theirs | 100 | NULL | 400 |" ]] || false

    # Asert the data we expect is in the table
    run dolt sql -r csv -q "SELECT * from other;"
    [ $status -eq 0 ]
    [[ $output =~ "1,3" ]] || false
    [[ $output =~ "10,30" ]] || false
    [[ ! $output =~ "100,300" ]] || false

    # Resolve the conflict and commit
    dolt conflicts resolve --ours other
    run dolt sql -r csv -q "SELECT count(*) from dolt_conflicts;"
    [ $status -eq 0 ]
    [[ $output =~ "0" ]] || false
    dolt commit -m "cherry-picked HEAD commit from branch2"
}

@test "cherry-pick: foreign key violation" {
    dolt --branch branch1 sql -q "CREATE TABLE other (pk int primary key, v varchar(10), FOREIGN KEY (v) REFERENCES test(v))"
    dolt --branch branch1 sql -q "INSERT INTO other VALUES (1, 'a')"
    dolt --branch branch1 commit -Am "add other table (on branch1)"

    dolt branch branch2 branch1
    dolt --branch branch2 sql -q "SET @@foreign_key_checks=0; UPDATE other SET v = 'z' where pk = 1;"
    dolt --branch branch2 sql -q "INSERT INTO test VALUES (100, 'q');"
    dolt --branch branch2 commit -Am "update row 1 in other and insert row 100 into test (on branch2)"

    dolt --branch branch1 sql -q "SET @@foreign_key_checks=1;"
    run dolt --branch branch1 cherry-pick branch2
    [ $status -eq 1 ]
    [[ $output =~ "Unable to apply commit cleanly due to conflicts or constraint violations" ]] || false

    # Assert that only 'test' is staged for commit ('other' has a constraint violation)
    run dolt --branch branch1 sql -r csv -q "SELECT * from dolt_status;"
    [ $status -eq 0 ]
    [[ $output =~ "other,0,constraint violation" ]] || false
    [[ $output =~ "test,1,modified" ]] || false

    # Assert the expected constraint violations
    run dolt --branch branch1 sql -q "SELECT * FROM dolt_constraint_violations;"
    [ $status -eq 0 ]
    [[ $output =~ "| other | 1 " ]] || false
    run dolt --branch branch1 sql -q "SELECT * FROM dolt_constraint_violations_other;"
    [ $status -eq 0 ]
    [[ $output =~ "foreign key    | 1  | z " ]] || false

    # Abort the cherry-pick and assert that all state has been properly cleared
    dolt --branch branch1 cherry-pick --abort
    run dolt --branch branch1 sql -q "SELECT * from dolt_status;"
    [ $status -eq 0 ]
    [[ ! $output =~ "other" ]] || false
    [[ ! $output =~ "test" ]] || false
    run dolt --branch branch1 sql -q "SELECT * FROM dolt_constraint_violations;"
    [ $status -eq 0 ]
    [[ ! $output =~ "other" ]] || false
    run dolt --branch branch1 sql -r csv -q "SELECT * from other;"
    [ $status -eq 0 ]
    [[ $output =~ "1,a" ]] || false
    [[ ! $output =~ "100,q" ]] || false
}

@test "cherry-pick: conflict resolution" {
    dolt --branch branch1 sql -q "CREATE TABLE other (pk int primary key, v int)"
    dolt --branch branch1 add .
    dolt --branch branch1 sql -q "INSERT INTO other VALUES (1, 2)"
    dolt --branch branch1 sql -q "INSERT INTO test VALUES (4,'f')"
    dolt --branch branch1 commit -am "add other table"

    dolt sql -q "CREATE TABLE other (pk int primary key, v int)"
    dolt add .
    dolt sql -q "INSERT INTO other VALUES (1, 3)"
    dolt sql -q "INSERT INTO test VALUES (4,'k')"
    dolt commit -am "add other table with conflict and test with conflict"

    run dolt cherry-pick branch1
    [ $status -eq 1 ]
    [[ $output =~ "Unable to apply commit cleanly due to conflicts or constraint violations" ]] || false

    run dolt conflicts cat .
    [ $status -eq 0 ]
    [[ $output =~ "|  +  | ours   | 1  | 3 |" ]] || false
    [[ $output =~ "|  +  | theirs | 1  | 2 |" ]] || false
    [[ $output =~ "|  +  | ours   | 4  | k |" ]] || false
    [[ $output =~ "|  +  | theirs | 4  | f |" ]] || false

    dolt cherry-pick --abort

    run dolt status
    [ $status -eq 0 ]
    [[ ! $output =~ "Unmerged paths" ]] || false

    run dolt cherry-pick branch1
    [ $status -eq 1 ]
    [[ $output =~ "Unable to apply commit cleanly due to conflicts or constraint violations" ]] || false

    run dolt status
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

@test "cherry-pick: commit with CREATE TABLE" {
    dolt --branch branch1 sql -q "CREATE TABLE table_a (pk BIGINT PRIMARY KEY, v varchar(10))"
    dolt --branch branch1 add .
    dolt --branch branch1 sql -q "INSERT INTO table_a VALUES (11, 'aa'), (22, 'ab'), (33, 'ac')"
    dolt --branch branch1 sql -q "DELETE FROM test WHERE pk = 2"
    dolt --branch branch1 commit -am "Added table_a with rows and delete pk=2 from test"

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
    dolt --branch branch1 sql -q "DROP TABLE test"
    dolt --branch branch1 commit -am "Drop table test"

    run dolt --branch branch1 sql -q "SHOW TABLES" -r csv
    [[ ! "$output" =~ "test" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ "$output" =~ "test" ]] || false

    run dolt cherry-pick branch1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "table was modified in one branch and deleted in the other" ]] || false

    run dolt sql -q "SHOW TABLES" -r csv
    [[ "$output" =~ "test" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE rename table name" {
    # get main and branch1 in sync, so that the data on branch1 doesn't
    # cause a conflict when we cherry pick the table rename statement
    dolt sql -q "CALL DOLT_MERGE('branch1')"

    dolt --branch branch1 sql -q "ALTER TABLE test RENAME TO new_name"
    dolt --branch branch1 commit -Am "rename table test to new_name"

    dolt cherry-pick branch1

    run dolt sql -q "show tables;"
    [ $status -eq 0 ]
    [[ ! $output =~ "test" ]] || false
    [[ $output =~ "new_name" ]] || false
}

@test "cherry-pick: cherry-pick commit is a merge commit" {
    dolt branch branch2 branch1
    dolt --branch branch2 sql -q "INSERT INTO test VALUES (4, 'd'), (5, 'e')"
    dolt --branch branch2 commit -am "add more rows in branch2"

    dolt --branch branch1 sql -q "INSERT INTO test VALUES (6, 'f'), (7, 'g')"
    dolt --branch branch1 commit -am "add more rows in branch1"
    dolt --branch branch1 sql -q "CALL DOLT_MERGE('branch2', '-m', 'merge branch2')"

    run dolt cherry-pick branch1
    [ $status -eq 1 ]
    [[ $output =~ "cherry-picking a merge commit is not supported" ]] || false
}

@test "cherry-pick: cherry-pick commit is a cherry-picked commit" {
    dolt branch branch2 branch1
    dolt --branch branch2 sql -q "INSERT INTO test VALUES (4, 'd'), (5, 'e')"
    dolt --branch branch2 commit -am "add more rows in branch2"

    dolt --branch branch1 sql -q "INSERT INTO test VALUES (6, 'f'), (7, 'g')"
    dolt --branch branch1 commit -am "add more rows in branch1"
    run dolt --branch branch1 cherry-pick branch2
    [ "$status" -eq "0" ]

    run dolt --branch branch1 sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "4,d" ]] || false
    [[ "$output" =~ "5,e" ]] || false

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SELECT * FROM test" -r csv
    [[ "$output" =~ "4,d" ]] || false
    [[ "$output" =~ "5,e" ]] || false
}

@test "cherry-pick: add triggers" {
    dolt --branch branch1 sql -q "CREATE TRIGGER trigger1 BEFORE INSERT ON test FOR EACH ROW SET new.v = concat(new.v, ' inserted')"
    dolt --branch branch1 sql -q "CREATE view two as select 1+1"
    dolt --branch branch1 sql -q "INSERT INTO test VALUES (4,'z')"
    run dolt --branch branch1 sql -q "SELECT * FROM test"
    [[ "$output" =~ "z inserted" ]] || false

    dolt --branch branch1 add .
    dolt --branch branch1 commit -am "add trigger"

    run dolt sql -q "SHOW TRIGGERS"
    [[ ! "$output" =~ "trigger1" ]] || false

    dolt cherry-pick branch1

    run dolt sql -q "SELECT * FROM test"
    [[ "$output" =~ "z inserted" ]] || false

    run dolt sql -q "SHOW TRIGGERS"
    [[ "$output" =~ "trigger1" ]] || false

    dolt --branch branch1 sql -q "DROP TRIGGER trigger1"
    dolt --branch branch1 commit -am "drop trigger"

    dolt cherry-pick branch1

    run dolt sql -q "SHOW TRIGGERS"
    [[ ! "$output" =~ "trigger1" ]] || false
}

@test "cherry-pick: drop triggers" {
    dolt --branch branch1 sql -q "CREATE TRIGGER trigger1 BEFORE INSERT ON test FOR EACH ROW SET new.v = concat(new.v, ' inserted')"
    dolt --branch branch1 sql -q "INSERT INTO test VALUES (4,'z')"
    run dolt --branch branch1 sql -q "SELECT * FROM test"
    [[ "$output" =~ "z inserted" ]] || false

    dolt --branch branch1 add .
    dolt --branch branch1 commit -am "add trigger"

    run dolt sql -q "SHOW TRIGGERS"
    [[ ! "$output" =~ "trigger1" ]] || false

    dolt cherry-pick branch1

    run dolt sql -q "SELECT * FROM test"
    [[ "$output" =~ "z inserted" ]] || false

    run dolt sql -q "SHOW TRIGGERS"
    [[ "$output" =~ "trigger1" ]] || false

    dolt --branch branch1 sql -q "DROP TRIGGER trigger1"
    dolt --branch branch1 commit -am "drop trigger"

    skip "merge cannot handle dropped dolt_schemas" table
    dolt cherry-pick branch1

    run dolt sql -q "SHOW TRIGGERS"
    [[ ! "$output" =~ "trigger1" ]] || false
}

@test "cherry-pick: add procedures" {
    dolt --branch branch1 sql -q "CREATE PROCEDURE proc1 (in x int) select x from dual"
    run dolt --branch branch1 sql -q "CALL proc1(434)"
    [[ "$output" =~ "434" ]] || false

    dolt --branch branch1 add .
    dolt --branch branch1 commit -am "add procedure"

    run dolt sql -q "SHOW PROCEDURE STATUS"
    [[ ! "$output" =~ "proc1" ]] || false

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    run dolt sql -q "SHOW PROCEDURE STATUS"
    [[ "$output" =~ "proc1" ]] || false

    run dolt sql -q "CALL proc1(434)"
    [[ "$output" =~ "434" ]] || false
}

@test "cherry-pick: keyless table" {
    dolt sql -q "CREATE TABLE keyless (id int, name varchar(10))"
    dolt add .
    dolt commit -am "add keyless table"

    dolt branch branch2
    dolt --branch branch2 sql -q "INSERT INTO keyless VALUES (1,'1'), (2,'3')"
    dolt --branch branch2 commit -am "insert into keyless table"

    run dolt cherry-pick branch1
    [ "$status" -eq "0" ]

    dolt sql -q "SELECT * FROM keyless" -r csv
    [[ ! "$output" =~ "1,1" ]] || false
    [[ ! "$output" =~ "2,3" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE add column" {
    dolt --branch branch1 sql -q "ALTER TABLE test ADD COLUMN c int"
    dolt --branch branch1 commit -am "alter table test add column c"

    dolt cherry-pick branch1

    run dolt sql -q "SHOW CREATE TABLE test;"
    [ $status -eq 0 ]
    [[ $output =~ '`c` int' ]] || false
}

@test "cherry-pick: commit with ALTER TABLE change column" {
    dolt --branch branch1 sql -q "ALTER TABLE test CHANGE COLUMN v c varchar(100)"
    dolt --branch branch1 commit -am "alter table test change column v"

    dolt cherry-pick branch1

    run dolt sql -q "SHOW CREATE TABLE test;"
    [ $status -eq 0 ]
    [[ $output =~ '`c` varchar(100)' ]] || false
    [[ ! $output =~ '`v` varchar(10)' ]] || false
}

@test "cherry-pick: commit with ALTER TABLE modify column" {
    dolt --branch branch1 sql -q "UPDATE test SET v = '1' WHERE pk < 4"
    dolt --branch branch1 sql -q "ALTER TABLE test MODIFY COLUMN v int"
    dolt --branch branch1 commit -am "alter table test modify column v"

    # TODO: Incompatible type changes currently trigger an error response, instead of
    #       being tracked as a schema conflict artifact. Once we fix that, update this test.
    run dolt cherry-pick branch1
    [ $status -eq 1 ]
    [[ $output =~ "merge aborted: schema conflict found for table test" ]] || false
}

@test "cherry-pick: commit with ALTER TABLE drop column" {
    dolt --branch branch1 sql -q "ALTER TABLE test DROP COLUMN v"
    dolt --branch branch1 commit -am "alter table test drop column v"

    # Dropping column v on branch1 modifies all rows in the table, and those rows
    # don't exist on main, so they would be a conflict. However, cell-wise merging is able to resolve the conflict.
    run dolt cherry-pick branch1
    [ $status -eq 0 ]

    run dolt sql -q "SHOW CREATE TABLE test;"
    [ $status -eq 0 ]
    [[ ! $output =~ '`v` varchar(10)' ]] || false
}

@test "cherry-pick: commit with ALTER TABLE rename column" {
    dolt --branch branch1 sql -q "ALTER TABLE test RENAME COLUMN v TO c"
    dolt --branch branch1 commit -am "alter table test rename column v"

    dolt cherry-pick branch1

    run dolt sql -q "SHOW CREATE TABLE test;"
    [ $status -eq 0 ]
    [[ ! $output =~ '`v` varchar(10)' ]] || false
    [[ $output =~ '`c` varchar(10)' ]] || false
}

@test "cherry-pick: commit with ALTER TABLE drop and add primary key" {
    dolt --branch branch1 sql -q "ALTER TABLE test DROP PRIMARY KEY, ADD PRIMARY KEY (pk, v)"
    dolt --branch branch1 commit -am "alter table test drop and add primary key"

    run dolt cherry-pick branch1
    [ $status -eq 1 ]
    [[ $output =~ "error: cannot merge because table test has different primary keys" ]] || false
}

@test "cherry-pick: author and timestamp preserved during cherry-pick" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "see: https://github.com/dolthub/dolt/issues/10116"
    fi

    dolt --branch branch1 sql -q "INSERT INTO test VALUES (99, 'auth')"
    dolt --branch branch1 add .
    
    # Create commit with specific author and timestamp
    DOLT_AUTHOR_DATE='2023-09-26T01:23:45' dolt --branch branch1 commit --author="Original Author <original@example.com>" -m "commit with specific author"
    COMMIT_HASH=$(get_head_commit branch1)
    
    run dolt --branch branch1 log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Original Author <original@example.com>" ]] || false
    
    run dolt cherry-pick $COMMIT_HASH
    [ $status -eq 0 ]
    
    run dolt log -n 1
    [ $status -eq 0 ]
    [[ "$output" =~ "Original Author <original@example.com>" ]] || false
    
    # Verify dolt_log table shows correct author and preserved timestamp
    run dolt sql -q "SELECT committer, email, date FROM dolt_log WHERE message = 'commit with specific author'" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "Original Author,original@example.com" ]] || false
    [[ "$output" =~ "2023-09-26 01:23:45" ]] || false
}

@test "cherry-pick: multiple authors preserved in sequence with merge workflow" {
    dolt --branch branch1 sql -q "INSERT INTO test VALUES (200, 'alice')"
    dolt --branch branch1 add .
    dolt --branch branch1 commit --author="Alice Developer <alice@company.com>" -m "Alice's feature"
    ALICE_HASH=$(get_head_commit branch1)
    
    dolt --branch branch1 sql -q "INSERT INTO test VALUES (201, 'bob')"
    dolt --branch branch1 add .
    dolt --branch branch1 commit --author="Bob Engineer <bob@company.com>" -m "Bob's improvement"
    BOB_HASH=$(get_head_commit branch1)
    
    dolt --branch branch1 sql -q "INSERT INTO test VALUES (202, 'carol')"
    dolt --branch branch1 add .
    dolt --branch branch1 commit --author="Carol Architect <carol@company.com>" -m "Carol's refactor"
    CAROL_HASH=$(get_head_commit branch1)
    
    run dolt cherry-pick $ALICE_HASH
    [ $status -eq 0 ]
    run dolt log -n 1
    [[ "$output" =~ "Alice Developer <alice@company.com>" ]] || false
    
    run dolt cherry-pick $BOB_HASH  
    [ $status -eq 0 ]
    run dolt log -n 1
    [[ "$output" =~ "Bob Engineer <bob@company.com>" ]] || false
    
    run dolt cherry-pick $CAROL_HASH
    [ $status -eq 0 ]
    run dolt log -n 1
    [[ "$output" =~ "Carol Architect <carol@company.com>" ]] || false
    
    # Test merge workflow - customer requirement for merge commits
    dolt branch integration_branch
    dolt --branch integration_branch sql -q "INSERT INTO test VALUES (300, 'merge')"
    dolt --branch integration_branch add .
    dolt --branch integration_branch commit --author="Integration Manager <integration@company.com>" -m "prepare for merge"

    run dolt sql -q "CALL DOLT_MERGE('integration_branch','--no-ff', '-m', 'Merge integration_branch')"
    [ $status -eq 0 ]
    
    run dolt log -n 1
    [[ "$output" =~ "Merge integration_branch" ]] || false
    
    run dolt log -n 5
    [[ "$output" =~ "Alice Developer <alice@company.com>" ]] || false
    [[ "$output" =~ "Bob Engineer <bob@company.com>" ]] || false  
    [[ "$output" =~ "Carol Architect <carol@company.com>" ]] || false
    [[ "$output" =~ "Integration Manager <integration@company.com>" ]] || false
    
    # Verify dolt_log table shows all authors correctly in commit order (including merge workflow)
    run dolt sql -q "SELECT committer, email, message FROM dolt_log WHERE committer IN ('Alice Developer', 'Bob Engineer', 'Carol Architect', 'Integration Manager') OR message LIKE 'Merge%' ORDER BY commit_order" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "Alice Developer,alice@company.com" ]] || false
    [[ "$output" =~ "Bob Engineer,bob@company.com" ]] || false
    [[ "$output" =~ "Carol Architect,carol@company.com" ]] || false
    [[ "$output" =~ "Integration Manager,integration@company.com" ]] || false
    [[ "$output" =~ "Merge integration_branch" ]] || false
}
@test "cherry-pick: --continue after resolving conflicts" {
    dolt branch continue_test
    dolt --branch continue_test sql -q "INSERT INTO test VALUES (100, 'branch1')"
    dolt --branch continue_test add .
    dolt --branch continue_test commit --author="Feature Dev <feature@example.com>" --date="2022-01-01T12:00:00" -m "Add row from branch1"
    COMMIT1=$(get_head_commit continue_test)
    
    dolt sql -q "INSERT INTO test VALUES (100, 'main')"
    dolt add .
    dolt commit -am "Add row from main"
    
    run dolt cherry-pick $COMMIT1
    [ $status -eq 1 ]
    [[ $output =~ "Unable to apply commit cleanly due to conflicts" ]] || false

    # Resolve the conflict (need to disable autocommit for conflict resolution)
    dolt sql -q "SET autocommit = 0; UPDATE test SET v = 'resolved' WHERE pk = 100; DELETE FROM dolt_conflicts_test; COMMIT;"
    dolt add test

    run dolt cherry-pick --continue --abort
    [ $status -eq 1 ]
    [[ $output =~ "--continue and --abort are mutually exclusive" ]] || false

    run dolt cherry-pick --continue
    [ $status -eq 0 ]
    
    # Verify the commit was created with original metadata
    run dolt log -n 1
    [ $status -eq 0 ]
    [[ $output =~ "Feature Dev" ]] || false
    [[ $output =~ "feature@example.com" ]] || false
    [[ $output =~ "Add row from branch1" ]] || false
    
    # Verify the resolved data is present
    run dolt sql -q "SELECT * FROM test WHERE pk = 100" -r csv
    [ $status -eq 0 ]
    [[ $output =~ "100,resolved" ]] || false
}

@test "cherry-pick: --continue with no active cherry-pick" {
    run dolt cherry-pick --continue
    [ $status -eq 1 ]
    [[ $output =~ "There is no cherry-pick merge to continue" ]] || false
}

@test "cherry-pick: --continue with unresolved conflicts" {
    # Create a branch with a conflicting change
    dolt branch continue_test2
    dolt --branch continue_test2 sql -q "INSERT INTO test VALUES (100, 'branch1')"
    dolt --branch continue_test2 add .
    dolt --branch continue_test2 commit -am "Add row from branch1"
    COMMIT1=$(get_head_commit continue_test2)
    
    # Create a conflicting change on main
    dolt sql -q "INSERT INTO test VALUES (100, 'main')"
    dolt add .
    dolt commit -am "Add row from main"
    
    # Cherry-pick should create a conflict
    run dolt cherry-pick $COMMIT1
    [ $status -eq 1 ]
    [[ $output =~ "Unable to apply commit cleanly due to conflicts" ]] || false
    
    # Try to continue without resolving conflicts
    run dolt cherry-pick --continue
    [ $status -eq 1 ]
    [[ $output =~ "Unable to apply commit cleanly due to conflicts" ]] || false
    
}
