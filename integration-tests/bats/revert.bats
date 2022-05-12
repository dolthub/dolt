#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT)"
    dolt add -A
    dolt commit -m "Created table"
    dolt sql -q "INSERT INTO test VALUES (1, 1)"
    dolt add -A
    dolt commit -m "Inserted 1"
    dolt sql -q "INSERT INTO test VALUES (2, 2)"
    dolt add -A
    dolt commit -m "Inserted 2"
    dolt sql -q "INSERT INTO test VALUES (3, 3)"
    dolt add -A
    dolt commit -m "Inserted 3"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "revert: HEAD" {
    dolt revert HEAD
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "revert: HEAD~1" {
    dolt revert HEAD~1
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "revert: HEAD & HEAD~1" {
    dolt revert HEAD HEAD~1
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "revert: has changes in the working set" {
    dolt sql -q "INSERT INTO test VALUES (4, 4)"
    run dolt revert HEAD
    [ "$status" -eq "1" ]
    [[ "$output" =~ "changes" ]] || false
}

@test "revert: conflicts" {
    dolt sql -q "INSERT INTO test VALUES (4, 4)"
    dolt add -A
    dolt commit -m "Inserted 4"
    dolt sql -q "REPLACE INTO test VALUES (4, 5)"
    dolt add -A
    dolt commit -m "Updated 4"
    run dolt revert HEAD~1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "conflict" ]] || false
}

@test "revert: constraint violations" {
    skip_nbf_dolt_1
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt sql -q "DELETE FROM child WHERE pk = 2"
    dolt add -A
    dolt commit -m "MC2"
    dolt sql -q "DELETE FROM parent WHERE pk = 20"
    dolt add -A
    dolt commit -m "MC3"
    run dolt revert HEAD~1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "constraint violation" ]] || false
}

@test "revert: too far back" {
    run dolt revert HEAD~10
    [ "$status" -eq "1" ]
    [[ "$output" =~ "ancestor" ]] || false
}

@test "revert: no changes" {
    run dolt revert HEAD~4
    [ "$status" -eq "0" ]
    [[ "$output" =~ "No changes were made" ]] || false
}

@test "revert: invalid hash" {
    run dolt revert aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    [ "$status" -eq "1" ]
    [[ "$output" =~ "target commit not found" ]] || false
}

@test "revert: HEAD with --author parameter" {
    dolt revert HEAD --author "john <johndoe@gmail.com>"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    run dolt log -n 1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Author: john <johndoe@gmail.com>" ]] || false
}

@test "revert: HEAD & HEAD~1 with --author parameter" {
    dolt revert HEAD HEAD~1 --author "john <johndoe@gmail.com>"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    run dolt log -n 1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "Author: john <johndoe@gmail.com>" ]] || false
}

@test "revert: SQL HEAD" {
    dolt sql -q "SELECT DOLT_REVERT('HEAD')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "revert: SQL HEAD dfunc" {
    dolt sql -q "CALL drevert('HEAD')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "revert: Stored Procedure HEAD" {
    dolt sql -q "CALL DOLT_REVERT('HEAD')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "revert: SQL HEAD~1" {
    dolt sql -q "SELECT DOLT_REVERT('HEAD~1')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "revert: Stored Procedure HEAD~1" {
    dolt sql -q "CALL DOLT_REVERT('HEAD~1')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "revert: SQL HEAD & HEAD~1" {
    dolt sql -q "SELECT DOLT_REVERT('HEAD', 'HEAD~1')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "revert: Stored Procedure HEAD & HEAD~1" {
    dolt sql -q "CALL DOLT_REVERT('HEAD', 'HEAD~1')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "revert: SQL has changes in the working set" {
    dolt sql -q "INSERT INTO test VALUES (4, 4)"
    run dolt sql -q "SELECT DOLT_REVERT('HEAD')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "changes" ]] || false
}

@test "revert: Stored Procedure has changes in the working set" {
    dolt sql -q "INSERT INTO test VALUES (4, 4)"
    run dolt sql -q "CALL DOLT_REVERT('HEAD')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "changes" ]] || false
}

@test "revert: SQL conflicts" {
    dolt sql -q "INSERT INTO test VALUES (4, 4)"
    dolt add -A
    dolt commit -m "Inserted 4"
    dolt sql -q "REPLACE INTO test VALUES (4, 5)"
    dolt add -A
    dolt commit -m "Updated 4"
    run dolt sql -q "SELECT DOLT_REVERT('HEAD~1')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "conflict" ]] || false
}

@test "revert: Stored Procedure conflicts" {
    dolt sql -q "INSERT INTO test VALUES (4, 4)"
    dolt add -A
    dolt commit -m "Inserted 4"
    dolt sql -q "REPLACE INTO test VALUES (4, 5)"
    dolt add -A
    dolt commit -m "Updated 4"
    run dolt sql -q "CALL DOLT_REVERT('HEAD~1')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "conflict" ]] || false
}

@test "revert: SQL constraint violations" {
    skip_nbf_dolt_1
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt sql -q "DELETE FROM child WHERE pk = 2"
    dolt add -A
    dolt commit -m "MC2"
    dolt sql -q "DELETE FROM parent WHERE pk = 20"
    dolt add -A
    dolt commit -m "MC3"
    run dolt sql -q "SELECT DOLT_REVERT('HEAD~1')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "constraint violation" ]] || false
}

@test "revert: Stored Procedure constraint violations" {
    skip_nbf_dolt_1
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt sql -q "DELETE FROM child WHERE pk = 2"
    dolt add -A
    dolt commit -m "MC2"
    dolt sql -q "DELETE FROM parent WHERE pk = 20"
    dolt add -A
    dolt commit -m "MC3"
    run dolt sql -q "CALL DOLT_REVERT('HEAD~1')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "constraint violation" ]] || false
}

@test "revert: SQL too far back" {
    run dolt sql -q "SELECT DOLT_REVERT('HEAD~10')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "ancestor" ]] || false
}

@test "revert: Stored Procedure too far back" {
    run dolt sql -q "CALL DOLT_REVERT('HEAD~10')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "ancestor" ]] || false
}

@test "revert: SQL no changes" {
    dolt sql -q "SELECT DOLT_REVERT('HEAD~4')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "revert: Stored Procedure no changes" {
    dolt sql -q "CALL DOLT_REVERT('HEAD~4')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "revert: SQL invalid hash" {
    run dolt sql -q "SELECT DOLT_REVERT('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "target commit not found" ]] || false
}

@test "revert: Stored Procedure invalid hash" {
    run dolt sql -q "CALL DOLT_REVERT('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "target commit not found" ]] || false
}

@test "revert: SQL HEAD with author" {
    dolt sql -q "SELECT DOLT_REVERT('HEAD', '--author', 'john doe <johndoe@gmail.com>')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    run dolt log -n 1
    [[ "$output" =~ "Author: john doe <johndoe@gmail.com>" ]] || false
}

@test "revert: Stored Procedure HEAD with author" {
    dolt sql -q "CALL DOLT_REVERT('HEAD', '--author', 'john doe <johndoe@gmail.com>')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    run dolt log -n 1
    [[ "$output" =~ "Author: john doe <johndoe@gmail.com>" ]] || false
}

@test "revert: SQL HEAD & HEAD~1 with author" {
    dolt sql -q "SELECT DOLT_REVERT('HEAD', 'HEAD~1', '--author', 'john doe <johndoe@gmail.com>')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    run dolt log -n 1
    [[ "$output" =~ "Author: john doe <johndoe@gmail.com>" ]] || false
}

@test "revert: Stored Procedure HEAD & HEAD~1 with author" {
    dolt sql -q "CALL DOLT_REVERT('HEAD', 'HEAD~1', '--author', 'john doe <johndoe@gmail.com>')"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    run dolt log -n 1
    [[ "$output" =~ "Author: john doe <johndoe@gmail.com>" ]] || false
}
