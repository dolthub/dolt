#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1

    dolt sql <<SQL
CREATE TABLE parent (
    id int PRIMARY KEY,
    v1 int,
    v2 int,
    INDEX v1 (v1),
    INDEX v2 (v2)
);
CREATE TABLE child (
    id int primary key,
    v1 int,
    v2 int
);
SQL
}

teardown() {
    teardown_common
}

@test "default-values: Standard default literal" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 2)"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: Default expression with function and referenced column" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 SMALLINT DEFAULT (GREATEST(pk, 2)))"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2), (3)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "default-values: Default expression converting to proper column type" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 VARCHAR(20) DEFAULT (GREATEST(pk, 2)))"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2), (3)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "default-values: Default literal of different type but implicitly converts" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,4" ]] || false
    [[ "$output" =~ "2,4" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: Back reference to default literal" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2), v2 BIGINT DEFAULT 7)"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "1,7,7" ]] || false
    [[ "$output" =~ "2,7,7" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: Forward reference to default literal" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 9, v2 BIGINT DEFAULT (v1))"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "1,9,9" ]] || false
    [[ "$output" =~ "2,9,9" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: Forward reference to default expression" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (8), v2 BIGINT DEFAULT (v1))"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "1,8,8" ]] || false
    [[ "$output" =~ "2,8,8" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: Back reference to value" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2 + 1), v2 BIGINT)"
    dolt sql -q "INSERT INTO test (pk, v2) VALUES (1, 4), (2, 6)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "1,5,4" ]] || false
    [[ "$output" =~ "2,7,6" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: TEXT expression" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 LONGTEXT DEFAULT (77))"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,77" ]] || false
    [[ "$output" =~ "2,77" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: REPLACE INTO with default expression" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 SMALLINT DEFAULT (GREATEST(pk, 2)))"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    dolt sql -q "REPLACE INTO test (pk) VALUES (2), (3)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "default-values: Add column last default literal" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    dolt sql -q "ALTER TABLE test ADD COLUMN v2 BIGINT DEFAULT 5"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "1,4,5" ]] || false
    [[ "$output" =~ "2,4,5" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: Add column implicit last default expression" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    dolt sql -q "ALTER TABLE test ADD COLUMN v2 BIGINT DEFAULT (v1 + 2)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "1,2,4" ]] || false
    [[ "$output" =~ "2,3,5" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: Add column explicit last default expression" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    dolt sql -q "ALTER TABLE test ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) AFTER v1"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "1,2,4" ]] || false
    [[ "$output" =~ "2,3,5" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: Add column first default literal" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT '4')"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    dolt sql -q "ALTER TABLE test ADD COLUMN v2 BIGINT DEFAULT 5 FIRST"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,pk,v1" ]] || false
    [[ "$output" =~ "5,1,4" ]] || false
    [[ "$output" =~ "5,2,4" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: Add column first default expression" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT)"
    dolt sql -q "INSERT INTO test VALUES (1, 3), (2, 4)"
    dolt sql -q "ALTER TABLE test ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) FIRST"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,pk,v1" ]] || false
    [[ "$output" =~ "5,1,3" ]] || false
    [[ "$output" =~ "6,2,4" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: Add column forward reference to default expression" {
    dolt sql -q "CREATE TABLE test(pk BIGINT DEFAULT (v1) PRIMARY KEY, v1 BIGINT)"
    dolt sql -q "INSERT INTO test (v1) VALUES (1), (2)"
    dolt sql -q "ALTER TABLE test ADD COLUMN v2 BIGINT DEFAULT (pk + 1) AFTER pk"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v2,v1" ]] || false
    [[ "$output" =~ "1,2,1" ]] || false
    [[ "$output" =~ "2,3,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: Add column back reference to default literal" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 5)"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    dolt sql -q "ALTER TABLE test ADD COLUMN v2 BIGINT DEFAULT (v1 - 1) AFTER pk"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v2,v1" ]] || false
    [[ "$output" =~ "1,4,5" ]] || false
    [[ "$output" =~ "2,4,5" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: Add column first with existing defaults still functioning" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 10))"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    dolt sql -q "ALTER TABLE test ADD COLUMN v2 BIGINT DEFAULT (-pk) FIRST"
    dolt sql -q "INSERT INTO test (pk) VALUES (3)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v2,pk,v1" ]] || false
    [[ "$output" =~ "-1,1,11" ]] || false
    [[ "$output" =~ "-2,2,12" ]] || false
    [[ "$output" =~ "-3,3,13" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "default-values: Drop column referencing other column" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2), v2 BIGINT)"
    dolt sql -q "ALTER TABLE test DROP COLUMN v1"
}

@test "default-values: Modify column move first forward reference default literal" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 2), v2 BIGINT DEFAULT (pk + 1))"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    dolt sql -q "ALTER TABLE test MODIFY COLUMN v1 BIGINT DEFAULT (pk + 2) FIRST"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk,v2" ]] || false
    [[ "$output" =~ "3,1,2" ]] || false
    [[ "$output" =~ "4,2,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "default-values: Modify column move first add reference" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))"
    dolt sql -q "INSERT INTO test (pk, v1) VALUES (1, 2), (2, 3)"
    dolt sql -q "ALTER TABLE test MODIFY COLUMN v1 BIGINT DEFAULT (pk + 5) FIRST"
    dolt sql -q "INSERT INTO test (pk) VALUES (3)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1,pk,v2" ]] || false
    [[ "$output" =~ "2,1,3" ]] || false
    [[ "$output" =~ "3,2,4" ]] || false
    [[ "$output" =~ "8,3,9" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "default-values: Modify column move last being referenced" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))"
    dolt sql -q "INSERT INTO test (pk, v1) VALUES (1, 2), (2, 3)"
    dolt sql -q "ALTER TABLE test MODIFY COLUMN v1 BIGINT AFTER v2"
    dolt sql -q "INSERT INTO test (pk, v1) VALUES (3, 4)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v2,v1" ]] || false
    [[ "$output" =~ "1,3,2" ]] || false
    [[ "$output" =~ "2,4,3" ]] || false
    [[ "$output" =~ "3,5,4" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "default-values: Modify column move last add reference" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (pk * 2))"
    dolt sql -q "INSERT INTO test (pk, v1) VALUES (1, 2), (2, 3)"
    dolt sql -q "ALTER TABLE test MODIFY COLUMN v1 BIGINT DEFAULT (-pk) AFTER v2"
    dolt sql -q "INSERT INTO test (pk) VALUES (3)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v2,v1" ]] || false
    [[ "$output" =~ "1,2,2" ]] || false
    [[ "$output" =~ "2,4,3" ]] || false
    [[ "$output" =~ "3,6,-3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "default-values: Modify column no move add reference" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (pk * 2))"
    dolt sql -q "INSERT INTO test (pk, v1) VALUES (1, 2), (2, 3)"
    dolt sql -q "ALTER TABLE test MODIFY COLUMN v1 BIGINT DEFAULT (-pk)"
    dolt sql -q "INSERT INTO test (pk) VALUES (3)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1,v2" ]] || false
    [[ "$output" =~ "1,2,2" ]] || false
    [[ "$output" =~ "2,3,4" ]] || false
    [[ "$output" =~ "3,-3,6" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "default-values: Table referenced with column" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (test.pk))"
    dolt sql -q "INSERT INTO test (pk) VALUES (1), (2)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v1\` bigint DEFAULT (pk)" ]] || false
}

@test "default-values: Column referenced with name change" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1 + 1))"
    dolt sql -q "INSERT INTO test (pk, v1) VALUES (1, 2)"
    dolt sql -q "ALTER TABLE test RENAME COLUMN v1 to v1x"
    dolt sql -q "INSERT INTO test (pk, v1x) VALUES (2, 3)"
    dolt sql -q "ALTER TABLE test CHANGE COLUMN v1x v1y BIGINT"
    dolt sql -q "INSERT INTO test (pk, v1y) VALUES (3, 4)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1y,v2" ]] || false
    [[ "$output" =~ "1,2,3" ]] || false
    [[ "$output" =~ "2,3,4" ]] || false
    [[ "$output" =~ "3,4,5" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    run dolt schema show
    [ "$status" -eq "0" ]
    [[ "$output" =~ "\`v2\` bigint DEFAULT ((v1y + 1))" ]] || false
}

@test "default-values: Invalid literal for column type" {
    run dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 INT UNSIGNED DEFAULT -1)"
    [ "$status" -eq "1" ]
}

@test "default-values: Invalid literal for column type 2" {
    run dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 'hi')"
    [ "$status" -eq "1" ]
}

@test "default-values: Expression contains invalid literal once implicitly converted" {
    run dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 INT UNSIGNED DEFAULT '-1')"
    [ "$status" -eq "1" ]
}

@test "default-values: Null literal is invalid for NOT NULL" {
    run dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT NOT NULL DEFAULT NULL)"
    [ "$status" -eq "1" ]
}

@test "default-values: Back reference to expression" {
    run dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2), v2 BIGINT DEFAULT (9))"
    [ "$status" -eq "1" ]
}

@test "default-values: TEXT literals" {
    run dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 TEXT DEFAULT 'hi')"
    [ "$status" -eq "1" ]
    run dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 LONGTEXT DEFAULT 'hi')"
    [ "$status" -eq "1" ]
}

@test "default-values: Other types using NOW/CURRENT_TIMESTAMP literal" {
    run dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT NOW())"
    [ "$status" -eq "1" ]
    run dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 VARCHAR(20) DEFAULT CURRENT_TIMESTAMP())"
    [ "$status" -eq "1" ]
    run dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIT(5) DEFAULT NOW())"
    [ "$status" -eq "1" ]
    run dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 DATE DEFAULT CURRENT_TIMESTAMP())"
    [ "$status" -eq "1" ]
}

@test "default-values: Custom functions are invalid" {
    run dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (CUSTOMFUNC(1)))"
    [ "$status" -eq "1" ]
}

@test "default-values: Default expression references own column" {
    run dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v1))"
    [ "$status" -eq "1" ]
}

@test "default-values: Expression contains invalid literal, fails on insertion" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 INT UNSIGNED DEFAULT (-1))"
    run dolt sql -q "INSERT INTO test (pk) VALUES (1)"
    [ "$status" -eq "1" ]
}

@test "default-values: Expression contains null on NOT NULL, fails on insertion" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT NOT NULL DEFAULT (NULL))"
    run dolt sql -q "INSERT INTO test (pk) VALUES (1)"
    [ "$status" -eq "1" ]
}

@test "default-values: Add column first back reference to expression" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))"
    run dolt sql -q "ALTER TABLE test ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) FIRST"
    [ "$status" -eq "1" ]
}

@test "default-values: Add column after back reference to expression" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))"
    run dolt sql -q "ALTER TABLE test ADD COLUMN v2 BIGINT DEFAULT (v1 + 2) AFTER pk"
    [ "$status" -eq "1" ]
}

@test "default-values: Add column self reference" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk + 1))"
    run dolt sql -q "ALTER TABLE test ADD COLUMN v2 BIGINT DEFAULT (v2)"
    [ "$status" -eq "1" ]
}

@test "default-values: Drop column referenced by other column" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT DEFAULT (v1))"
    run dolt sql -q "ALTER TABLE test DROP COLUMN v1"
    [ "$status" -eq "1" ]
}

@test "default-values: Modify column moving back creates back reference to expression" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT DEFAULT (v1))"
    run dolt sql -q "ALTER TABLE test MODIFY COLUMN v1 BIGINT DEFAULT (pk) AFTER v2"
    [ "$status" -eq "1" ]
}

@test "default-values: Modify column moving forward creates back reference to expression" {
    dolt sql -q "CREATE TABLE test(pk BIGINT DEFAULT (v2) PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT)"
    run dolt sql -q "ALTER TABLE test MODIFY COLUMN v1 BIGINT DEFAULT (pk) FIRST"
    [ "$status" -eq "1" ]
}

@test "default-values: Modify column invalid after" {
    dolt sql -q "CREATE TABLE test(pk BIGINT DEFAULT (v2) PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT)"
    run dolt sql -q "ALTER TABLE test MODIFY COLUMN v1 BIGINT DEFAULT (pk) AFTER v3"
    [ "$status" -eq "1" ]
}

@test "default-values: Add column invalid after" {
    dolt sql -q "CREATE TABLE test(pk BIGINT DEFAULT (v2) PRIMARY KEY, v1 BIGINT DEFAULT (pk), v2 BIGINT)"
    run dolt sql -q "ALTER TABLE test ADD COLUMN v1 BIGINT DEFAULT (pk) AFTER v3"
    [ "$status" -eq "1" ]
}

@test "default-values: Import with default values" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT 2 NOT NULL, v2 int)"
    dolt sql -q "INSERT INTO test (pk, v2) VALUES (1, 3), (2, 4)"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,2,3" ]] || false
    [[ "$output" =~ "2,2,4" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    echo -e 'pk,v2\n3,5\n4,6'| dolt table import -u test

    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,2,3" ]] || false
    [[ "$output" =~ "2,2,4" ]] || false
    [[ "$output" =~ "3,2,5" ]] || false
    [[ "$output" =~ "4,2,6" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false

    # validate that nulls are not accepted for a not null import
    cat <<DELIM > bad-update.csv
pk,v1,v2
5,,5
6,,6
DELIM
    run dolt table import -u test bad-update.csv
    [ "$status" -eq "1" ]
    [[ "$output" =~ "Bad Row: [5,<nil>,5]" ]] || false
    [[ "$output" =~ "column name 'v1' is non-nullable but attempted to set a value of null" ]] || false
}

@test "default-values: Defining literal default value to NULL or EMPTY value in ALTER TABLE" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, c1 BIGINT, c2 BIGINT, c3 INT)"
    run dolt sql -q "SELECT column_name, is_nullable, column_default FROM information_schema.columns WHERE table_name = 'test'" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "column_name,is_nullable,column_default" ]] || false
    [[ "$output" =~ "pk,NO," ]] || false
    [[ "$output" =~ "c1,YES," ]] || false
    [[ "$output" =~ "c2,YES," ]] || false
    [[ "$output" =~ "c3,YES," ]] || false

    dolt sql -q "ALTER TABLE test CHANGE c1 c1 INT NULL DEFAULT NULL"
    dolt sql -q "ALTER TABLE test CHANGE c2 c2 varchar(4) NOT NULL DEFAULT ''"
    dolt sql -q "ALTER TABLE test CHANGE c3 c3 varchar(4) NOT NULL DEFAULT 'ln'"
    run dolt sql -q "SELECT column_name, is_nullable, column_default FROM information_schema.columns WHERE table_name = 'test'" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "column_name,is_nullable,column_default" ]] || false
    [[ "$output" =~ "pk,NO," ]] || false
    [[ "$output" =~ "c1,YES," ]] || false
    [[ "$output" =~ "c2,NO," ]] || false
    [[ "$output" =~ "c3,NO,ln" ]] || false

    dolt sql -q "ALTER TABLE test CHANGE c1 c1 FLOAT NOT NULL DEFAULT '4.44'"
    dolt sql -q "ALTER TABLE test CHANGE c2 c2 DOUBLE NOT NULL DEFAULT '3.333'"
    dolt sql -q "ALTER TABLE test CHANGE c3 c3 BOOLEAN NULL DEFAULT FALSE"
    run dolt sql -q "SELECT column_name, is_nullable, column_default FROM information_schema.columns WHERE table_name = 'test'" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "column_name,is_nullable,column_default" ]] || false
    [[ "$output" =~ "pk,NO," ]] || false
    [[ "$output" =~ "c1,NO,4.44" ]] || false
    [[ "$output" =~ "c2,NO,3.333" ]] || false
    [[ "$output" =~ "c3,YES,false" ]] || false

    dolt sql -q "ALTER TABLE test CHANGE c1 c1 DATETIME NOT NULL DEFAULT '2020-04-01 16:16:16'"
    dolt sql -q "ALTER TABLE test CHANGE c2 c2 TIMESTAMP NULL DEFAULT '2008-04-22 16:16:16'"
    run dolt sql -q "SELECT column_name, is_nullable, column_default FROM information_schema.columns WHERE table_name = 'test'" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "column_name,is_nullable,column_default" ]] || false
    [[ "$output" =~ "pk,NO," ]] || false
    [[ "$output" =~ "c1,NO,2020-04-01 16:16:16" ]] || false
    [[ "$output" =~ "c2,YES,2008-04-22 16:16:16" ]] || false
    [[ "$output" =~ "c3,YES,false" ]] || false
}

@test "default-values: Function default values are correctly represented in the information_schema.columns table" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 SMALLINT DEFAULT (GREATEST(pk, 2)))"
    run dolt sql -q "SELECT column_name, is_nullable, column_default FROM information_schema.columns WHERE table_name = 'test'" -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "column_name,is_nullable,column_default" ]] || false
    [[ "$output" =~ "pk,NO," ]] || false
    [[ "$output" =~ 'v1,YES,"GREATEST(pk, 2)"' ]] || false
}

@test "default-values: Additional test with function defaults" {
    dolt sql -q "CREATE TABLE test_table (pk int primary key, fname varchar(20), lname varchar(20), height int)"
    dolt sql -q "ALTER TABLE test_table CHANGE fname col2 float NOT NULL DEFAULT length('hello')"
    dolt sql -q "ALTER TABLE test_table CHANGE lname col3 double NOT NULL DEFAULT ROUND(-1.58)"
    dolt sql -q "ALTER TABLE test_table CHANGE height col4 float NULL DEFAULT RAND()"

    run dolt sql -q "SELECT column_name, column_default FROM information_schema.columns WHERE table_name = 'test_table'" -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "column_name,column_default" ]] || false
    [[ "$output" =~ "pk," ]] || false
    [[ "$output" =~ 'col2,"LENGTH(""hello"")"' ]] || false
    [[ "$output" =~ 'col3,"ROUND(-1.58, 0)"' ]] || false
    [[ "$output" =~ 'col4,RAND()' ]] || false
}

@test "default-values: Outputting the string version of a more complex default value works" {
    dolt sql -q "CREATE TABLE test_table (pk int primary key, fname varchar(20), lname varchar(20))"
		dolt sql -q "ALTER TABLE test_table CHANGE fname col2 INT NULL DEFAULT (RAND()+RAND());"
		dolt sql -q "ALTER TABLE test_table CHANGE lname col3 BOOLEAN NULL DEFAULT (CASE pk WHEN 1 THEN false ELSE true END);"

    run dolt sql -q "SELECT column_name, column_default FROM information_schema.columns where table_name='test_table'" -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "column_name,column_default" ]] || false
    [[ "$output" =~ "pk," ]] || false
    [[ "$output" =~ "col2,(RAND() + RAND())" ]] || false
    [[ "$output" =~ "col3,CASE pk WHEN 1 THEN false ELSE true END" ]] || false
}
