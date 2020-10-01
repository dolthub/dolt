#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

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

@test "triggers: Mix of CREATE TRIGGER and CREATE VIEW in batch mode" {
    # There were issues with batch mode that this is verifying no longer occurs
    dolt sql <<SQL
CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT);
CREATE TRIGGER trigger1 BEFORE INSERT ON test FOR EACH ROW SET new.v1 = -new.v1;
CREATE VIEW view1 AS SELECT v1 FROM test;
CREATE TABLE a (x INT PRIMARY KEY);
CREATE TABLE b (y INT PRIMARY KEY);
INSERT INTO test VALUES (1, 1);
CREATE VIEW view2 AS SELECT y FROM b;
CREATE TRIGGER trigger2 AFTER INSERT ON a FOR EACH ROW INSERT INTO b VALUES (new.x * 2);
INSERT INTO a VALUES (2);
SQL
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,-1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM a" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "x" ]] || false
    [[ "$output" =~ "2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM b" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "y" ]] || false
    [[ "$output" =~ "4" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM view1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "v1" ]] || false
    [[ "$output" =~ "-1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM view2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "y" ]] || false
    [[ "$output" =~ "4" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_schemas ORDER BY 1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "id,type,name,fragment" ]] || false
    [[ "$output" =~ "1,trigger,trigger1,CREATE TRIGGER trigger1 BEFORE INSERT ON test FOR EACH ROW SET new.v1 = -new.v1" ]] || false
    [[ "$output" =~ "2,view,view1,SELECT v1 FROM test" ]] || false
    [[ "$output" =~ "3,view,view2,SELECT y FROM b" ]] || false
    [[ "$output" =~ "4,trigger,trigger2,CREATE TRIGGER trigger2 AFTER INSERT ON a FOR EACH ROW INSERT INTO b VALUES (new.x * 2)" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
}

@test "triggers: Order preservation" {
    skip "DROP TRIGGER not yet implemented and broken otherwise"
    dolt sql <<SQL
CREATE TABLE x (a BIGINT PRIMARY KEY);
CREATE TRIGGER trigger1 BEFORE INSERT ON x FOR EACH ROW SET new.a = new.a + 1;
CREATE TRIGGER trigger2 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 10;
CREATE TRIGGER trigger3 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 100;
CREATE TRIGGER trigger4 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 1000;
DROP TRIGGER trigger1;
CREATE TRIGGER trigger5 BEFORE INSERT ON x FOR EACH ROW PRECEDES trigger3 SET new.a = (new.a * 2) + 10000;
DROP TRIGGER trigger3;
INSERT INTO x VALUES (0);
SQL
    run dolt sql -q "SELECT * FROM x" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "a" ]] || false
    [[ "$output" =~ "21040" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "triggers: Writing directly into dolt_schemas" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT);"
    dolt sql -q "CREATE VIEW view1 AS SELECT v1 FROM test;"
    dolt sql -q "INSERT INTO dolt_schemas VALUES (2, 'trigger', 'trigger1', 'CREATE TRIGGER trigger1 BEFORE INSERT ON test FOR EACH ROW SET new.v1 = -new.v1;');"
    dolt sql -q "INSERT INTO test VALUES (1, 1);"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,-1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}
