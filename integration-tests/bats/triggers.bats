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
    assert_feature_version
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
    run dolt sql -q "SELECT * FROM dolt_schemas ORDER BY name" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "type,name,fragment" ]] || false
    [[ "$output" =~ "trigger,trigger1,CREATE TRIGGER trigger1 BEFORE INSERT ON test FOR EACH ROW SET new.v1 = -new.v1" ]] || false
    [[ "$output" =~ "view,view1,CREATE VIEW view1 AS SELECT v1 FROM test" ]] || false
    [[ "$output" =~ "view,view2,CREATE VIEW view2 AS SELECT y FROM b" ]] || false
    [[ "$output" =~ "trigger,trigger2,CREATE TRIGGER trigger2 AFTER INSERT ON a FOR EACH ROW INSERT INTO b VALUES (new.x * 2)" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
}

@test "triggers: Order preservation" {
    skip "DROP TRIGGER with dependencies not implemented"
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

@test "triggers: import with triggers" {
       dolt sql <<SQL
CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT);
CREATE TRIGGER trigger1 BEFORE INSERT ON test FOR EACH ROW SET new.v1 = new.v1 + 1;
INSERT INTO test VALUES (1, 1);
SQL
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    echo -e 'pk,v1\n2,2\n3,3'|dolt table import -u test

    run dolt sql -q "SELECT * FROM test" -r=csv
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "2,3" ]] || false
    [[ "$output" =~ "3,4" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false

    # check information_schema.TRIGGERS table
    run dolt sql -q "select trigger_name, event_manipulation, event_object_table, action_order, action_condition, action_statement, action_timing, definer, character_set_client, collation_connection, database_collation from information_schema.TRIGGERS;" -r csv
    [[ "$output" =~ "trigger1,INSERT,test,1,,SET new.v1 = new.v1 + 1,BEFORE,root@localhost,utf8mb4,utf8mb4_0900_bin,utf8mb4_0900_bin" ]] || false
}

@test "triggers: Writing directly into dolt_schemas" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT);"
    dolt sql -q "CREATE VIEW view1 AS SELECT v1 FROM test;"
    dolt sql -q "INSERT INTO dolt_schemas VALUES ('trigger', 'trigger1', 'CREATE TRIGGER trigger1 BEFORE INSERT ON test FOR EACH ROW SET new.v1 = -new.v1;', json_object('CreatedAt', 1), NULL);"
    dolt sql -q "INSERT INTO test VALUES (1, 1);"
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,-1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "triggers: Merge triggers on different branches, no conflict" {
    dolt sql <<SQL
CREATE TABLE x(a BIGINT PRIMARY KEY);
CREATE TRIGGER trigger1 BEFORE INSERT ON x FOR EACH ROW SET new.a = new.a + 1;
SQL
    dolt add -A
    dolt commit -m "Initial Commit"
    dolt checkout -b other
    dolt checkout main
    dolt sql -q "CREATE TRIGGER trigger2 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 10;"
    dolt add -A
    dolt commit -m "On main"
    dolt checkout other
    dolt sql <<SQL
CREATE TRIGGER trigger3 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 100;
CREATE TRIGGER trigger4 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 1000;
SQL
    dolt add -A
    dolt commit -m "On other"
    dolt checkout main
    run dolt diff other
    [ "$status" -eq "0" ]
    [[ "$output" =~ "CREATE TRIGGER trigger2 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 10" ]] || false
    [[ "$output" =~ "CREATE TRIGGER trigger3 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 100" ]] || false

    dolt merge other --no-commit
    dolt add dolt_schemas
    dolt commit -m "Merged other table"

    run dolt sql -q "SELECT * FROM dolt_schemas" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "type,name,fragment" ]] || false
    [[ "$output" =~ "trigger,trigger1,CREATE TRIGGER trigger1 BEFORE INSERT ON x FOR EACH ROW SET new.a = new.a + 1" ]] || false
    [[ "$output" =~ "trigger,trigger2,CREATE TRIGGER trigger2 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 10" ]] || false
    [[ "$output" =~ "trigger,trigger3,CREATE TRIGGER trigger3 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 100" ]] || false
    [[ "$output" =~ "trigger,trigger4,CREATE TRIGGER trigger4 BEFORE INSERT ON x FOR EACH ROW SET new.a = (new.a * 2) + 1000" ]] || false
    [[ "${#lines[@]}" = "5" ]] || false
}