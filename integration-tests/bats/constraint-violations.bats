#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "constraint-violations: functions blocked with violations" {
    dolt sql <<"SQL"
CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, UNIQUE INDEX(v1));
INSERT INTO test VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO test VALUES (3, 3)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (4, 3), (9, 9)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main

    run dolt merge other
    [ "$status" -eq "0" ]
    [[ "$output" =~ "fix constraint violations" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "test,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt status
    [ "$status" -eq "0" ]
    [[ "$output" =~ "fix constraint violations" ]] || false
    [[ "$output" =~ "test" ]] || false
    run dolt merge other
    [ "$status" -eq "1" ]
    [[ "$output" =~ "constraint violation" ]] || false

    # we can stage conflicts, but not commit them
    dolt add test
    run dolt commit -m "this should fail"
    [ "$status" -eq "1" ]
    [[ "$output" =~ "constraint violation" ]] || false

    dolt sql -q "DELETE FROM dolt_constraint_violations_test"
    run dolt status
    [ "$status" -eq "0" ]
    [[ "$output" =~ "constraint violations fixed" ]] || false
    dolt add test
    dolt commit -m "this works"
    run dolt merge other
    [ "$status" -eq "0" ]
    [[ "$output" =~ "up to date" ]] || false
}

@test "constraint-violations: dolt_force_transaction_commit along with dolt_allow_commit_conflicts ignores constraint violations" {
    dolt sql <<"SQL"
CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, UNIQUE INDEX(v1));
INSERT INTO test VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO test VALUES (3, 3)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (4, 3), (9, 9)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main

    run dolt sql <<"SQL"
SET dolt_allow_commit_conflicts = 0;
SELECT DOLT_MERGE('other');
SQL
    [ "$status" -eq "1" ]
    [[ "$output" =~ "constraint violations" ]] || false
    run dolt sql <<"SQL"
SET dolt_force_transaction_commit = 1;
SELECT DOLT_MERGE('other');
SELECT DOLT_COMMIT("-am", "msg", "--force");
SQL
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "constraint violations" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent remove, other child add, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main child add, other parent remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent add and remove, other child add and remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM parent WHERE pk = 20;
INSERT INTO parent VALUES (30, 3);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
DELETE FROM CHILD WHERE pk = 1;
INSERT INTO child VALUES (2,2), (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent illegal remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor contains fk, other child illegal add, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM child WHERE pk = 1;
DELETE FROM parent WHERE pk = 10;
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO child VALUES (3, 3);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,3,3,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, other parent illegal remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent remove, other child add, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main child add, other parent remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent add and remove, other child add and remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM parent WHERE pk = 20;
INSERT INTO parent VALUES (30, 3);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
DELETE FROM CHILD WHERE pk = 1;
INSERT INTO child VALUES (2,2), (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent illegal remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor contains fk, other child illegal add, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM child WHERE pk = 1;
DELETE FROM parent WHERE pk = 10;
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO child VALUES (3, 3);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,3,3,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, other parent illegal remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent remove, other child add, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main child add, other parent remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent add and remove, other child add and remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM parent WHERE pk = 20;
INSERT INTO parent VALUES (30, 3);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
DELETE FROM CHILD WHERE pk = 1;
INSERT INTO child VALUES (2,2), (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent illegal remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor contains fk, other child illegal add, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM child WHERE pk = 1;
DELETE FROM parent WHERE pk = 10;
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO child VALUES (3, 3);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,3,3,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, other parent illegal remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor contains fk, main parent remove with backup, other child add, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2), (30, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor contains fk, main child add, other parent remove with backup, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1), (20, 2), (30, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent remove, other child add, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);
INSERT INTO child VALUES (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main child add, other parent remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);
DELETE FROM parent WHERE pk = 20;
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent add and remove, other child add and remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM parent WHERE pk = 20;
INSERT INTO parent VALUES (30, 3);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);
DELETE FROM CHILD WHERE pk = 1;
INSERT INTO child VALUES (2,2), (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent illegal remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "MC2"
    dolt checkout other
dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);
INSERT INTO child VALUES (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor missing fk, other child illegal add, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM child WHERE pk = 1;
DELETE FROM parent WHERE pk = 10;
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO child VALUES (3, 3);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,3,3,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, other parent illegal remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent remove, other child add, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE;
INSERT INTO child VALUES (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main child add, other parent remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE;
DELETE FROM parent WHERE pk = 20;
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent add and remove, other child add and remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM parent WHERE pk = 20;
INSERT INTO parent VALUES (30, 3);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE;
DELETE FROM CHILD WHERE pk = 1;
INSERT INTO child VALUES (2,2), (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent illegal remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE;
INSERT INTO child VALUES (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor missing fk, other child illegal add, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM child WHERE pk = 1;
DELETE FROM parent WHERE pk = 10;
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE;
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO child VALUES (3, 3);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,3,3,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, other parent illegal remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE;
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent remove, other child add, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL;
INSERT INTO child VALUES (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main child add, other parent remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL;
DELETE FROM parent WHERE pk = 20;
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent add and remove, other child add and remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM parent WHERE pk = 20;
INSERT INTO parent VALUES (30, 3);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL;
DELETE FROM CHILD WHERE pk = 1;
INSERT INTO child VALUES (2,2), (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "30,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, main parent illegal remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL;
INSERT INTO child VALUES (3, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor missing fk, other child illegal add, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
DELETE FROM child WHERE pk = 1;
DELETE FROM parent WHERE pk = 10;
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL;
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO child VALUES (3, 3);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,3,3,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing fk, other parent illegal remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO parent VALUES (10, 1), (20, 2);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (3, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL;
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM parent WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "$output" =~ 'foreign key,3,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,2" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: ancestor missing parent, main child add, restrict" {
    dolt sql <<"SQL"
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
INSERT INTO parent VALUES (10, 1);
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing parent, main child add, cascade" {
    dolt sql <<"SQL"
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
INSERT INTO parent VALUES (10, 1);
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE;
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing parent, main child add, set null" {
    dolt sql <<"SQL"
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO child VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO child VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
INSERT INTO parent VALUES (10, 1);
ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL;
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing child, main parent remove, restrict" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing child, main parent remove, cascade" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing child, main parent remove, set null" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
INSERT INTO parent VALUES (10, 1), (20, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 20;"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE SET NULL ON UPDATE SET NULL);
INSERT INTO child VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""SET NULL"", ""OnUpdate"": ""SET NULL"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: ancestor missing both, other illegal operations, restrict" {
    dolt sql <<"SQL"
CREATE TABLE unrelated (pk BIGINT PRIMARY KEY, v1 BIGINT);
INSERT INTO unrelated VALUES (1, 1);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO unrelated VALUES (2, 2)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent (v1));
INSERT INTO parent VALUES (10, 1);
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO child VALUES (1, 1), (2, 2);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: chained foreign keys, parent deleted on theirs, child added on ours" {
    # Only child1 has a constraint violation, as child2 properly references the merged in values of child1
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child1 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c1 FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c2 FOREIGN KEY (v1) REFERENCES child1 (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (100, 1), (200, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
INSERT INTO child1 VALUES (10, 1), (20, 2);
INSERT INTO child2 VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "DELETE FROM parent WHERE pk = 200"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,20,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_c1"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child1""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "100,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: chained foreign keys, parent deleted on ours, child added on theirs" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child1 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c1 FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c2 FOREIGN KEY (v1) REFERENCES child1 (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (100, 1), (200, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM parent WHERE pk = 200"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
INSERT INTO child1 VALUES (10, 1), (20, 2);
INSERT INTO child2 VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,20,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_c1"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child1""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "100,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM child1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: chained foreign keys, parent updated on theirs, child added on ours" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child1 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c1 FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c2 FOREIGN KEY (v1) REFERENCES child1 (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (100, 1), (200, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql <<"SQL"
INSERT INTO child1 VALUES (10, 1), (20, 2);
INSERT INTO child2 VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "UPDATE parent SET v1 = 3 WHERE pk = 200"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,20,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_c1"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child1""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "100,1" ]] || false
    [[ "$output" =~ "200,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: chained foreign keys, parent updated on ours, child added on theirs" {
    dolt sql <<"SQL"
CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE child1 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c1 FOREIGN KEY (v1) REFERENCES parent (v1) ON DELETE CASCADE ON UPDATE CASCADE);
CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_c2 FOREIGN KEY (v1) REFERENCES child1 (v1) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO parent VALUES (100, 1), (200, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "UPDATE parent SET v1 = 3 WHERE pk = 200"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql <<"SQL"
INSERT INTO child1 VALUES (10, 1), (20, 2);
INSERT INTO child2 VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "child1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,20,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_c1"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""parent"", ""Table"": ""child1""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_child2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM parent" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "100,1" ]] || false
    [[ "$output" =~ "200,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM child2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: cyclic foreign keys, illegal deletion" {
    # We're deleting a reference in a cycle from each table to make sure it properly applies a violation in both instances
    dolt sql <<"SQL"
CREATE TABLE t1 (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX(v1));
CREATE TABLE t2 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_t2 FOREIGN KEY (v1) REFERENCES t1 (v1));
ALTER TABLE t1 ADD CONSTRAINT fk_t1 FOREIGN KEY (v1) REFERENCES t2 (v1);
SET FOREIGN_KEY_CHECKS = 0;
INSERT INTO t1 VALUES (10, 1), (20, 2);
INSERT INTO t2 VALUES (1, 1), (2, 2);
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch main2
    dolt branch other
    dolt branch other2
    dolt checkout other
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM t1 WHERE pk = 20;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC1"
    dolt checkout other2
    dolt sql <<"SQL"
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM t2 WHERE pk = 2;
SET FOREIGN_KEY_CHECKS = 1;
SQL
    dolt add -A
    dolt commit --force -m "OC2"
    dolt checkout main
    dolt merge other

    # FF merge no longer checks constraints; forced commits require constraint reification
    run dolt constraints verify --all
    [ "$status" -eq "1" ]

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "t2,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_t1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_t2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,2,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_t2"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""t1"", ""Table"": ""t2""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM t1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM t2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    dolt reset --hard
    dolt checkout main2
    dolt merge other2

    # FF merge no longer checks constraints; forced commits require constraint reification
    run dolt constraints verify --all
    [ "$status" -eq "1" ]

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "t1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_t1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,20,2,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_t1"", ""Index"": ""v1"", ""OnDelete"": ""RESTRICT"", ""OnUpdate"": ""RESTRICT"", ""ReferencedColumns"": [""v1""], ""ReferencedIndex"": ""v1"", ""ReferencedTable"": ""t2"", ""Table"": ""t1""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_t2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
    run dolt sql -q "SELECT * FROM t1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "10,1" ]] || false
    [[ "$output" =~ "20,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM t2" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "constraint-violations: self-referential foreign keys, add on ours, delete on theirs" {
    dolt sql <<"SQL"
CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES test (pk) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO test VALUES (1, NULL), (2, 1), (3, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO test VALUES (4, 3)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "DELETE FROM test WHERE pk = 3"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "test,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,4,3,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""pk""], ""ReferencedIndex"": ""pk"", ""ReferencedTable"": ""test"", ""Table"": ""test""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1," ]] || false
    [[ "$output" =~ "2,1" ]] || false
    [[ "$output" =~ "4,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

@test "constraint-violations: self-referential foreign keys, add on theirs, delete on ours" {
    dolt sql <<"SQL"
CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES test (pk) ON DELETE CASCADE ON UPDATE CASCADE);
INSERT INTO test VALUES (1, NULL), (2, 1), (3, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "DELETE FROM test WHERE pk = 3"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (4, 3)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "test,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'foreign key,4,3,"{""Columns"": [""v1""], ""ForeignKey"": ""fk_name"", ""Index"": ""v1"", ""OnDelete"": ""CASCADE"", ""OnUpdate"": ""CASCADE"", ""ReferencedColumns"": [""pk""], ""ReferencedIndex"": ""pk"", ""ReferencedTable"": ""test"", ""Table"": ""test""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1," ]] || false
    [[ "$output" =~ "2,1" ]] || false
    [[ "$output" =~ "4,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
    
}

@test "constraint-violations: unique keys, insert violation" {
    dolt sql <<"SQL"
CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, UNIQUE INDEX(v1));
INSERT INTO test VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO test VALUES (3, 3), (4, 4)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (5, 5), (6, 3)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "test,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'unique index,6,3,"{""Columns"": [""v1""], ""Name"": ""v1""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "$output" =~ "4,4" ]] || false
    [[ "$output" =~ "5,5" ]] || false
    [[ "${#lines[@]}" = "6" ]] || false
}

@test "constraint-violations: unique keys, update violation from ours" {
    dolt sql <<"SQL"
CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, UNIQUE INDEX(v1));
INSERT INTO test VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "UPDATE test SET v1 = 3 WHERE pk = 2"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (3, 3)"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "test,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'unique index,3,3,"{""Columns"": [""v1""], ""Name"": ""v1""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,3" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "constraint-violations: unique keys, update violation from theirs" {
    dolt sql <<"SQL"
CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, UNIQUE INDEX(v1));
INSERT INTO test VALUES (1, 1), (2, 2);
SQL
    dolt add -A
    dolt commit -m "MC1"
    dolt branch other
    dolt sql -q "INSERT INTO test VALUES (3, 3)"
    dolt add -A
    dolt commit -m "MC2"
    dolt checkout other
    dolt sql -q "UPDATE test SET v1 = 3 WHERE pk = 2"
    dolt add -A
    dolt commit -m "OC1"
    dolt checkout main
    dolt merge other

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "test,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations_test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "violation_type,pk,v1,violation_info" ]] || false
    [[ "$output" =~ 'unique index,2,3,"{""Columns"": [""v1""], ""Name"": ""v1""}"' ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "$output" =~ "2,2" ]] || false
    [[ "$output" =~ "3,3" ]] || false
    [[ "${#lines[@]}" = "4" ]] || false
}

