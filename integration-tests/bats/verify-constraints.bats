#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1
    dolt sql <<"SQL"
CREATE TABLE parent3 (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1));
CREATE TABLE child3 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name1 FOREIGN KEY (v1) REFERENCES parent3 (v1));
CREATE TABLE parent4 (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1));
CREATE TABLE child4 (pk BIGINT PRIMARY KEY, v1 BIGINT, CONSTRAINT fk_name2 FOREIGN KEY (v1) REFERENCES parent4 (v1));
INSERT INTO parent3 VALUES (1, 1);
INSERT INTO parent4 VALUES (2, 2);
SET foreign_key_checks=0;
INSERT INTO child3 VALUES (1, 1), (2, 2);
INSERT INTO child4 VALUES (1, 1), (2, 2);
SET foreign_key_checks=1;
SQL
    dolt add -A
    dolt commit --force -m "has fk violations"
    dolt sql <<SQL
CREATE TABLE parent1 (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  INDEX (v1)
);
CREATE TABLE parent2 (
  pk BIGINT PRIMARY KEY,
  v1 BIGINT,
  INDEX (v1)
);
CREATE TABLE child1 (
  pk BIGINT PRIMARY KEY,
  parent1_v1 BIGINT,
  parent2_v1 BIGINT,
  CONSTRAINT child1_parent1 FOREIGN KEY (parent1_v1) REFERENCES parent1 (v1),
  CONSTRAINT child1_parent2 FOREIGN KEY (parent2_v1) REFERENCES parent2 (v1)
);
CREATE TABLE child2 (
  pk BIGINT PRIMARY KEY,
  parent2_v1 BIGINT,
  CONSTRAINT child2_parent2 FOREIGN KEY (parent2_v1) REFERENCES parent2 (v1)
);
INSERT INTO parent1 VALUES (1,1), (2,2), (3,3);
INSERT INTO parent2 VALUES (1,1), (2,2), (3,3);
INSERT INTO child1 VALUES (1,1,1), (2,2,2);
INSERT INTO child2 VALUES (2,2), (3,3);
SET foreign_key_checks=0;
INSERT INTO child3 VALUES (3, 3);
INSERT INTO child4 VALUES (3, 3);
SET foreign_key_checks=1;
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "verify-constraints: Constraints verified" {
    dolt constraints verify child1 child2
}

@test "verify-constraints: One table fails" {
    dolt sql <<SQL
SET foreign_key_checks=0;
DELETE FROM parent1 WHERE pk = 1;
SET foreign_key_checks=1;
SQL
    run dolt constraints verify child1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "child1_parent1" ]] || false
    dolt constraints verify child2
    run dolt constraints verify child1 child2
    [ "$status" -eq "1" ]
    [[ "$output" =~ "child1_parent1" ]] || false
    [[ ! "$output" =~ "child1_parent2" ]] || false
    [[ ! "$output" =~ "child2_parent2" ]] || false
}

@test "verify-constraints: Two tables fail" {
    dolt sql <<SQL
SET foreign_key_checks=0;
DELETE FROM parent2 WHERE pk = 2;
SET foreign_key_checks=1;
SQL
    run dolt constraints verify child1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "child1_parent2" ]] || false
    [[ ! "$output" =~ "child1_parent1" ]] || false
    run dolt constraints verify child2
    [ "$status" -eq "1" ]
    [[ "$output" =~ "child2_parent2" ]] || fals
    run dolt constraints verify child1 child2
    [ "$status" -eq "1" ]
    [[ "$output" =~ "child1_parent2" ]] || false
    [[ "$output" =~ "child2_parent2" ]] || false
    [[ ! "$output" =~ "child1_parent1" ]] || false
}

@test "verify-constraints: Ignores NULLs" {
    dolt sql <<SQL
CREATE TABLE parent (
    id BIGINT PRIMARY KEY,
    v1 BIGINT,
    v2 BIGINT,
    INDEX idx1 (v1, v2)
);
CREATE TABLE child (
    id BIGINT primary key,
    v1 BIGINT,
    v2 BIGINT,
    CONSTRAINT fk_named FOREIGN KEY (v1,v2) REFERENCES parent(v1,v2)
);
INSERT INTO parent VALUES (1, 1, 1), (2, 2, 2);
INSERT INTO child VALUES (1, 1, 1), (2, 20, NULL);
SQL
    dolt constraints verify child

    dolt sql <<SQL
SET foreign_key_checks=0;
INSERT INTO child VALUES (3, 30, 30);
SET foreign_key_checks=1;
SQL
    run dolt constraints verify child
    [ "$status" -eq "1" ]
    [[ "$output" =~ "fk_named" ]] || false
}

@test "verify-constraints: CLI missing --all and --output-only, no named tables" {
    run dolt constraints verify
    [ "$status" -eq "1" ]
    [[ "$output" =~ "fk_name1" ]] || false
    [[ "$output" =~ "fk_name2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,1" ]] || false
    [[ "$output" =~ "child4,1" ]] || false
}

@test "verify-constraints: CLI missing --all and --output-only, named tables" {
    run dolt constraints verify child3
    [ "$status" -eq "1" ]
    [[ "$output" =~ "fk_name1" ]] || false
    [[ ! "$output" =~ "fk_name2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "verify-constraints: CLI --all" {
    run dolt constraints verify --all child3 child4
    [ "$status" -eq "1" ]
    [[ "$output" =~ "fk_name1" ]] || false
    [[ "$output" =~ "fk_name2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,2" ]] || false
    [[ "$output" =~ "child4,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "verify-constraints: CLI --output-only" {
    run dolt constraints verify --output-only child3 child4
    [ "$status" -eq "1" ]
    [[ "$output" =~ "fk_name1" ]] || false
    [[ "$output" =~ "fk_name2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ ! "$output" =~ "child3,2" ]] || false
    [[ ! "$output" =~ "child4,2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "verify-constraints: CLI --all and --output-only" {
    run dolt constraints verify --all --output-only child3 child4
    [ "$status" -eq "1" ]
    [[ "$output" =~ "fk_name1" ]] || false
    [[ "$output" =~ "fk_name2" ]] || false
    [[ "$output" =~ "| 2  | 2  |" ]] || false
    [[ "$output" =~ "| 3  | 3  |" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ ! "$output" =~ "child3,2" ]] || false
    [[ ! "$output" =~ "child4,2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "verify-constraints: SQL no violations" {
    run dolt sql -q "SELECT CONSTRAINTS_VERIFY('child1')" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "CONSTRAINTS_VERIFY('child1')" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ ! "$output" =~ "child1_parent1" ]] || false
    [[ ! "$output" =~ "child1_parent2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false

    run dolt sql -q "SELECT CONSTRAINTS_VERIFY_ALL('child1')" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "CONSTRAINTS_VERIFY_ALL('child1')" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ ! "$output" =~ "child1_parent1" ]] || false
    [[ ! "$output" =~ "child1_parent2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "verify-constraints: Stored Procedure no violations" {
    run dolt sql -q "CALL DOLT_VERIFY_CONSTRAINTS('child1')" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "no_violations" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ ! "$output" =~ "child1_parent1" ]] || false
    [[ ! "$output" =~ "child1_parent2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false

    run dolt sql -q "CALL DOLT_VERIFY_ALL_CONSTRAINTS('child1')" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "no_violations" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    run dolt sql -q "CALL DVERIFY_ALL_CONSTRAINTS('child1')" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "no_violations" ]] || false
    [[ "$output" =~ "1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ ! "$output" =~ "child1_parent1" ]] || false
    [[ ! "$output" =~ "child1_parent2" ]] || false
    [[ "${#lines[@]}" = "1" ]] || false
}

@test "verify-constraints: SQL CONSTRAINTS_VERIFY() no named tables" {
    run dolt sql -b -q "SET dolt_force_transaction_commit = 1;SELECT CONSTRAINTS_VERIFY();" -r=json
    [ "$status" -eq "0" ]
    [[ "$output" =~ "{\"CONSTRAINTS_VERIFY()\":0}" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,1" ]] || false
    [[ "$output" =~ "child4,1" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "verify-constraints: Stored Procedure CONSTRAINTS_VERIFY() no named tables" {
    run dolt sql -b -q "SET dolt_force_transaction_commit = 1;CALL DOLT_VERIFY_CONSTRAINTS();" -r=json
    [ "$status" -eq "0" ]
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,1" ]] || false
    [[ "$output" =~ "child4,1" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "verify-constraints: SQL CONSTRAINTS_VERIFY() named table" {
    run dolt sql -b -q "SET dolt_force_transaction_commit = 1;SELECT CONSTRAINTS_VERIFY('child3');" -r=json
    [ "$status" -eq "0" ]
    [[ "$output" =~ "{\"CONSTRAINTS_VERIFY('child3')\":0}" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,1" ]] || false
    [[ ! "$output" =~ "child4,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "verify-constraints: Stored Procedure CONSTRAINTS_VERIFY() named table" {
    run dolt sql -b -q "SET dolt_force_transaction_commit = 1;CALL DOLT_VERIFY_CONSTRAINTS('child3');" -r=json
    [ "$status" -eq "0" ]
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,1" ]] || false
    [[ ! "$output" =~ "child4,1" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "verify-constraints: SQL CONSTRAINTS_VERIFY() named tables" {
    run dolt sql -b -q "SET dolt_force_transaction_commit = 1;SELECT CONSTRAINTS_VERIFY('child3', 'child4');" -r=json
    [ "$status" -eq "0" ]
    [[ "$output" =~ "{\"CONSTRAINTS_VERIFY('child3', 'child4')\":0}" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,1" ]] || false
    [[ "$output" =~ "child4,1" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "verify-constraints: Stored Procedure CONSTRAINTS_VERIFY() named tables" {
    run dolt sql -b -q "SET dolt_force_transaction_commit = 1;CALL DOLT_VERIFY_CONSTRAINTS('child3', 'child4');" -r=json
    [ "$status" -eq "0" ]
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,1" ]] || false
    [[ "$output" =~ "child4,1" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "verify-constraints: SQL CONSTRAINTS_VERIFY_ALL() no named tables" {
    run dolt sql -b -q "SET dolt_force_transaction_commit = 1;SELECT CONSTRAINTS_VERIFY_ALL();" -r=json
    [ "$status" -eq "0" ]
    [[ "$output" =~ "{\"CONSTRAINTS_VERIFY_ALL()\":0}" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,2" ]] || false
    [[ "$output" =~ "child4,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "verify-constraints: Stored Procedure CONSTRAINTS_VERIFY_ALL() no named tables" {
    run dolt sql -b -q "SET dolt_force_transaction_commit = 1;CALL DOLT_VERIFY_ALL_CONSTRAINTS();" -r=json
    [ "$status" -eq "0" ]
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,2" ]] || false
    [[ "$output" =~ "child4,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "verify-constraints: SQL CONSTRAINTS_VERIFY_ALL() named table" {
    run dolt sql -b -q "SET dolt_force_transaction_commit = 1;SELECT CONSTRAINTS_VERIFY_ALL('child3');" -r=json
    [ "$status" -eq "0" ]
    [[ "$output" =~ "{\"CONSTRAINTS_VERIFY_ALL('child3')\":0}" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,2" ]] || false
    [[ ! "$output" =~ "child4,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "verify-constraints: Stored Procedure CONSTRAINTS_VERIFY_ALL() named table" {
    run dolt sql -b -q "SET dolt_force_transaction_commit = 1;CALL DOLT_VERIFY_ALL_CONSTRAINTS('child3');" -r=json
    [ "$status" -eq "0" ]
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,2" ]] || false
    [[ ! "$output" =~ "child4,2" ]] || false
    [[ "${#lines[@]}" = "2" ]] || false
}

@test "verify-constraints: SQL CONSTRAINTS_VERIFY_ALL() named tables" {
    run dolt sql -b -q "SET dolt_force_transaction_commit = 1;SELECT CONSTRAINTS_VERIFY_ALL('child3', 'child4');" -r=json
    [ "$status" -eq "0" ]
    [[ "$output" =~ "{\"CONSTRAINTS_VERIFY_ALL('child3', 'child4')\":0}" ]] || false
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,2" ]] || false
    [[ "$output" =~ "child4,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}

@test "verify-constraints: Stored Procedure CONSTRAINTS_VERIFY_ALL() named tables" {
    run dolt sql -b -q "SET dolt_force_transaction_commit = 1;CALL DOLT_VERIFY_ALL_CONSTRAINTS('child3', 'child4');" -r=json
    [ "$status" -eq "0" ]
    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [[ "$output" =~ "child3,2" ]] || false
    [[ "$output" =~ "child4,2" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false
}
