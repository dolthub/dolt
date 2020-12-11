#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
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
SQL
}

teardown() {
    teardown_common
}

@test "verify-constraints: Constraints verified" {
    dolt verify-constraints child1 child2
}

@test "verify-constraints: One table fails" {
    dolt sql <<SQL
SET foreign_key_checks=0;
DELETE FROM parent1 WHERE pk = 1;
SET foreign_key_checks=1;
SQL
    run dolt verify-constraints child1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "child1_parent1" ]] || false
    dolt verify-constraints child2
    run dolt verify-constraints child1 child2
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
    run dolt verify-constraints child1
    [ "$status" -eq "1" ]
    [[ "$output" =~ "child1_parent2" ]] || false
    [[ ! "$output" =~ "child1_parent1" ]] || false
    run dolt verify-constraints child2
    [ "$status" -eq "1" ]
    [[ "$output" =~ "child2_parent2" ]] || fals
    run dolt verify-constraints child1 child2
    [ "$status" -eq "1" ]
    [[ "$output" =~ "child1_parent2" ]] || false
    [[ "$output" =~ "child2_parent2" ]] || false
    [[ ! "$output" =~ "child1_parent1" ]] || false
}
