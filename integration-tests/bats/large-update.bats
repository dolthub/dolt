#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "large-update: run a large update" {
    dolt sql <<SQL
CREATE TABLE test (
  id INT PRIMARY KEY,
  val FLOAT
);
INSERT INTO test (id, val) VALUES
  (0, 0.0),
  (1, 0.0),
  (2, 0.0),
  (3, 0.0),
  (4, 0.0),
  (5, 0.0),
  (6, 0.0),
  (7, 0.0),
  (8, 0.0),
  (9, 0.0),
  (10, 0.0),
  (11, 0.0),
  (12, 0.0),
  (13, 0.0),
  (14, 0.0),
  (15, 0.0);
-- 32
INSERT INTO test (id, val)
  SELECT id + (SELECT COUNT(*) FROM test), 0.0 FROM test;
-- 64
INSERT INTO test (id, val)
  SELECT id + (SELECT COUNT(*) FROM test), 0.0 FROM test;
-- 128
INSERT INTO test (id, val)
  SELECT id + (SELECT COUNT(*) FROM test), 0.0 FROM test;
-- 256
INSERT INTO test (id, val)
  SELECT id + (SELECT COUNT(*) FROM test), 0.0 FROM test;
-- 512
INSERT INTO test (id, val)
  SELECT id + (SELECT COUNT(*) FROM test), 0.0 FROM test;
-- 1024
INSERT INTO test (id, val)
  SELECT id + (SELECT COUNT(*) FROM test), 0.0 FROM test;
-- 2048
INSERT INTO test (id, val)
  SELECT id + (SELECT COUNT(*) FROM test), 0.0 FROM test;
-- 4096
INSERT INTO test (id, val)
  SELECT id + (SELECT COUNT(*) FROM test), 0.0 FROM test;
-- 8192
INSERT INTO test (id, val)
  SELECT id + (SELECT COUNT(*) FROM test), 0.0 FROM test;
-- 16384
INSERT INTO test (id, val)
  SELECT id + (SELECT COUNT(*) FROM test), 0.0 FROM test;
-- 32768
INSERT INTO test (id, val)
  SELECT id + (SELECT COUNT(*) FROM test), 0.0 FROM test;
-- 65536
INSERT INTO test (id, val)
  SELECT id + (SELECT COUNT(*) FROM test), 0.0 FROM test;
-- 131072
INSERT INTO test (id, val)
  SELECT id + (SELECT COUNT(*) FROM test), 0.0 FROM test;
-- 262144
INSERT INTO test (id, val)
  SELECT id + (SELECT COUNT(*) FROM test), 0.0 FROM test;
UPDATE test SET val = val + 0.001;
SQL
    run dolt sql -r csv -q 'select count(*) from test'
    [[ "${#lines[@]}" = "2" ]] || false
    [[ "${lines[1]}" = "262144" ]] || (echo "expected count to be 262144 but got ${lines[1]}"; false)
}
