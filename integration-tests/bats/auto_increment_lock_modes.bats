#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
CREATE TABLE test1 (
  pk int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  c0 int,
  index t1_c_index (c0)
);
CREATE TABLE test2 (
  pk int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  c0 int,
  index t2_c_index (c0)
);
-- We use this table to store the first id and row count of both insert operations, which helps us prove
-- that the generated id ranges are contiguous for each transaction.
CREATE TABLE ranges (
  pk int NOT NULL PRIMARY KEY,
  firstId int,
  rowCount int
);

-- We use this table to demonstrate that concurrent writes to different tables can and are interleaved
CREATE TABLE timestamps (
  pk int NOT NULL PRIMARY KEY AUTO_INCREMENT,
  t int
);

delimiter |
CREATE TRIGGER t1 AFTER INSERT ON test1
  FOR EACH ROW
  BEGIN
    INSERT INTO timestamps VALUES (0, 1);
  END|

CREATE TRIGGER t2 AFTER INSERT ON test2
  FOR EACH ROW
  BEGIN
    INSERT INTO timestamps VALUES (0, 2);
  END|
delimiter ;

CREATE VIEW bin AS SELECT 0 AS v UNION ALL SELECT 1;
CREATE VIEW sequence5bit AS SELECT b1.v + 2*b2.v + 4*b3.v + 8*b4.v + 16*b5.v AS v from bin b1, bin b2, bin b3, bin b4, bin b5;
CREATE VIEW sequence10bit AS SELECT b1.v + 32*b2.v AS v from sequence5bit b1, sequence5bit b2;
CREATE VIEW sequence15bit AS SELECT b1.v + 32*b2.v + 32*32*b3.v AS v from sequence5bit b1, sequence5bit b2, sequence5bit b3;
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "auto_increment_lock_modes: multiple inserts to the same table aren't interleaved when innodb_autoinc_lock_mode = 0" {
      cat > config.yml <<EOF
system_variables:
  innodb_autoinc_lock_mode: 0
EOF
    start_sql_server_with_config "" config.yml
    dolt sql -q "SET @@autocommit=1; INSERT INTO test1 (c0) select 0 from sequence10bit; INSERT INTO ranges VALUES (0, LAST_INSERT_ID(), ROW_COUNT());" &
    dolt sql -q "SET @@autocommit=1; INSERT INTO test1 (c0) select 1 from sequence10bit; INSERT INTO ranges VALUES (1, LAST_INSERT_ID(), ROW_COUNT());"
    wait $!

    stop_sql_server

    run dolt sql -r csv -q "select
      c0,
      min(pk) = firstId,
      rowCount = 1024,
      max(pk) = firstId + rowCount -1
    from test1 join ranges on test1.c0 = ranges.pk group by c0"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,true,true,true" ]] || false
    [[ "$output" =~ "1,true,true,true" ]] || false
}


@test "auto_increment_lock_modes: multiple tables can have interleaved inserts without blocking each other." {
      cat > config.yml <<EOF
system_variables:
  innodb_autoinc_lock_mode: 0
EOF
    start_sql_server_with_config "" config.yml
    # We use run here so that we don't terminate if the command fails, since terminating without stopping the server will cause the test to hang.
    # We also use a small sequence table because larger tables increase the risk of triggering https://github.com/dolthub/dolt/issues/7702
    # If we detect interleaved writes to `timestamps` with a smaller sequence table, then we would see it on the larger table anyway.
    run dolt sql -q "INSERT INTO test1 (c0) select v from sequence5bit; SELECT * from timestamps; COMMIT;" &
    run dolt sql -q "INSERT INTO test2 (c0) select v from sequence5bit; SELECT * from timestamps; COMMIT;"
    wait $!
    stop_sql_server
    # We confirm that the two inserts are interleaved by comparing the min and max timestamps from both tables.
    run dolt sql -q "select (select min(pk) from timestamps where t = 1) < (select max(pk) from timestamps where t = 2)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false
    run dolt sql -q "select (select min(pk) from timestamps where t = 2) < (select max(pk) from timestamps where t = 1)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false
}
