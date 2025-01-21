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
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test tests remote connections directly, SQL_ENGINE is not needed."
    fi
    start_sql_server_with_config "" config.yml
    dolt sql -q "INSERT INTO test1 (c0) select 0 from sequence10bit; INSERT INTO ranges VALUES (0, LAST_INSERT_ID(), ROW_COUNT()); COMMIT;" &
    dolt sql -q "INSERT INTO test1 (c0) select 1 from sequence10bit; INSERT INTO ranges VALUES (1, LAST_INSERT_ID(), ROW_COUNT()); COMMIT;"
    wait $!

    stop_sql_server

    run dolt sql -r csv -q "select
      c0,
      min(pk) = firstId - rowCount + 1,
      rowCount = 1024,
      max(pk) = firstId
    from test1 join ranges on test1.c0 = ranges.pk group by c0"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "0,true,true,true" ]] || false
    [[ "$output" =~ "1,true,true,true" ]] || false
}
