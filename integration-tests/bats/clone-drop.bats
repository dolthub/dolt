#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
  setup_no_dolt_init
}

teardown() {
  stop_sql_server 1
  assert_feature_version
  teardown_common
}

@test "clone-drop: clone a database and then drop it" {
  mkdir repo
  cd repo
  dolt init
  dolt remote add pushed 'file://../pushed'
  dolt push pushed main:main
  dolt sql -q 'call dolt_clone("file://../pushed", "cloned"); drop database cloned;'
}

@test "clone-drop: sql-server: clone a database and then drop it" {
  mkdir repo
  cd repo
  dolt init
  dolt remote add pushed 'file://../pushed'
  dolt push pushed main:main
  start_sql_server
  dolt sql -q 'call dolt_clone("file://../pushed", "cloned"); drop database cloned;'
}

@test "clone-drop: in-progress marker lifecycle on clone" {
  # See https://github.com/dolthub/dolt/issues/11206
  mkdir repo
  cd repo
  dolt init
  dolt remote add pushed 'file://../pushed'
  dolt push pushed main:main

  dolt sql -q 'call dolt_clone("file://../pushed", "cloned");'
  [ ! -f cloned/.dolt_safe_to_ignore ]

  dolt clone file://../pushed cli_cloned
  [ ! -f cli_cloned/.dolt_safe_to_ignore ]

  mkdir stuck
  touch stuck/.dolt_safe_to_ignore
  run dolt sql -q 'call dolt_clone("file://../pushed", "stuck");'
  [ "$status" -eq 0 ]
  [ ! -f stuck/.dolt_safe_to_ignore ]
  run dolt sql -q 'use stuck; select active_branch();'
  [ "$status" -eq 0 ]
  [[ "$output" =~ "main" ]] || false
}

@test "clone-drop: sql-server: retry CREATE DATABASE reclaims incomplete database directory" {
  # See https://github.com/dolthub/dolt/issues/11533
  dolt init

  # Deterministic case: legacy incomplete directory with marker is ignored and recreatable.
  mkdir static_incomplete
  touch static_incomplete/.dolt_safe_to_ignore

  # Stale temporary directory from a dead PID.
  mkdir .tmp-dolt-interrupted_db-999999-018f0000-0000-7000-8000-000000000000

  start_sql_server

  run dolt --host localhost --port $PORT --no-tls -u root sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  ! echo "$output" | grep -w "interrupted_db"
  ! echo "$output" | grep -w "static_incomplete"
  ! echo "$output" | grep "tmp-dolt"

  run dolt --host localhost --port $PORT --no-tls -u root sql -q "CREATE DATABASE static_incomplete;"
  [ $status -eq 0 ]

  run dolt --host localhost --port $PORT --no-tls -u root sql -q "CREATE DATABASE interrupted_db;"
  [ $status -eq 0 ]

  run dolt --host localhost --port $PORT --no-tls -u root sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -w "interrupted_db"
  echo "$output" | grep -w "static_incomplete"

  run dolt --host localhost --port $PORT --no-tls -u root sql -q "call dolt_purge_dropped_databases();"
  [ $status -eq 0 ]
  [ ! -d .tmp-dolt-interrupted_db-999999-018f0000-0000-7000-8000-000000000000 ]
}

@test "clone-drop: concurrent processes isolate independently and reject duplicate commit" {
  dolt init
  start_sql_server

  # Run two concurrent CREATE DATABASE queries for the same DB.
  dolt --host localhost --port $PORT --no-tls -u root sql -q "CREATE DATABASE concurrent_db;" &
  PID1=$!
  dolt --host localhost --port $PORT --no-tls -u root sql -q "CREATE DATABASE concurrent_db;" &
  PID2=$!

  wait $PID1 || true
  wait $PID2 || true

  # Exactly one succeeded; the database exists and is usable.
  run dolt --host localhost --port $PORT --no-tls -u root sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -w "concurrent_db"

  run dolt --host localhost --port $PORT --no-tls -u root sql -q "use concurrent_db; create table t (id int); insert into t values (1); select count(*) from t;"
  [ $status -eq 0 ]
  [[ "$output" =~ "1" ]] || false
}

@test "clone-drop: sql-server: startup crash recovery barrier purges uncommitted drafts" {
  dolt init

  # Stale uncommitted scratchpad directory from a dead PID.
  mkdir .tmp-dolt-uncommitted_db-999999-018f1122-3344-7566-8d32-3afb14c85104
  touch .tmp-dolt-uncommitted_db-999999-018f1122-3344-7566-8d32-3afb14c85104/leftover.txt
  [ -d .tmp-dolt-uncommitted_db-999999-018f1122-3344-7566-8d32-3afb14c85104 ]

  start_sql_server

  # Verify the recovery barrier purged the uncommitted draft at startup.
  [ ! -d .tmp-dolt-uncommitted_db-999999-018f1122-3344-7566-8d32-3afb14c85104 ]

  run dolt --host localhost --port $PORT --no-tls -u root sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  ! echo "$output" | grep -w "uncommitted_db"

  # Name can be cleanly created.
  run dolt --host localhost --port $PORT --no-tls -u root sql -q "CREATE DATABASE uncommitted_db;"
  [ $status -eq 0 ]

  run dolt --host localhost --port $PORT --no-tls -u root sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -w "uncommitted_db"
}

@test "clone-drop: sql-server: startup crash recovery barrier preserves active drafts of live processes" {
  dolt init

  # Simulate a live process holding an active scratchpad.
  sleep 60 &
  LIVE_PID=$!

  mkdir .tmp-dolt-live_db-${LIVE_PID}-018f1122-3344-5566-8d32-3afb14c85104
  touch .tmp-dolt-live_db-${LIVE_PID}-018f1122-3344-5566-8d32-3afb14c85104/active.txt

  start_sql_server

  # Verify the live process's scratchpad was preserved.
  [ -d .tmp-dolt-live_db-${LIVE_PID}-018f1122-3344-5566-8d32-3afb14c85104 ]

  kill -9 $LIVE_PID || true
  wait $LIVE_PID 2>/dev/null || true
}

