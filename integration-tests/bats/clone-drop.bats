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

  # Deterministic case avoiding timing races with the marker.
  mkdir static_incomplete
  touch static_incomplete/.dolt_safe_to_ignore

  start_sql_server

  dolt --host localhost --port $PORT --no-tls -u root sql -q "CREATE DATABASE interrupted_db;" &
  CLIENT_PID=$!

  for i in $(seq 1 5000); do
    if [ -f "interrupted_db/.dolt_safe_to_ignore" ]; then
      kill -9 $SERVER_PID
      kill -9 $CLIENT_PID 2>/dev/null || true
      break
    fi
  done
  wait $CLIENT_PID 2>/dev/null || true
  wait $SERVER_PID 2>/dev/null || true

  [ -f "interrupted_db/.dolt_safe_to_ignore" ]

  start_sql_server

  run dolt --host localhost --port $PORT --no-tls -u root sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  ! echo "$output" | grep -w "interrupted_db"
  ! echo "$output" | grep -w "static_incomplete"

  run dolt --host localhost --port $PORT --no-tls -u root sql -q "CREATE DATABASE static_incomplete;"
  [ $status -eq 0 ]

  run dolt --host localhost --port $PORT --no-tls -u root sql -q "CREATE DATABASE interrupted_db;"
  [ $status -eq 0 ]
}

@test "clone-drop: concurrent processes respect in-progress marker during creation" {
  skiponwindows "flock is unix-specific"
  dolt init
  start_sql_server

  # Process 1 starts creating a database and holds the OS file lock on the in-progress marker.
  mkdir in_progress_db
  touch in_progress_db/.dolt_safe_to_ignore

  # Launch Process 1 holding the lock in the background for 2 seconds.
  flock -x in_progress_db/.dolt_safe_to_ignore sleep 2 &
  PROC1_PID=$!

  # Give Process 1 a moment to acquire the lock.
  sleep 0.2

  # Process 2 attempts to create the same database while Process 1 holds the lock.
  run dolt --host localhost --port $PORT --no-tls -u root sql -q "CREATE DATABASE in_progress_db;"
  [ $status -ne 0 ]
  [[ "$output" =~ "incomplete database directory from an interrupted create already exists" ]] || false

  # Wait for Process 1 to complete and release the lock.
  wait $PROC1_PID

  # Process 2 retries after Process 1 terminates/releases the lock; it reclaims and succeeds.
  run dolt --host localhost --port $PORT --no-tls -u root sql -q "CREATE DATABASE in_progress_db;"
  [ $status -eq 0 ]

  run dolt --host localhost --port $PORT --no-tls -u root sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -w "in_progress_db"
}

