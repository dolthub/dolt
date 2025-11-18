#!/usr/bin/env bats
#
# Server compatibility tests verify that SQL server and client work across Dolt versions.
#
# The tests run against a pre-populated repository created by setup_repo.sh. Each test starts a SQL server using one
# Dolt version and connects with a client using potentially a different Dolt version.
#
# The CI pipeline runs these tests in three scenarios:
# * old server, current client: starts SQL server with old Dolt version, connects with current client
# * current server, old client: starts SQL server with current Dolt version, connects with old client
# * current server, current client: starts SQL server with current Dolt version, connects with current client
#
# Environment variables:
# REPO_DIR contains the test repository path.
# DEFAULT_BRANCH contains the default branch name.
# DOLT_VERSION contains the Dolt version used to create the repository.
# DOLT_SERVER_BIN contains the path to the dolt binary to use for the server.
# DOLT_CLIENT_BIN contains the path to the dolt binary to use for the client.
# If DOLT_SERVER_BIN or DOLT_CLIENT_BIN are not set, the dolt in PATH is used.

load $BATS_TEST_DIRNAME/helper/common.bash

definePORT() {
  for i in {0..99}
  do
    port=$((RANDOM % 4096 + 2048))
    run nc -z localhost $port
    if [ "$status" -eq 1 ]; then
      echo $port
      break
    fi
  done
}

wait_for_connection() {
  local port=$1
  local timeout=$2
  local end_time=$((SECONDS+($timeout/1000)))
  
  while [ $SECONDS -lt $end_time ]; do
    run $dolt_client sql -q "SELECT 1;"
    if [ $status -eq 0 ]; then
      return 0
    fi
    sleep 1
  done
  
  echo "Failed to connect to database on port $port within $timeout ms." >&2
  return 1
}

start_sql_server() {
  PORT=$(definePORT)
  DB_NAME=$(basename "$PWD")
  
  if [ "$IS_WINDOWS" != true ]; then
    $dolt_server sql-server --host 0.0.0.0 --port=$PORT --socket "dolt.$PORT.sock" > server.log 2>&1 3>&- &
  else
    $dolt_server sql-server --host 0.0.0.0 --port=$PORT > server.log 2>&1 3>&- &
  fi
  SERVER_PID=$!
  
  wait_for_connection $PORT 8500
}

stop_sql_server() {
  if [ ! -z "$SERVER_PID" ]; then
    kill $SERVER_PID 2>/dev/null || true
    while ps -p $SERVER_PID > /dev/null 2>&1; do
      sleep 0.1
    done
  fi
  SERVER_PID=""
  if [ -n "$PORT" ] && [ -f "dolt.$PORT.sock" ]; then
    rm -f "dolt.$PORT.sock"
  fi
}

setup_file() {
  export dolt_server=${DOLT_SERVER_BIN:-dolt}
  export dolt_client=${DOLT_CLIENT_BIN:-dolt}
}

setup() {
  setup_common
  cp -Rpf $REPO_DIR bats_repo
  cd bats_repo
  SERVER_PID=""
  PORT=""
  DB_NAME=""
}

teardown() {
  stop_sql_server
  cd ..
  rm -rf bats_repo
  teardown_common
}

strip_ansi() {
  printf "%s\n" "$1" | sed 's/\x1b\[[0-9;]*m//g'
}

extract_commit_hash() {
  printf "%s\n" "$1" | sed 's/\x1b\[[0-9;]*m//g' | grep -m1 '^commit ' | awk '{print $2}'
}

latest_commit() {
  run $dolt_client log
  [ "$status" -eq 0 ]
  extract_commit_hash "$output"
}

@test "sql-server: basic connection" {
  start_sql_server
  
  run $dolt_client sql -q "SELECT 1 as test;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "| test |" ]] || false
  [[ "$output" =~ "| 1    |" ]] || false
}

@test "sql-server: commit-related commands can retrieve commit metadata via internal dolt_log interface" {
  start_sql_server

  server_version=$($dolt_server version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
  if [ -n "$server_version" ]; then
    major_minor=$(echo "$server_version" | cut -d. -f1,2)
    if [ "$(printf '%s\n' "$major_minor" "1.58" | sort -V | head -n1)" != "1.58" ]; then
      skip "dolt show requires dolt_tests system table (added in v1.58.6), skipping for server version $server_version"
    fi
  fi

  # Test dolt log - should display commit info
  run $dolt_client log -n 3
  [ "$status" -eq 0 ]
  [[ "$output" =~ "initialized data" ]] || false
  [[ "$output" =~ "made changes to main" ]] || false

  # Test dolt show - extract commit hash and verify output
  local latest_commit
  latest_commit=$(latest_commit)
  [[ -n "$latest_commit" ]] || false

  run $dolt_client show "$latest_commit"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "made changes to main" ]] || false

  # Test dolt commit - create a new commit and verify output
  # Use 'big' table to avoid conflicts with 'abc' table modifications from other branches
  run $dolt_client sql -q "USE $DB_NAME; INSERT INTO big VALUES (10000, 'test commit');"
  [ "$status" -eq 0 ]

  run $dolt_client add -A
  [ "$status" -eq 0 ]

  run $dolt_client commit -m "test commit message"
  [ "$status" -eq 0 ]
  commit_output=$(strip_ansi "$output")
  [[ "$commit_output" =~ "test commit message" ]] || false
  [[ "$commit_output" =~ "commit " ]] || false

  # Test dolt revert - revert the commit we just created
  commit_to_revert=$(extract_commit_hash "$output")
  [[ -n "$commit_to_revert" ]] || false

  run $dolt_client revert "$commit_to_revert"
  [ "$status" -eq 0 ]
  revert_output=$(strip_ansi "$output")
  [[ "$revert_output" =~ "commit " ]] || false

  # Note: dolt merge is not tested here because it cannot be used when a SQL server is running.
  # The dolt merge command requires direct repository access which conflicts with an active server.

  # Test dolt cherry-pick - cherry-pick a commit from check_merge branch (only modifies def table, won't conflict)
  run $dolt_client log check_merge -n 1
  [ "$status" -eq 0 ]
  cherry_commit=$(extract_commit_hash "$output")
  [[ -n "$cherry_commit" ]] || false

  run $dolt_client cherry-pick "$cherry_commit"
  # If it creates a commit, it shows commit info, otherwise it says "No changes were made"
  [ "$status" -eq 0 ]
  cherry_output=$(strip_ansi "$output")
  [[ "$cherry_output" =~ "commit " ]] || [[ "$cherry_output" =~ "No changes were made" ]] || false
}
