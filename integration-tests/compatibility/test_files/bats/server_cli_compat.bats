#!/usr/bin/env bats
#
# Server compatibility tests verify that the SQL server and CLI client work across Dolt versions.
# Each test starts a server with one binary and connects using another, simulating the following
# scenarios run by CI:
#   old server, current client:     DOLT_LEGACY_BIN starts the server, DOLT_NEW_BIN is the client
#   current server, current client: both roles use the current build
#
# Required:
#   REPO_DIR        path to the pre-populated test repository created by setup_repo.sh
#   DEFAULT_BRANCH  default branch name of the repository
#
# Optional:
#   DOLT_LEGACY_BIN  server binary path; defaults to dolt in PATH
#   DOLT_NEW_BIN     client binary path; defaults to dolt in PATH
#   DOLT_VERSION     version string used to create the repository

setup_file() {
  export dolt_server=${DOLT_LEGACY_BIN:-dolt}
  export dolt_client=${DOLT_NEW_BIN:-dolt}
}

setup() {
  load helper/common
  load helper/server
  setup_common
  cp -Rpf "$REPO_DIR" "$BATS_TEST_TMPDIR/repo"
  cd "$BATS_TEST_TMPDIR/repo"
  SERVER_PID=""
  PORT=""
  DB_NAME=""
}

teardown() {
  stop_sql_server
  teardown_common
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

  skip_if_server_lt "1.58.6" "dolt show requires the dolt_tests system table, added in v1.58.6"

  run $dolt_client log -n 3
  [ "$status" -eq 0 ]
  [[ "$output" =~ "initialized data" ]] || false
  [[ "$output" =~ "made changes to main" ]] || false

  local latest_commit
  latest_commit=$(latest_commit)
  [[ -n "$latest_commit" ]] || false

  run $dolt_client show "$latest_commit"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "made changes to main" ]] || false

  # Use the big table to avoid conflicts with modifications to the abc table made on other branches.
  run $dolt_client sql -q "USE $DB_NAME; INSERT INTO big VALUES (10000, 'test commit');"
  [ "$status" -eq 0 ]

  run $dolt_client add -A
  [ "$status" -eq 0 ]

  run $dolt_client commit -m "test commit message"
  [ "$status" -eq 0 ]
  commit_output=$(strip_ansi "$output")
  [[ "$commit_output" =~ "test commit message" ]] || false
  [[ "$commit_output" =~ "commit " ]] || false

  commit_to_revert=$(extract_commit_hash "$output")
  [[ -n "$commit_to_revert" ]] || false

  run $dolt_client revert "$commit_to_revert"
  [ "$status" -eq 0 ]
  revert_output=$(strip_ansi "$output")
  [[ "$revert_output" =~ "commit " ]] || false

  # dolt merge is not tested here because it requires direct repository access and cannot run against a SQL server.

  # Use check_merge branch because it only modifies the def table, avoiding conflicts with the big table changes above.
  run $dolt_client log check_merge -n 1
  [ "$status" -eq 0 ]
  cherry_commit=$(extract_commit_hash "$output")
  [[ -n "$cherry_commit" ]] || false

  run $dolt_client cherry-pick "$cherry_commit"
  [ "$status" -eq 0 ]
  cherry_output=$(strip_ansi "$output")
  # cherry-pick prints commit info when it creates a commit, or "No changes were made" when the patch is already applied.
  [[ "$cherry_output" =~ "commit " ]] || [[ "$cherry_output" =~ "No changes were made" ]] || false
}
