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
  export BATS_TEST_RETRIES=3
}

setup() {
  load helper/common
  load helper/server
  cp -Rpf "$REPO_DIR" "$BATS_TEST_TMPDIR/repo"
  cd "$BATS_TEST_TMPDIR/repo"
  SERVER_PID=""
  PORT=""
  DB_NAME=""
  skip_if_client_lt "1.20.0" "client does not support global --host/--port/--user/--password connection flags"
  skip_if_server_lt "1.2.0" "v1.0.0 sql-server does not accept connections from the current client"
  start_sql_server
}

teardown() {
  stop_sql_server
}

@test "server-cli-compat: basic connection" {
  run dolt_cli sql -q "SELECT 1 as test;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "| test |" ]] || false
  [[ "$output" =~ "| 1    |" ]] || false
}

@test "server-cli-compat: dolt log shows commit history" {
  skip_if_server_lt "1.54.0" "dolt log queries the commit_order column in dolt_log, added in v1.54.0"

  run dolt_cli log -n 3
  [ "$status" -eq 0 ]
  [[ "$output" =~ "initialized data" ]] || false
  [[ "$output" =~ "made changes to main" ]] || false
}

@test "server-cli-compat: dolt show displays commit metadata" {
  skip_if_server_lt "1.54.0" "dolt show queries the commit_order column in dolt_log, added in v1.54.0"
  skip_if_client_lt "1.54.0" "dolt show queries the commit_order column in dolt_log, added in v1.54.0"

  local latest_commit
  latest_commit=$(latest_commit)
  [[ -n "$latest_commit" ]] || false

  run dolt_cli show "$latest_commit"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "made changes to main" ]] || false
}

@test "server-cli-compat: commit and revert via client" {
  skip_if_server_lt "1.86.0" "DOLT_REVERT returns conflict count columns starting in v1.86.0, and dolt commit/revert print commit info using the commit_order column in dolt_log added in v1.54.0"

  run dolt_cli sql -q "INSERT INTO big VALUES (10000, 'test commit');"
  [ "$status" -eq 0 ]

  run dolt_cli add -A
  [ "$status" -eq 0 ]

  run dolt_cli commit -m "test commit message"
  [ "$status" -eq 0 ]
  commit_output=$(strip_ansi "$output")
  [[ "$commit_output" =~ "test commit message" ]] || false
  [[ "$commit_output" =~ "commit " ]] || false

  commit_to_revert=$(extract_commit_hash "$output")
  [[ -n "$commit_to_revert" ]] || false

  run dolt_cli revert "$commit_to_revert"
  [ "$status" -eq 0 ]
  revert_output=$(strip_ansi "$output")
  [[ "$revert_output" =~ "commit " ]] || false
}

@test "server-cli-compat: old client can read dolt_log and commits created with separate author and committer" {
  skip_if_server_lt "1.44.2" "servers older than v1.44.2 reject writes via --use-db with 'database is read only'"

  # New server creates a commit with distinct author and committer via --author flag.
  run dolt_cli sql -q "INSERT INTO big VALUES (10001, 'author committer test');"
  [ "$status" -eq 0 ]
  run dolt_cli add -A
  [ "$status" -eq 0 ]
  run dolt_cli commit --author "Explicit Author <explicit@author.com>" -m "author committer compat test"
  [ "$status" -eq 0 ]

  # Old client must still be able to read dolt_log and see the commit.
  run $dolt_server sql -r csv -q "SELECT committer, email, message FROM dolt_log WHERE message = 'author committer compat test' LIMIT 1;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "author committer compat test" ]] || false
}

@test "server-cli-compat: cherry-pick via client" {
  skip_if_server_lt "1.54.0" "dolt log/cherry-pick query the commit_order column in dolt_log, added in v1.54.0"
  skip_if_client_lt "1.44.2" "clients older than v1.44.2 can't read the current server's storage format (unknown record field tag)"

  # dolt merge is not tested here because it requires direct repository access and cannot run against a SQL server.

  # Use check_merge branch because it only modifies the def table, avoiding conflicts with other changes.
  run dolt_cli log check_merge -n 1
  [ "$status" -eq 0 ]
  cherry_commit=$(extract_commit_hash "$output")
  [[ -n "$cherry_commit" ]] || false

  run dolt_cli cherry-pick "$cherry_commit"
  [ "$status" -eq 0 ]
  cherry_output=$(strip_ansi "$output")
  # cherry-pick prints commit info when it creates a commit, or "No changes were made" when the patch is already applied.
  [[ "$cherry_output" =~ "commit " ]] || [[ "$cherry_output" =~ "No changes were made" ]] || false
}

