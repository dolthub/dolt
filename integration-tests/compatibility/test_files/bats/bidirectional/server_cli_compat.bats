#!/usr/bin/env bats
# Cross-version SQL server and CLI client tests. DOLT_OLD_BIN runs the sql-server,
# DOLT_NEW_BIN runs the client; runner.sh swaps them on the second bidirectional pass.

setup_file() {
  export BATS_TEST_RETRIES=3
}

setup() {
  bats_load_library windows-compat.bash
  bats_load_library query-server-common.bash
  bats_load_library common.bash
  bats_load_library compat-common.bash
  bats_load_library compat-server.bash

  # Each test runs in a fresh tmpdir so state never leaks between tests or passes.
  mkdir -p "$BATS_TEST_TMPDIR/repo"
  cd "$BATS_TEST_TMPDIR/repo"

  skip_if_new_lte "1.20.0" "new_dolt does not support global --host/--port/--user/--password connection flags"
  skip_if_old_lte "1.2.0" "sql-server does not accept connections from client"

  init_min_repo

  start_old_sql_server
}

teardown() {
  stop_old_sql_server
}

# init_min_repo seeds the schema and commits the tests rely on. It writes via $DOLT_OLD_BIN
# so the on-disk format matches what the sql-server, run from the same binary, will read.
init_min_repo() {
  "$DOLT_OLD_BIN" init >/dev/null
  "$DOLT_OLD_BIN" sql <<SQL
CREATE TABLE big (pk INT PRIMARY KEY, str LONGTEXT);
CREATE TABLE def (i INT);
INSERT INTO def VALUES (1), (2), (3);
SQL
  "$DOLT_OLD_BIN" add .
  "$DOLT_OLD_BIN" commit -m "initialized data" >/dev/null

  "$DOLT_OLD_BIN" sql -q "INSERT INTO big VALUES (1, 'hello');"
  "$DOLT_OLD_BIN" add .
  "$DOLT_OLD_BIN" commit -m "made changes to main" >/dev/null

  "$DOLT_OLD_BIN" branch check_merge
  "$DOLT_OLD_BIN" checkout check_merge >/dev/null
  "$DOLT_OLD_BIN" sql -q "INSERT INTO def VALUES (4);"
  "$DOLT_OLD_BIN" add .
  "$DOLT_OLD_BIN" commit -m "check_merge: extend def" >/dev/null
  "$DOLT_OLD_BIN" checkout main >/dev/null
}

@test "server-cli-compat: client connects and runs SELECT 1" {
  run new_dolt_cli sql -r csv -q "SELECT 1 AS test;"
  [ "$status" -eq 0 ]
  [[ "${lines[0]}" == "test" ]] || false
  [[ "${lines[1]}" == "1" ]] || false
}

@test "server-cli-compat: client reads commit history via dolt_log" {
  skip_if_old_lte "1.54.0" "dolt_log requires commit_order for deterministic ordering"

  run new_dolt_cli sql -r csv -q "SELECT committer, email, message FROM dolt_log ORDER BY commit_order LIMIT 3;"
  [ "$status" -eq 0 ]
  [[ "${lines[0]}" == "committer,email,message" ]] || false
  [[ "${lines[1]}" == "Bats Tests,bats@email.fake,Initialize data repository" ]] || false
  [[ "${lines[2]}" == "Bats Tests,bats@email.fake,initialized data" ]] || false
  [[ "${lines[3]}" == "Bats Tests,bats@email.fake,made changes to main" ]] || false
}

@test "server-cli-compat: commit and revert via client" {
  skip_if_old_lte "1.86.0" "DOLT_REVERT response columns and commit/revert info printing require dolt_log's commit_order column"
  skip_if_old_lte "1.86.6" "new client requires the fixed dolt_log schema"

  run new_dolt_cli sql -q "INSERT INTO big VALUES (10000, 'test commit');"
  [ "$status" -eq 0 ]

  run new_dolt_cli add -A
  [ "$status" -eq 0 ]

  run new_dolt_cli commit -m "test commit message"
  [ "$status" -eq 0 ]
  commit_output=$(strip_ansi "$output")
  [[ "$commit_output" =~ "test commit message" ]] || false
  [[ "$commit_output" =~ "commit " ]] || false

  commit_to_revert=$(extract_commit_hash "$output")
  [[ -n "$commit_to_revert" ]] || false

  run new_dolt_cli revert "$commit_to_revert"
  [ "$status" -eq 0 ]
  revert_output=$(strip_ansi "$output")
  [[ "$revert_output" =~ "commit " ]] || false

  run new_dolt_cli sql -r csv -q "SELECT count(*) AS n FROM big WHERE pk = 10000;"
  [ "$status" -eq 0 ]
  [[ "${lines[0]}" == "n" ]] || false
  [[ "${lines[1]}" == "0" ]] || false
}

@test "server-cli-compat: --author override surfaces through committer" {
  skip_if_old_lte "1.44.2" "servers reject writes via --use-db with 'database is read only'"

  run new_dolt_cli sql -q "INSERT INTO big VALUES (10001, 'author committer test');"
  [ "$status" -eq 0 ]
  run new_dolt_cli add -A
  [ "$status" -eq 0 ]
  run new_dolt_cli commit --author "Explicit Author <explicit@author.com>" -m "author committer compat test"
  [ "$status" -eq 0 ]

  run old_dolt sql -r csv -q "SELECT committer, email FROM dolt_log WHERE message = 'author committer compat test' LIMIT 1;"
  [ "$status" -eq 0 ]
  [[ "${lines[0]}" == "committer,email" ]] || false
  [[ "${lines[1]}" == "Explicit Author,explicit@author.com" ]] || false
}

@test "server-cli-compat: cherry-pick via client preserves author and applies row" {
  skip_if_old_lte "1.54.0" "dolt log and cherry-pick query the commit_order column in dolt_log"
  skip_if_new_lte "1.44.2" "client cannot read the server's storage format and reports an unknown record field tag"
  skip_if_old_lte "1.86.6" "new client requires the fixed dolt_log schema"

  # check_merge only modifies the def table, avoiding conflicts with other branches.
  run new_dolt_cli log check_merge -n 1
  [ "$status" -eq 0 ]
  cherry_commit=$(extract_commit_hash "$output")
  [[ -n "$cherry_commit" ]] || false

  # init_min_repo seeds def with rows (1,2,3), so the pre-cherry-pick count is exactly 3.
  run new_dolt_cli sql -r csv -q "SELECT count(*) AS n FROM def;"
  [ "$status" -eq 0 ]
  [[ "${lines[0]}" == "n" ]] || false
  [[ "${lines[1]}" == "3" ]] || false

  run new_dolt_cli cherry-pick "$cherry_commit"
  [ "$status" -eq 0 ]
  cherry_output=$(strip_ansi "$output")
  [[ "$cherry_output" =~ "commit " ]] || false

  # check_merge inserts (4), so the cherry-pick brings the count to 4.
  run new_dolt_cli sql -r csv -q "SELECT count(*) AS n FROM def;"
  [ "$status" -eq 0 ]
  [[ "${lines[0]}" == "n" ]] || false
  [[ "${lines[1]}" == "4" ]] || false
}
