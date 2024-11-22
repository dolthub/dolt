#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

# This test suite was created to validate that CLI commands work against a hosted instance of Dolt. It is not a full
# test suite, but really comes from the fact that we added the  --host, --port, and --no-tls flags for this specific
# purpose, and otherwise the CLI commands should work the same as against any server instance. The tests for those
# features exist in many other places.
#
# Honestly, this test was created in haste, and because multiple parts of the default setup infrastructure don't work
# for this use case.

setup() {
  setup_no_dolt_init

  mkdir emptyDB
  cd emptyDB

  # Unusual situation - this is the only test that I know of where we want to start a server completely
  # empty - no `dolt init` at all. Reason for this is testing `dolt sql -q "create database bats_test_cli_hosted"`
  # we also to test credentials, I decided to start the server with a password, which is also different. So the server
  # creation is done here. We may want to move this to helper/query-server-common.bash later.
  PORT=$( definePORT )
  DOLT_CLI_PASSWORD="d01t"
  dolt sql-server --host 0.0.0.0 --port=$PORT --user="dolt" --password=$DOLT_CLI_PASSWORD --socket "dolt.$PORT.sock" &
  SERVER_PID=$!

  # Also, wait_for_connection code is pulled in here and replaced with a use of `dolt sql` instead. This
  # doesn't have the timeout option yet, which is a reason for not updating the original code. We do have one last difference
  # in that we need to wait for connection to work without any server - which the existing code doesn't allow for.
  timeout=7500
  user="dolt"
  end_time=$((SECONDS+($timeout/1000)))

  while [ $SECONDS -lt $end_time ]; do
    run dolt --no-tls --host localhost --port $PORT -u "dolt" sql -q "SELECT 1;"
    if [ $status -eq 0 ]; then
      echo "Connected successfully!"
      break
    fi
    sleep 1
  done

  export  HST=localhost
  export  PRT=$PORT
  export  DOLT_CLI_USER="dolt"
  export  TLS="--no-tls"

# If you want to run this against your hosted instance, uncomment the following lines and fill in the values.
#  export HST=dolthub-macneale-2.dbs.hosteddev.ld-corp.com
#  export PRT=3306
#  export DOLT_CLI_USER="d9e4d4y1e696st4z"
#  export DOLT_CLI_PASSWORD="****************"
#  export TLS=

  # Tests should not run anywhere that they may happen upon a sql-server.lock file.
  mkdir nowhere
  cd nowhere
}

teardown() {
    stop_sql_server
    teardown_common
}


@test "cli-hosted: dolt diff" {
  dolt $TLS --host $HST --port $PRT sql -q "create database bats_test_cli_hosted"

  dolt $TLS --host $HST --port $PRT --use-db bats_test_cli_hosted sql -q "create table t1 (a int primary key, b int, c varchar(255))"

  dolt $TLS --host $HST --port $PRT --use-db bats_test_cli_hosted commit -A -m "create t1"

  dolt $TLS --host $HST --port $PRT --use-db bats_test_cli_hosted sql -q "insert into t1 values (1, 42, 'forty-two')"
  dolt $TLS --host $HST --port $PRT --use-db bats_test_cli_hosted sql -q "insert into t1 values (2, 7, 'seven')"

  run dolt $TLS --host $HST --port $PRT --use-db bats_test_cli_hosted diff
  [ "$status" -eq 0 ]
  [[ $output =~ "|   | a | b  | c         |" ]] || false
  [[ $output =~ "+---+---+----+-----------+" ]] || false
  [[ $output =~ "| + | 1 | 42 | forty-two |" ]] || false
  [[ $output =~ "| + | 2 | 7  | seven     |" ]] || false

  dolt $TLS --host $HST --port $PRT --use-db bats_test_cli_hosted commit -A -m "add two rows to t1"

  dolt $TLS --host $HST --port $PRT sql -q "drop database bats_test_cli_hosted"
}

@test "cli-hosted: bogus user rejected" {
  run dolt $TLS --host $HST --port $PRT --user bogus sql -q "create database bats_test_cli_hosted"
  [ "$status" -eq 1 ]
  [[ $output =~ "No authentication" ]] || false
}

# This test will not work if you change DOLT_CLI_USER above. Be aware if you run against a hosted instance.
@test "cli-hosted: bogus password rejected" {
  run dolt $TLS --host $HST --port $PRT --user dolt --password bogus sql -q "create database bats_test_cli_hosted"
  [ "$status" -eq 1 ]
  [[ $output =~ "Access denied for user '" ]] || false
}
