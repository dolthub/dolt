#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test tests remote connections directly, SQL_ENGINE is not needed."
    fi
    setup_common
}

teardown() {
    stop_sql_server 1 && sleep 0.5
    teardown_common
}

@test "dolt-test-run: sanity test on sql-server" {
    start_sql_server

    dolt sql -q "insert into dolt_tests values ('test', 'test', 'select 1', 'expected_rows', '==', '1');"
    run dolt sql -q "select * from dolt_test_run()"
    [ $status -eq 0 ]
    [[ $output =~ "| test      | test            | select 1 | PASS   |         |" ]] || false
}
