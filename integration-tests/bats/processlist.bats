#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "processlist: kill query" {
    if [ "$SQL_ENGINE" != "remote-engine" ]; then
      skip "processlist is remote-engine specific"
    fi

    dolt sql -q "select sleep(1000)" &
    sleep 1

    run dolt sql -q "SHOW PROCESSLIST"
    [[ "$output" =~ "select sleep(1000)" ]] || false

    qpid=$(dolt sql -q "show processlist" | grep --text "select sleep(1000)" | cut -d"|" -f2)
    run dolt sql -q "kill query $qpid"
    sleep 1

    run dolt sql -q "SHOW PROCESSLIST"
    [[ ! "$output" =~ "select sleep(1000)" ]] || false
}