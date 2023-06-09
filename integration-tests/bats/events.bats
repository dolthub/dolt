#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir "$1"
  cd "$1"
  dolt init
  cd ..
}

setup() {
    skiponwindows "tests are flaky on Windows"
    setup_no_dolt_init
    make_repo repo1
    make_repo repo2
}

teardown() {
    stop_sql_server 1 && sleep 0.5
    rm -rf $BATS_TMPDIR/sql-server-test$$
    teardown_common
}

@test "events: simple insert into table event" {
    cd repo1
    dolt sql -q "CREATE TABLE totals (id int PRIMARY KEY AUTO_INCREMENT, int_col int)"

    start_sql_server
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CREATE EVENT insert1 ON SCHEDULE EVERY 5 SECOND DO INSERT INTO totals (int_col) VALUES (1); SELECT SLEEP(11); SELECT COUNT(*) FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 3        |" ]] || false
}

@test "events: disabling recurring event with ends not defined should not be dropped" {
    cd repo1
    dolt sql -q "CREATE TABLE totals (id int PRIMARY KEY AUTO_INCREMENT, int_col int)"

    start_sql_server
    dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CREATE EVENT insert1 ON SCHEDULE EVERY 1 DAY DO INSERT INTO totals (int_col) VALUES (1);"
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "ALTER EVENT insert1 DISABLE; SELECT * FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "DISABLED" ]] || false
}
