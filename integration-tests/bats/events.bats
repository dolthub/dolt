#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_test_repo_and_start_server() {
  rm -rf ./"$1"
  mkdir "$1"
  cd "$1"
  start_sql_server
  dolt sql-client -P $PORT -u dolt --use-db information_schema -q "CREATE DATABASE repo1;"
  dolt sql-client -P $PORT -u dolt --use-db repo1 -q "CREATE TABLE totals (id int PRIMARY KEY AUTO_INCREMENT, int_col int);"
  dolt sql-client -P $PORT -u dolt --use-db repo1 -q "call dolt_commit('-Am', 'creating table');"
}

setup() {
    skiponwindows "tests are flaky on Windows"
    setup_no_dolt_init
    make_test_repo_and_start_server repo1
}

teardown() {
    stop_sql_server 1 && sleep 0.5
    teardown_common
}

@test "events: disabling recurring event should not be dropped" {
    dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CREATE EVENT insert1 ON SCHEDULE EVERY 1 DAY DO INSERT INTO totals (int_col) VALUES (1);"
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "ALTER EVENT insert1 DISABLE; SELECT * FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "DISABLED" ]] || false
}

@test "events: disabling current_timestamp one time event after execution" {
    dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CREATE EVENT insert9 ON SCHEDULE AT CURRENT_TIMESTAMP DO INSERT INTO totals (int_col) VALUES (9);"
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SELECT COUNT(*) FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1        |" ]] || false

    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SELECT COUNT(*) FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "| 0        |" ]] || false

    dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CREATE EVENT insert8 ON SCHEDULE AT CURRENT_TIMESTAMP ON COMPLETION PRESERVE DO INSERT INTO totals (int_col) VALUES (8);"
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SELECT COUNT(*) FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 2        |" ]] || false

    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SHOW CREATE EVENT insert8;"
    [ $status -eq 0 ]
    [[ $output =~ "ON COMPLETION PRESERVE DISABLE" ]] || false

    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "ALTER EVENT insert8 ON COMPLETION NOT PRESERVE; SELECT COUNT(*) FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1        |" ]] || false

    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "ALTER EVENT insert8 ENABLE; SELECT COUNT(*) FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "| 0        |" ]] || false
}

@test "events: disabling future one time event after execution from the scheduler" {
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CREATE EVENT insert9 ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 3 SECOND ON COMPLETION PRESERVE DO INSERT INTO totals (int_col) VALUES (9); SHOW CREATE EVENT insert9;"
    [ $status -eq 0 ]
    [[ $output =~ "ON COMPLETION PRESERVE ENABLE" ]] || false

    sleep 4
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SELECT COUNT(*) FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1        |" ]] || false

    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SELECT COUNT(*) FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1        |" ]] || false

    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SHOW CREATE EVENT insert9;"
    [ $status -eq 0 ]
    [[ $output =~ "ON COMPLETION PRESERVE DISABLE" ]] || false
}

@test "events: recurring event with STARTS and ENDS defined" {
    dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CREATE EVENT insert1 ON SCHEDULE EVERY 2 SECOND STARTS CURRENT_TIMESTAMP + INTERVAL 2 SECOND ENDS CURRENT_TIMESTAMP + INTERVAL 5 SECOND DO INSERT INTO totals (int_col) VALUES (1);"
    sleep 10
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SELECT COUNT(*) FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 2        |" ]] || false

    # should be dropped
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SELECT COUNT(*) FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "| 0        |" ]] || false
}

@test "events: recurring event with ENDS defined" {
    dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CREATE EVENT insert1 ON SCHEDULE EVERY 2 SECOND ENDS CURRENT_TIMESTAMP + INTERVAL 3 SECOND ON COMPLETION PRESERVE DO INSERT INTO totals (int_col) VALUES (1); SELECT SLEEP(5);"
    sleep 2
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SELECT COUNT(*) FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 2        |" ]] || false

    # should be disabled
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SELECT * FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "DISABLED" ]] || false
}

@test "events: checking out a branch should disable all events leaving the working set dirty" {
    dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CREATE EVENT insert1 ON SCHEDULE EVERY 2 SECOND ENDS CURRENT_TIMESTAMP + INTERVAL 3 SECOND ON COMPLETION PRESERVE DO INSERT INTO totals (int_col) VALUES (1); SELECT SLEEP(5);"
    dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CALL DOLT_COMMIT('-am','commit event changes to totals table')"

    dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CALL DOLT_CHECKOUT('-b','newbranch')"
    # should be disabled
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SELECT * FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "DISABLED" ]] || false
}

@test "events: events on default branch still run after switching to non-default branch" {
    dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CREATE EVENT insert1 ON SCHEDULE EVERY 3 SECOND ENDS CURRENT_TIMESTAMP + INTERVAL 3 MINUTE DO INSERT INTO totals (int_col) VALUES (1);"
    dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CALL DOLT_COMMIT('-Am','commit with an event');"

    dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CALL DOLT_CHECKOUT('-b','newbranch');"
    # should be disabled
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SELECT COUNT(*) FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1        |" ]] || false

    # While we are sleeping, the event executor should still be running our event on schedule, on the main branch
    sleep 5
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "CALL DOLT_CHECKOUT('main'); SELECT COUNT(*) FROM totals;"
    [ $status -eq 0 ]
    [[ ! $output =~ "| 1        |" ]] || false
}

# Test that events with multiple statements in nested BEGIN/END blocks work correctly
@test "events: multiple statements in nested BEGIN END blocks in event body" {
    # Use dolt sql to pipe in a HEREDOC; Note that this will connect to the running sql-server
    cd repo1
    dolt sql << SQL
delimiter //
CREATE EVENT event1234
ON SCHEDULE AT CURRENT_TIMESTAMP
DO
BEGIN
INSERT INTO totals (int_col) VALUES (111);
BEGIN
INSERT INTO totals (int_col) VALUES (222);
INSERT INTO totals (int_col) VALUES (333);
END;
END;
//
delimiter ;
SQL

    # Verify that our event ran correctly and inserted three rows
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SELECT * FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1  | 111     |" ]] || false
    [[ $output =~ "| 2  | 222     |" ]] || false
    [[ $output =~ "| 3  | 333     |" ]] || false

    # Verify that the event did not persist after execution
    run dolt sql-client -P $PORT -u dolt --use-db 'repo1' -q "SELECT COUNT(*) FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "| 0        |" ]] || false
}
