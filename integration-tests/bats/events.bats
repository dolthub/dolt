#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_test_repo_and_start_server() {
  rm -rf ./"$1"
  mkdir "$1"
  cd "$1"

  # Override the default event scheduler period (30s) and set it to 1s so that we can run
  # tests faster, without having to wait for the default 30s period to elapse several times.
  export DOLT_EVENT_SCHEDULER_PERIOD=1
  start_sql_server

  dolt --port $PORT --host 0.0.0.0 --no-tls --use-db information_schema sql -q "CREATE DATABASE repo1;"
  dolt --port $PORT --host 0.0.0.0 --no-tls --use-db repo1 sql -q "CREATE TABLE totals (id int PRIMARY KEY AUTO_INCREMENT, int_col int);"
  dolt --port $PORT --host 0.0.0.0 --no-tls --use-db repo1 sql -q "call dolt_commit('-Am', 'creating table');"
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
    dolt sql -q "CREATE EVENT insert1 ON SCHEDULE EVERY 1 DAY DO INSERT INTO totals (int_col) VALUES (1);"
    run dolt sql -q "ALTER EVENT insert1 DISABLE; SELECT * FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "DISABLED" ]] || false
}

@test "events: disabling current_timestamp one time event after execution" {
    dolt sql -q "CREATE EVENT insert9 ON SCHEDULE AT CURRENT_TIMESTAMP DO INSERT INTO totals (int_col) VALUES (9);"
    # used for debugging
    dolt sql -q "SELECT COUNT(*) FROM totals;"
    run dolt sql -q "SELECT COUNT(*) FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1        |" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "| 0        |" ]] || false

    dolt sql -q "CREATE EVENT insert8 ON SCHEDULE AT CURRENT_TIMESTAMP ON COMPLETION PRESERVE DO INSERT INTO totals (int_col) VALUES (8);"
    # used for debugging
    dolt sql -q "SELECT COUNT(*) FROM totals;"
    run dolt sql -q "SELECT COUNT(*) FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 2        |" ]] || false

    run dolt sql -q "SHOW CREATE EVENT insert8;"
    [ $status -eq 0 ]
    [[ $output =~ "ON COMPLETION PRESERVE DISABLE" ]] || false

    run dolt sql -q "ALTER EVENT insert8 ON COMPLETION NOT PRESERVE; SELECT COUNT(*) FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1        |" ]] || false

    run dolt sql -q "ALTER EVENT insert8 ENABLE; SELECT COUNT(*) FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "| 0        |" ]] || false
}

@test "events: disabling future one time event after execution from the scheduler" {
    run dolt sql -q "CREATE EVENT insert9 ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 3 SECOND ON COMPLETION PRESERVE DO INSERT INTO totals (int_col) VALUES (9); SHOW CREATE EVENT insert9;"
    [ $status -eq 0 ]
    [[ $output =~ "ON COMPLETION PRESERVE ENABLE" ]] || false

    sleep 4
    # used for debugging
    dolt sql -q "SELECT COUNT(*) FROM totals;"

    run dolt sql -q "SELECT COUNT(*) >= 1 FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1             |" ]] || false

    run dolt sql -q "SELECT COUNT(*) FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1        |" ]] || false

    run dolt sql -q "SHOW CREATE EVENT insert9;"
    [ $status -eq 0 ]
    [[ $output =~ "ON COMPLETION PRESERVE DISABLE" ]] || false
}

@test "events: recurring event with STARTS and ENDS defined" {
    dolt sql -q "CREATE EVENT insert1 ON SCHEDULE EVERY 2 SECOND STARTS CURRENT_TIMESTAMP + INTERVAL 2 SECOND ENDS CURRENT_TIMESTAMP + INTERVAL 5 SECOND DO INSERT INTO totals (int_col) VALUES (1);"
    sleep 10

    # used for debugging
    dolt sql -q "SELECT COUNT(*) FROM totals;"

    run dolt sql -q "SELECT COUNT(*) >= 2 FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1             |" ]] || false

    # should be dropped
    run dolt sql -q "SELECT COUNT(*) FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "| 0        |" ]] || false
}

@test "events: recurring event with ENDS defined" {
    dolt sql -q "CREATE EVENT insert1 ON SCHEDULE EVERY 2 SECOND ENDS CURRENT_TIMESTAMP + INTERVAL 3 SECOND ON COMPLETION PRESERVE DO INSERT INTO totals (int_col) VALUES (1); SELECT SLEEP(5);"
    sleep 3

    # used for debugging
    dolt sql -q "SELECT COUNT(*) FROM totals;"
    run dolt sql -q "SELECT COUNT(*) FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 2        |" ]] || false

    # should be disabled
    run dolt sql -q "SELECT * FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "DISABLED" ]] || false
}

@test "events: checking out a branch should disable all events leaving the working set dirty" {
    dolt sql -q "CREATE EVENT insert1 ON SCHEDULE EVERY 2 SECOND ENDS CURRENT_TIMESTAMP + INTERVAL 3 SECOND ON COMPLETION PRESERVE DO INSERT INTO totals (int_col) VALUES (1); SELECT SLEEP(5);"
    dolt sql -q "CALL DOLT_COMMIT('-am','commit event changes to totals table')"

    dolt sql -q "CALL DOLT_CHECKOUT('-b','newbranch')"
    # should be disabled
    run dolt sql -q "SELECT * FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "DISABLED" ]] || false
}

@test "events: events on default branch still run after switching to non-default branch" {
    dolt sql -q "CREATE EVENT insert1 ON SCHEDULE EVERY 3 SECOND ENDS CURRENT_TIMESTAMP + INTERVAL 3 MINUTE DO INSERT INTO totals (int_col) VALUES (1);"
    dolt sql -q "CALL DOLT_COMMIT('-Am','commit with an event');"

    dolt sql -q "CALL DOLT_CHECKOUT('-b','newbranch');"
    # should be disabled
    run dolt sql -q "SELECT COUNT(*) FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1        |" ]] || false

    # While we are sleeping, the event executor should still be running our event on schedule, on the main branch
    sleep 5
    run dolt sql -q "CALL DOLT_CHECKOUT('main'); SELECT COUNT(*) FROM totals;"
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
    run dolt sql -q "SELECT * FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1  | 111     |" ]] || false
    [[ $output =~ "| 2  | 222     |" ]] || false
    [[ $output =~ "| 3  | 333     |" ]] || false

    # Verify that the event did not persist after execution
    run dolt sql -q "SELECT COUNT(*) FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "| 0        |" ]] || false
}

# Test that events containing procedure calls work correctly
@test "events: procedure calls in events" {
    # Create a procedure
    cd repo1
    dolt sql << SQL
DELIMITER //
CREATE PROCEDURE InsertIntoTotals()
BEGIN
  INSERT INTO totals (int_col) VALUES (42);
END //
DELIMITER ;
SQL

    # Use dolt sql to pipe in a HEREDOC; Note that this will connect to the running sql-server
    dolt sql << SQL
delimiter //
CREATE EVENT event1234
ON SCHEDULE AT CURRENT_TIMESTAMP
DO
BEGIN
  CALL InsertIntoTotals();
END;
//
delimiter ;
SQL

    # Verify that our event ran correctly and inserted one row
    run dolt sql -q "SELECT * FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1  | 42      |" ]] || false
    run dolt sql -q "SELECT COUNT(*) FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1  " ]] || false

    # Verify that the event did not persist after execution
    run dolt sql -q "SELECT COUNT(*) FROM information_schema.events;"
    [ $status -eq 0 ]
    [[ $output =~ "| 0        |" ]] || false
}

# Test that out-of-band event definition changes (e.g. merges, reverts, or anything else that doesn't go through
# CREATE EVENT statements) are reflected correctly.
@test "events: out-of-band event changes are detected" {
    # Use dolt sql to pipe in a HEREDOC; Note that this will connect to the running sql-server
    dolt sql << SQL
call dolt_checkout('-b', 'other');
CREATE EVENT event12345
ON SCHEDULE EVERY 1 SECOND STARTS CURRENT_TIMESTAMP
DO INSERT INTO totals (int_col) VALUES (42);
call dolt_commit('-Am', 'Adding a new recurring event');
SQL

    # Verify that our event IS NOT executing (on a non-main branch)
    sleep 1
    run dolt sql -q "SELECT COUNT(*) FROM totals;"
    [ $status -eq 0 ]
    [[ $output =~ "| 0  " ]] || false

    # Merge our event from other back to main and enable it
    dolt sql << SQL
call dolt_checkout('main');
call dolt_merge('other');
ALTER EVENT event12345 ENABLE;
call dolt_commit('-am', 'committing enabled event');
SQL

    # Verify that the new event starts executing on main after we merge it over
    sleep 2
    run dolt sql -q "SELECT (SELECT COUNT(*) FROM totals) > 0;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1  " ]] || false
}

@test "events: restarting a sql-server correctly schedules existing events" {
    # Create the recurring event and verify that it's enabled
    dolt sql -q "CREATE EVENT eventTest1 ON SCHEDULE EVERY 1 SECOND STARTS '2020-02-20 00:00:00' DO INSERT INTO totals (int_col) VALUES (111);"
    run dolt sql -q "SHOW EVENTS"
    [ $status -eq 0 ]
    [[ $output =~ '| repo1 | eventTest1 | `__dolt_local_user__`@`localhost` | SYSTEM    | RECURRING | NULL       | 1              | SECOND         | 2020-02-20 00:00:00 | NULL | ENABLED | 0          | utf8mb4              | utf8mb4_0900_bin     | utf8mb4_0900_bin   |' ]] || false

    # Sleep for a few seconds to give the scheduler time to run this event and verify that it executed
    sleep 2
    run dolt sql -q "SELECT (SELECT COUNT(*) FROM totals) > 0;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1  " ]] || false

    # Verify that the event is still enabled
    run dolt sql -q "SHOW EVENTS"
    [ $status -eq 0 ]
    [[ $output =~ '| repo1 | eventTest1 | `__dolt_local_user__`@`localhost` | SYSTEM    | RECURRING | NULL       | 1              | SECOND         | 2020-02-20 00:00:00 | NULL | ENABLED | 0          | utf8mb4              | utf8mb4_0900_bin     | utf8mb4_0900_bin   |' ]] || false

    # Stop the sql-server, truncate the totals table, and assert it's empty
    stop_sql_server 1
    dolt sql -q "truncate totals;"
    run dolt sql -q "SELECT (SELECT COUNT(*) FROM totals) > 0;"
    [ $status -eq 0 ]
    [[ $output =~ "| false  " ]] || false

    # Restart the server and assert that the event is still enabled
    start_sql_server
    run dolt sql -q "SHOW EVENTS;"
    [[ $output =~ '| repo1 | eventTest1 | `__dolt_local_user__`@`localhost` | SYSTEM    | RECURRING | NULL       | 1              | SECOND         | 2020-02-20 00:00:00 | NULL | ENABLED | 0          | utf8mb4              | utf8mb4_0900_bin     | utf8mb4_0900_bin   |' ]] || false

    # Sleep for a few seconds to give the scheduler time to run this event and verify that it is still enabled
    sleep 2
    run dolt sql -q "SHOW EVENTS"
    [ $status -eq 0 ]
    [[ $output =~ '| repo1 | eventTest1 | `__dolt_local_user__`@`localhost` | SYSTEM    | RECURRING | NULL       | 1              | SECOND         | 2020-02-20 00:00:00 | NULL | ENABLED | 0          | utf8mb4              | utf8mb4_0900_bin     | utf8mb4_0900_bin   |' ]] || false

    # Verify that the event executed and inserted a row in the totals table
    run dolt sql -q "SELECT (SELECT COUNT(*) FROM totals) > 0;"
    [ $status -eq 0 ]
    [[ $output =~ "| 1  " ]] || false
}
