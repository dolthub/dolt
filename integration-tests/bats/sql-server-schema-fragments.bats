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
    stop_sql_server
    teardown_common
}

@test "sql-server-schema-fragments: views get updated tables when changing _head" {
    cd repo1
    dolt sql -q 'CREATE TABLE `values` (id int primary key);'
    dolt sql -q 'CREATE VIEW query_values AS SELECT * FROM `values` ORDER BY id ASC;'
    dolt add .
    dolt commit -m 'create values table'
    dolt checkout -b values_has_one
    dolt sql -q 'INSERT INTO `values` VALUES (1);'
    dolt add .
    dolt commit -m 'add 1 to values'
    dolt checkout main
    dolt checkout -b values_has_two
    dolt sql -q 'INSERT INTO `values` VALUES (2);'
    dolt add .
    dolt commit -m 'add 2 to values'
    dolt checkout main
    start_sql_server repo1

    server_query repo1 0 "SELECT * FROM query_values" ""
    server_query repo1 0 "
    SET @@repo1_head=hashof('values_has_one');
    SELECT * FROM query_values;
    SET @@repo1_head=hashof('values_has_two');
    SELECT * FROM query_values;
    SET @@repo1_head=hashof('main');
    SELECT * FROM query_values;
    " ";id\n1;;id\n2;;id"
}

@test "sql-server-schema-fragments: views at new head visible changing _head" {
    skip "this is currently incorrect in dolt"

    cd repo1
    dolt checkout -b no_view
    dolt checkout -b with_view
    dolt sql -q "CREATE VIEW a_view AS SELECT 47 FROM DUAL;"
    dolt add .
    dolt commit -m 'Create a view'
    dolt checkout no_view
    start_sql_server repo1

    server_query repo1 0 "
    SET @@repo1_head=hashof('with_view');
    SELECT * FROM a_view;
    " ";47\n47"
}

@test "sql-server-schema-fragments: views not at new head no longer visible when changing _head" {
    skip "this is currently incorrect in dolt"

    cd repo1
    dolt checkout -b no_view
    dolt checkout -b with_view
    dolt sql -q "CREATE VIEW a_view AS SELECT 47 FROM DUAL;"
    dolt add .
    dolt commit -m 'Create a view'
    start_sql_server repo1

    server_query repo1 0 "
    SET @@repo1_head=hashof('no_view');
    SELECT * FROM information_schema.views
    " ";"
}
