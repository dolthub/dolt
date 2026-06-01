#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

# Tests the basic functionality of the dolt_update_column_tag stored procedure.
#
# Note that we use BATS to test this, since reading column tags is not supported
# via a SQL interface, only from the `dolt schema tags` command currently,
# otherwise we'd prefer enginetests in go.
@test "sql-update-column-tag: update column tag" {
    dolt sql -q "create table t1 (pk int primary key, c1 int);"

    run dolt schema tags
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "c1" ]] || false
    [[ ! "$output" =~ " t1    | pk     | 42 " ]] || false
    [[ ! "$output" =~ " t1    | c1     | 420 " ]] || false

    dolt sql -q "call dolt_update_column_tag('t1', 'pk', 42);"
    dolt sql -q "call dolt_update_column_tag('t1', 'c1', 420);"

    run dolt schema tags
    [ "$status" -eq 0 ]
    [[ "$output" =~ " t1    | pk     | 42 " ]] || false
    [[ "$output" =~ " t1    | c1     | 420 " ]] || false
}

# Tests error cases for the dolt_update_column_tag stored procedure.
@test "sql-update-column-tag: error cases" {
    dolt sql -q "create table t1 (pk int primary key, c1 int);"

    # invalid arg count
    run dolt sql -q "call dolt_update_column_tag();"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "incorrect number of arguments" ]] || false

    run dolt sql -q "call dolt_update_column_tag('t1', 'pk');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "incorrect number of arguments" ]] || false

    run dolt sql -q "call dolt_update_column_tag('t1', 'pk', 42, 'zzz');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Expected at most 3" ]] || false

    # invalid table
    run dolt sql -q "call dolt_update_column_tag('doesnotexist', 'pk', 42);"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "does not exist" ]] || false

    # invalid column
    run dolt sql -q "call dolt_update_column_tag('t1', 'doesnotexist', 42);"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "does not exist" ]] || false

    # invalid tag
    run dolt sql -q "call dolt_update_column_tag('t1', 'pk', 'not an integer');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "failed to parse tag" ]] || false
}

@test "sql-update-column-tag: preserves indexes, check constraints, comment, and multi-column primary key" {
    # See https://github.com/dolthub/dolt/issues/11007
    dolt sql <<SQL
CREATE TABLE accounts (
    id INT NOT NULL,
    region VARCHAR(8) NOT NULL,
    email VARCHAR(255) NOT NULL,
    PRIMARY KEY (region, id),
    UNIQUE KEY email_unique (email),
    KEY id_idx (id),
    CONSTRAINT email_format CHECK (email LIKE '%@%')
);
SQL
    dolt sql -q "ALTER TABLE accounts COMMENT='application accounts'" 2>/dev/null || true

    dolt sql -q "call dolt_update_column_tag('accounts', 'id', 999);"
    dolt sql -q "call dolt_update_column_tag('accounts', 'email', 888);"

    run dolt sql -q "show create table accounts"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "PRIMARY KEY (\`region\`,\`id\`)" ]] || false
    [[ "$output" =~ "UNIQUE KEY \`email_unique\`" ]] || false
    [[ "$output" =~ "KEY \`id_idx\`" ]] || false
    [[ "$output" =~ "CONSTRAINT \`email_format\` CHECK" ]] || false
    [[ "$output" =~ "COMMENT='application accounts'" ]] || false

    run dolt schema tags accounts
    [ "$status" -eq 0 ]
    [[ "$output" =~ "999" ]] || false
    [[ "$output" =~ "888" ]] || false

    run dolt sql -q "INSERT INTO accounts VALUES (1, 'us', 'no-at-sign')"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Check constraint" ]] || false

    dolt sql -q "INSERT INTO accounts VALUES (1, 'us', 'alice@example.com')"
    run dolt sql -q "INSERT INTO accounts VALUES (2, 'eu', 'alice@example.com')"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "duplicate" ]] || false

    dolt sql -q "INSERT INTO accounts VALUES (1, 'eu', 'bob@example.com')"

    run dolt sql -r csv -q "SELECT region, id FROM accounts ORDER BY region, id"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "eu,1" ]] || false
    [[ "$output" =~ "us,1" ]] || false
}
