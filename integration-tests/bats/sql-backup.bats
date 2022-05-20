#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1
}

teardown() {
    teardown_common
}

@test "sql-backup: dolt_backup no argument" {
    run dolt sql -q "select dolt_backup()"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup()"
    [ "$status" -ne 0 ]
}

@test "sql-backup: dolt_backup add" {
    run dolt sql -q "select dolt_backup('add', 'hostedapidb-0', 'file:///some_directory')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('add', 'hostedapidb-0', 'file:///some_directory')"
    [ "$status" -ne 0 ]
}

@test "sql-backup: dolt_backup rm" {
    run dolt sql -q "select dolt_backup('rm', 'hostedapidb-0')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('rm', 'hostedapidb-0')"
    [ "$status" -ne 0 ]
    run dolt sql -q "select dolt_backup('remove', 'hostedapidb-0')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('remove', 'hostedapidb-0')"
    [ "$status" -ne 0 ]
}

@test "sql-backup: dolt_backup restore" {
    run dolt sql -q "select dolt_backup('restore', 'hostedapidb-0', 'file:///some_directory')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('restore', 'hostedapidb-0', 'file:///some_directory')"
    [ "$status" -ne 0 ]
    run dolt sql -q "select dolt_backup('restore', 'hostedapidb-0', 'file:///some_directory')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('restore', 'hostedapidb-0', 'file:///some_directory')"
    [ "$status" -ne 0 ]
}

@test "sql-backup: dolt_backup unrecognized" {
    run dolt sql -q "select dolt_backup('unregonized', 'hostedapidb-0', 'file:///some_directory')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('unrecognized', 'hostedapidb-0', 'file:///some_directory')"
    [ "$status" -ne 0 ]
}

@test "sql-backup: dolt_backup sync wrong number of args" {
    run dolt sql -q "select dolt_backup('sync')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('sync')"
    [ "$status" -ne 0 ]
    run dolt sql -q "select dolt_backup('sync', 'hostedapidb-0', 'too many')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('sync', 'hostedapidb-0', 'too many')"
    [ "$status" -ne 0 ]
}

@test "sql-backup: dolt_backup no such backup" {
    run dolt sql -q "select dolt_backup('sync', 'hostedapidb-0')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('sync', 'hostedapidb-0')"
    [ "$status" -ne 0 ]
}

@test "sql-backup: dolt_backup sync to a backup" {
    mkdir the_backup
    dolt backup add hostedapidb-0 file://./the_backup
    dolt backup -v
    dolt sql -q "select dolt_backup('sync', 'hostedapidb-0')"
    # Initial backup works.
    dolt backup restore file://./the_backup the_restore
    (cd the_restore && dolt status)
    # Backup with nothing to push works.
    dolt sql -q "select dolt_backup('sync', 'hostedapidb-0')"

    rm -rf the_backup the_restore

    mkdir the_backup
    dolt sql -q "CALL dolt_backup('sync', 'hostedapidb-0')"
    dolt backup restore file://./the_backup the_restore
    (cd the_restore && dolt status)
    dolt sql -q "CALL dolt_backup('sync', 'hostedapidb-0')"
}
