#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "sql-backup: dolt_backup no argument" {
    run dolt sql -q "call dolt_backup()"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup()"
    [ "$status" -ne 0 ]
}

@test "sql-backup: dolt_backup add" {
    mkdir the_backup
    run dolt sql -q "call dolt_backup('add', 'hostedapidb-0', 'file://./the_backup')"
    [ "$status" -eq 0 ]
    run dolt backup -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "the_backup" ]] || false
}

@test "sql-backup: dolt_backup add cannot add remote with address of existing backup" {
    mkdir bac1
    dolt sql -q "call dolt_backup('add','bac1','file://./bac1')"
    run dolt sql -q "call dolt_backup('add','rem1','file://./bac1')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "address conflict with a remote: 'bac1'" ]] || false
}

@test "sql-backup: dolt_backup add invalid https backup" {
    mkdir bac1
    run dolt sql -q "call dolt_backup('add', 'bac1', 'https://doltremoteapi.dolthub.com/Dolthub/non-existing-repo')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "sync-url does not support http or https backup locations currently" ]] || false
}

@test "sql-backup: dolt_backup remove" {
    mkdir bac1
    dolt sql -q "call dolt_backup('add', 'bac1', 'file://./bac1')"
    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "bac1" ]] || false

    dolt sql -q "call dolt_backup('remove','bac1')"
    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

@test "sql-backup: dolt_backup remove cannot remove non-existent backup" {
    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
    [[ ! "$output" =~ "bac1" ]] || false

    run dolt sql -q "call dolt_backup('remove','bac1')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: unknown backup: 'bac1'" ]] || false
}

@test "sql-backup: dolt_backup rm" {
    mkdir bac1
    dolt sql -q "call dolt_backup('add', 'bac1', 'file://./bac1')"
    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "bac1" ]] || false

    dolt sql -q "call dolt_backup('rm','bac1')"
    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
    [[ ! "$output" =~ "bac1" ]] || false
}

@test "sql-backup: dolt_backup restore" {
    run dolt sql -q "call dolt_backup('restore', 'hostedapidb-0', 'file:///some_directory')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('restore', 'hostedapidb-0', 'file:///some_directory')"
    [ "$status" -ne 0 ]
    run dolt sql -q "call dolt_backup('restore', 'hostedapidb-0', 'file:///some_directory')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('restore', 'hostedapidb-0', 'file:///some_directory')"
    [ "$status" -ne 0 ]
}

@test "sql-backup: dolt_backup unrecognized" {
    run dolt sql -q "call dolt_backup('unregonized', 'hostedapidb-0', 'file:///some_directory')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('unrecognized', 'hostedapidb-0', 'file:///some_directory')"
    [ "$status" -ne 0 ]
}

@test "sql-backup: dolt_backup sync wrong number of args" {
    run dolt sql -q "call dolt_backup('sync')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('sync')"
    [ "$status" -ne 0 ]
    run dolt sql -q "call dolt_backup('sync', 'hostedapidb-0', 'too many')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('sync', 'hostedapidb-0', 'too many')"
    [ "$status" -ne 0 ]
}

@test "sql-backup: dolt_backup no such backup" {
    run dolt sql -q "call dolt_backup('sync', 'hostedapidb-0')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('sync', 'hostedapidb-0')"
    [ "$status" -ne 0 ]
}

@test "sql-backup: dolt_backup sync to a backup" {
    mkdir the_backup
    dolt backup add hostedapidb-0 file://./the_backup
    dolt backup -v
    dolt sql -q "call dolt_backup('sync', 'hostedapidb-0')"
    # Initial backup works.
    dolt backup restore file://./the_backup the_restore
    (cd the_restore && dolt status)
    # Backup with nothing to push works.
    dolt sql -q "call dolt_backup('sync', 'hostedapidb-0')"

    rm -rf the_backup the_restore

    mkdir the_backup
    dolt sql -q "CALL dolt_backup('sync', 'hostedapidb-0')"
    dolt backup restore file://./the_backup the_restore
    (cd the_restore && dolt status)
    dolt sql -q "CALL dolt_backup('sync', 'hostedapidb-0')"
}

@test "sql-backup: dolt_backup sync-url" {
    mkdir the_backup
    dolt sql -q "call dolt_backup('sync-url', 'file://./the_backup')"
    # Initial backup works.
    dolt backup restore file://./the_backup the_restore
    (cd the_restore && dolt status)

    rm -rf the_backup the_restore

    mkdir the_backup
    dolt sql -q "CALL dolt_backup('sync-url', 'file://./the_backup')"
    dolt backup restore file://./the_backup the_restore
    (cd the_restore && dolt status)
}

@test "sql-backup: dolt_backup sync-url fails for http remotes" {
    run dolt sql -q "call dolt_backup('sync-url', 'http://dolthub.com/dolthub/backup')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('sync-url', 'https://dolthub.com/dolthub/backup')"
    [ "$status" -ne 0 ]
}
