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
    [[ "$output" =~ "error: unknown backup 'bac1'" ]] || false
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

@test "sql-backup: dolt_backup restore invalid arguments" {
    # Not enough arguments
    run dolt sql -q "call dolt_backup('restore')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "usage: dolt_backup('restore', 'backup_url', 'database_name')" ]] || false

    # Not enough arguments
    run dolt sql -q "call dolt_backup('restore', 'file:///some_directory')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "usage: dolt_backup('restore', 'backup_url', 'database_name')" ]] || false

    # Too many arguments
    run dolt sql -q "call dolt_backup('restore', 'hostedapidb-0', 'file:///some_directory', 'too many')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "usage: dolt_backup('restore', 'backup_url', 'database_name')" ]] || false
}

@test "sql-backup: dolt_backup restore" {
    backupsDir="$PWD/backups"
    mkdir backupsDir

    # Created a nested database, back it up, drop it, then restore it with a new name
    dolt sql -q "create database db1;"
    cd db1
    dolt sql -q "create table t1 (pk int primary key); insert into t1 values (42); call dolt_commit('-Am', 'creating table t1');"
    dolt sql -q "call dolt_backup('add', 'backups', 'file://$backupsDir');"
    dolt sql -q "call dolt_backup('sync', 'backups');"
    cd ..
    dolt sql -q "drop database db1;"
    dolt sql -q "call dolt_backup('restore', 'file://$backupsDir', 'db2');"

    # Assert that db2 is present, and db1 is not
    run dolt sql -q "show databases;"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false

    # Sanity check some data in the database
    run dolt sql -q "use db2; select * from t1;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "42" ]] || false

    # Assert that db2 doesn't have any remotes
    cd db2
    run dolt remote -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

@test "sql-backup: dolt_backup restore --force" {
    backupsDir="$PWD/backups"
    mkdir backupsDir

    # Created a nested database, and back it up
    dolt sql -q "create database db1;"
    cd db1
    dolt sql -q "create table t1 (pk int primary key); insert into t1 values (42); call dolt_commit('-Am', 'creating table t1');"
    dolt sql -q "call dolt_backup('add', 'backups', 'file://$backupsDir');"
    dolt sql -q "call dolt_backup('sync', 'backups');"

    # Make a new commit in db1, but don't push it to the backup
    dolt sql -q "update t1 set pk=100; call dolt_commit('-Am', 'updating table t1');"

    # Assert that without --force, we can't update an existing db from a backup
    run dolt sql -q "call dolt_backup('restore', 'file://$backupsDir', 'db1');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot restore backup into db1. A database with that name already exists." ]] || false

    # Use --force to overwrite the existing database and sanity check the data
    run dolt sql -q "call dolt_backup('restore', '--force', 'file://$backupsDir', 'db1');"
    [ "$status" -eq 0 ]
    run dolt sql -q "use db1; select * from t1;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "42" ]] || false
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
