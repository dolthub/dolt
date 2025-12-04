#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "sql-backup: dolt_backup no argument" {
    run dolt sql -q "call dolt_backup()"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "use 'dolt_backups' table to list backups" ]] || false
    run dolt sql -q "CALL dolt_backup()"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "use 'dolt_backups' table to list backups" ]] || false
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
    backupFileUrl="file://$BATS_TEST_TMPDIR/backup"
    dolt sql -q "call dolt_backup('add','bac1', '$backupFileUrl')"
    run dolt sql -q "call dolt_backup('add','rem1', '$backupFileUrl')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "address conflict with a remote: 'bac1'" ]] || false
}

@test "sql-backup: dolt_backup remove" {
    backupFileUrl="file://$BATS_TEST_TMPDIR/backup"
    dolt sql -q "call dolt_backup('add', 'bac1', '$backupFileUrl')"
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
    [[ "$output" =~ "backup 'bac1' not found" ]] || false
}

@test "sql-backup: dolt_backup rm" {
    backupFileUrl="files://$BATS_TEST_TMPDIR/backup"
    dolt sql -q "call dolt_backup('add', 'bac1', '$backupFileUrl')"
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
    [[ "$output" =~ "usage: dolt_backup('restore', 'remote_url', 'new_db_name', ['--force'], ['--aws-region=<region>'], ['--aws-creds-type=<type>'], ['--aws-creds-file=<file>'], ['--aws-creds-profile=<profile>'])" ]] || false

    # Not enough arguments
    run dolt sql -q "call dolt_backup('restore', 'file:///some_directory')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "usage: dolt_backup('restore', 'remote_url', 'new_db_name', ['--force'], ['--aws-region=<region>'], ['--aws-creds-type=<type>'], ['--aws-creds-file=<file>'], ['--aws-creds-profile=<profile>'])" ]] || false

    # Too many arguments
    run dolt sql -q "call dolt_backup('restore', 'hostedapidb-0', 'file:///some_directory', 'too many')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "usage: dolt_backup('restore', 'remote_url', 'new_db_name', ['--force'], ['--aws-region=<region>'], ['--aws-creds-type=<type>'], ['--aws-creds-file=<file>'], ['--aws-creds-profile=<profile>'])" ]] || false
}

@test "sql-backup: dolt_backup restore" {
    backupFileUrl="file://$BATS_TEST_TMPDIR/backups"
    mkdir backupsDir

    # Created a nested database, back it up, drop it, then restore it with a new name
    dolt sql -q "create database db1;"
    cd db1
    dolt sql -q "create table t1 (pk int primary key); insert into t1 values (42); call dolt_commit('-Am', 'creating table t1');"
    dolt sql -q "call dolt_backup('add', 'backups', '$backupFileUrl');"
    dolt sql -q "call dolt_backup('sync', 'backups');"
    cd ..
    dolt sql -q "drop database db1;"
    dolt sql -q "call dolt_backup('restore', '$backupFileUrl', 'db2');"

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

@test "sql-backup: dolt_backup restore --force on current database" {
    backupFileUrl="file://$BATS_TEST_TMPDIR/backups"

    # Created a nested database, and back it up
    dolt sql -q "create database db1;"
    # We could cd into db1 but Windows does not like us touching its CWD when we drop the database when restoring.
    dolt sql -q "use db1; create table t1 (pk int primary key); insert into t1 values (42); call dolt_commit('-Am', 'creating table t1');"
    dolt sql -q "use db1; call dolt_backup('add', 'backups', '$backupFileUrl');"
    dolt sql -q "use db1; call dolt_backup('sync', 'backups');"

    # Make a new commit in db1, but don't push it to the backup
    dolt sql -q "use db1; update t1 set pk=100; call dolt_commit('-Am', 'updating table t1');"

    # Assert that without --force, we can't update an existing db from a backup
    run dolt sql -q "use db1; call dolt_backup('restore', '$backupFileUrl', 'db1');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "database 'db1' already exists, use '--force' to overwrite" ]] || false

    # Use --force to overwrite the existing database and sanity check the data
    run dolt sql -q "use db1; call dolt_backup('restore', '--force', '$backupFileUrl', 'db1');"
    [ "$status" -eq 0 ]
    run dolt sql -q "use db1; select * from t1;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "42" ]] || false
}

@test "sql-backup: dolt_backup restore --force on current database and as cwd" {
    skiponwindows "Windows storage system locks the terminal cwd when trying to drop database in restore procedure; this includes mounted storage in WSL"
    backupFileUrl="file://$BATS_TEST_TMPDIR/backup"
    dolt sql -q "create database db1;"
    cd db1
    dolt sql <<EOF
create table t (i int);
insert into t values (3), (4);
call dolt_backup('sync-url', '$backupFileUrl');
EOF

    run dolt sql -q "call dolt_backup('restore', '$backupFileUrl', 'db1');"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "database 'db1' already exists, use '--force' to overwrite" ]] || false

    run dolt sql -q "call dolt_backup('restore', '--force', '$backupFileUrl', 'db1');"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from t;"
    [[ "$output" =~ i.*3.*4 ]] || false
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
    backupFileUrl="file://$BATS_TEST_TMPDIR/the_backup"
    dolt backup add hostedapidb-0 "$backupFileUrl"
    dolt backup -v
    dolt sql -q "call dolt_backup('sync', 'hostedapidb-0')"
    # Initial backup works.
    dolt backup restore "$backupFileUrl" the_restore
    (cd the_restore && dolt status)
    # Backup with nothing to push works.
    dolt sql -q "call dolt_backup('sync', 'hostedapidb-0')"

    dolt sql -q "CALL dolt_backup('sync', 'hostedapidb-0')"
    dolt backup restore "$backupFileUrl" the_restore --force
    (cd the_restore && dolt status)
    dolt sql -q "CALL dolt_backup('sync', 'hostedapidb-0')"
}

@test "sql-backup: dolt_backup sync-url" {
    backupFileUrl="file://$BATS_TEST_TMPDIR/the_backup"
    dolt sql -q "call dolt_backup('sync-url', '$backupFileUrl')"
    # Initial backup works.
    dolt backup restore "$backupFileUrl" the_restore
    (cd the_restore && dolt status)

    dolt sql -q "CALL dolt_backup('sync-url', '$backupFileUrl')"
    dolt backup restore "$backupFileUrl" the_restore --force
    (cd the_restore && dolt status)
}

@test "sql-backup: dolt_backup sync-url fails on non-grpc http request" {
    run dolt sql -q "call dolt_backup('sync-url', 'http://dolthub.com/dolthub/backup')"
    [ "$status" -ne 0 ]
    run dolt sql -q "CALL dolt_backup('sync-url', 'https://dolthub.com/dolthub/backup')"
    [ "$status" -ne 0 ]
}

@test "sql-backup: dolt_backup rejects AWS parameters fails in sql-server" {
    skip_if_remote
    start_sql_server

    run dolt sql -q "call dolt_backup('add', 'backup1', 'aws://[table:bucket]/db', '--aws-region=us-east-1')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "AWS parameters are unavailable when running in server mode" ]] || false

    run dolt sql -q "call dolt_backup('sync-url', 'backup2', 'aws://[table:bucket]/db', '--aws-creds-type=file')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "AWS parameters are unavailable when running in server mode" ]] || false

    run dolt sql -q "call dolt_backup('restore', 'backup3', 'aws://[table:bucket]/db', '--aws-creds-file=/path/to/file')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "AWS parameters are unavailable when running in server mode" ]] || false

    run dolt sql -q "call dolt_backup('add', 'backup4', 'aws://[table:bucket]/db', '--aws-creds-profile=profile')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "AWS parameters are unavailable when running in server mode" ]] || false

    stop_sql_server 1
}

@test "sql-backup: dolt_backup works in invalid dolt repository" {
    backupFileUrl="file://$BATS_TEST_TMPDIR/t_backup"
    run dolt sql -q "create table t (i int);"
    [ "$status" -eq 0 ]
    run dolt sql -q "insert into t values (4), (3);"
    [ "$status" -eq 0 ]
    run dolt sql -q "call dolt_backup('sync-url', '$backupFileUrl')"
    [ "$status" -eq 0 ]

    invalidRepoDir="$BATS_TEST_TMPDIR/invalid_repo"
    mkdir -p "$invalidRepoDir"
    cd $invalidRepoDir
    dolt sql -q "call dolt_backup('restore', '$backupFileUrl', 't_db')"
    [ "$status" -eq 0 ]

    cd t_db
    run dolt sql --result-format csv -q "select * from t"
    [ "$status" -eq 0 ]
    [[ "$output" =~ i.*3.*4 ]] || false
}
