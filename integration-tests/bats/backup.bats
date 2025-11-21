#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/remotesrv-common.bash

setup() {
    setup_common
    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{rem1,bac1,repo1}

    # repo1 -> rem1 -> repo2
    cd $TMPDIRS/repo1
    dolt init
    dolt tag v1
    dolt sql -q "create table t1 (a int)"
    dolt add .
    dolt commit -am "cm"
    dolt branch feature
    dolt remote add origin file://../rem1
    dolt push origin main
    cd $TMPDIRS
}

teardown() {
    stop_remotesrv
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "backup: add named backup" {
    cd repo1
    dolt backup add bac1 file://../bac1
    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "bac1" ]] || false

    mkdir newdir && cd newdir
    run dolt backup add bac1 file://../bac1
    [ "$status" -ne 0 ]
}

@test "backup: remove named backup" {
    cd repo1
    dolt backup add bac1 file://../bac1
    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "bac1" ]] || false

    dolt backup remove bac1

    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
    [[ ! "$output" =~ "bac1" ]] || false

    mkdir newdir && cd newdir
    run dolt backup remove bac1
    [ "$status" -ne 0 ]
}

@test "backup: rm named backup" {
    cd repo1
    dolt backup add bac1 file://../bac1
    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "bac1" ]] || false

    dolt backup rm bac1

    run dolt backup -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
    [[ ! "$output" =~ "bac1" ]] || false

    mkdir newdir && cd newdir
    run dolt backup rm bac1
    [ "$status" -ne 0 ]
}

@test "backup: removing a backup with the same name as a remote does not impact remote tracking refs" {
    cd repo1
    dolt backup add origin file://../bac1
    dolt backup remove origin

    run dolt sql -q "show tables as of hashof('origin/main')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "Table" ]] || false
    [[ "${lines[1]}" =~ "t1" ]] || false
}

@test "backup: sync master to backup" {
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    dolt backup restore file://./bac1 repo2
    cd repo2
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "backup: sync in a non-dolt directory" {
    mkdir newdir && cd newdir
    run dolt backup sync bac1
    [ "$status" -ne 0 ]
}

@test "backup: sync feature to backup" {
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    dolt backup restore file://./bac1 repo2
    cd repo2
    run dolt branch
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "* main" ]] || false
    [[ "$output" =~ "feature" ]] || false
}

@test "backup: sync tag to backup" {
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    dolt backup restore file://./bac1 repo2
    cd repo2
    run dolt tag
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "backup: sync remote ref to backup" {
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    dolt backup restore file://./bac1 repo2
    cd repo2
    run dolt sql -q "show tables as of hashof('origin/main')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "backup: sync working set to backup" {
    cd repo1
    dolt sql -q "create table t2 (a int)"
    dolt add t2
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    dolt backup restore file://./bac1 repo2
    cd repo2
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t2" ]] || false
}

@test "backup: no origin on restore" {
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    run dolt backup restore file://./bac1 repo2
    cd repo2
    run dolt remote -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
    [[ ! "$output" =~ "origin" ]] || false
}

@test "backup: backup already up to date" {
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1
    dolt backup sync bac1
}

@test "backup: no backup exists" {
    cd repo1
    run dolt backup sync bac1
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "unknown backup 'bac1'" ]] || false
}

@test "backup: cannot override another client's backup" {
    skip "todo implement backup lock file"
    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd .. && mkdir repo2 && cd repo2
    dolt init
    dolt sql -q "create table s1 (a int)"
    dolt add .
    dolt commit -am "cm"

    dolt backup add bac1 file://../bac1
    dolt backup sync bac1
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "unknown backup 'bac1'" ]] || false
}

@test "backup: cannot clone a backup" {
    skip "todo implement backup lock file"

    cd repo1
    dolt backup add bac1 file://../bac1
    dolt backup sync bac1

    cd ..
    dolt clone file://./bac1 repo2
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "cannot clone backup" ]] || false
}

@test "backup: cannot add backup with address of existing remote" {
    cd repo1
    dolt remote add rem1 file://../bac1
    run dolt backup add bac1 file://../bac1
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "address conflict with a remote: 'rem1'" ]] || false
}

@test "backup: cannot add backup with address of existing backup" {
    cd repo1
    dolt backup add bac1 file://../bac1
    run dolt backup add bac2 file://../bac1
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "address conflict with a remote: 'bac1'" ]] || false
}

@test "backup: cannot add remote with address of existing backup" {
    cd repo1
    dolt backup add bac1 file://../bac1
    run dolt remote add rem1 file://../bac1
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "address conflict with a remote: 'bac1'" ]] || false
}

@test "backup: sync-url" {
    cd repo1
    dolt backup sync-url file://../bac1

    cd ..
    run dolt backup restore file://./bac1 repo2
    cd repo2
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "backup: restore existing database fails" {
    cd repo1
    dolt backup sync-url file://../bac1

    cd ..
    mkdir repo2
    cd repo2
    dolt init

    # Check in the ".dolt" is in my current directory case...
    run dolt backup restore file://../bac1 repo2
    [ "$status" -ne 0 ]
    [[ "$output" =~ "cannot restore backup into repo2. A database with that name already exists" ]] || false

    # Check in the ".dolt" is in a subdirectory case...
    cd ..
    run dolt backup restore file://../bac1 repo2
    [ "$status" -ne 0 ]
    [[ "$output" =~ "cannot restore backup into repo2. A database with that name already exists" ]] || false
}

@test "backup: restore existing database with --force succeeds" {
    cd repo1
    dolt backup sync-url file://../bac1

    cd ..
    mkdir repo2
    cd repo2
    dolt init

    # Check in the ".dolt" is in my current directory case...
    dolt backup restore --force file://../bac1 repo2

    cd ../repo1
    dolt commit --allow-empty -m 'another commit'
    dolt backup sync-url file://../bac1

    # Check in the ".dolt" is in a subdirectory case...
    cd ..
    dolt backup restore --force file://./bac1 repo2
}

@test "backup: sync-url in a non-dolt directory" {
    mkdir newdir && cd newdir
    run dolt backup sync-url file://../bac1
    [ "$status" -ne 0 ]
}

@test "backup: add HTTP and HTTPS" {
    cd repo1
    dolt backup add httpbackup http://localhost:$REMOTESRV_PORT/test-org/backup-repo
    dolt backup add httpsbackup https://localhost:$REMOTESRV_PORT/test-org/backup-repo
    run dolt backup -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "httpbackup" ]] || false
    [[ "$output" =~ "http://localhost:$REMOTESRV_PORT/test-org/backup-repo" ]] || false
    [[ "$output" =~ "httpsbackup" ]] || false
    [[ "$output" =~ "https://localhost:$REMOTESRV_PORT/test-org/backup-repo" ]] || false
}

# No HTTPS test for sync-url; `dolt/go/utils/remotesrv` does not expose TLS configuration flags.
@test "backup: sync-url and restore HTTP" {
    start_remotesrv
    
    cd repo1
    dolt sql -q "CREATE TABLE t2 (b int)"
    dolt add .
    dolt commit -am "add t2"
    
    run dolt backup sync-url http://localhost:$REMOTESRV_PORT/test-org/backup-repo
    [ "$status" -eq 0 ]
    
    cd ..
    dolt backup restore http://localhost:$REMOTESRV_PORT/test-org/backup-repo repo3
    cd repo3
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
}
