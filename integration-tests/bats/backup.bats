#!/usr/bin/env bats
# Since the dolt backup command uses dolt_backup procedure internally, `sql-backup.bats` tests should generally apply to
# `backup.bats` as well, removing the need to duplicate tests. As a result, prefer writing backup tests in
# `sql-backup.bats`. Exceptions to this rule is testing external factors related to commands (i.e. invalid repo checks
#  before calling dolt_backup) or command specific functionality (i.e. the command version can list current database
#  backup table).

load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/remotesrv-common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

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
    [[ "$output" =~ "backup 'bac1' not found" ]] || false
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
    echo $output
    [[ "$output" =~ "database 'repo2' already exists, use '--force' to overwrite" ]] || false

    # Check in the ".dolt" is in a subdirectory case...
    cd ..
    run dolt backup restore file://bac1 repo2
    [ "$status" -ne 0 ]
    [[ "$output" =~ "database 'repo2' already exists, use '--force' to overwrite" ]] || false
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

@test "backup: rejects AWS parameters in sql-server" {
    skip_if_remote
    cd repo1
    start_sql_server
    
    run dolt backup add test aws://[table:bucket]/db --aws-region=us-east-1
    [ "$status" -eq 1 ]
    [[ "$output" =~ "AWS parameters are unavailable when running in server mode" ]] || false

    run dolt backup sync-url aws://[table:bucket]/db --aws-creds-type=file
    [ "$status" -eq 1 ]
    [[ "$output" =~ "AWS parameters are unavailable when running in server mode" ]] || false

    run dolt backup restore aws://[table:bucket]/db newdb --aws-region=us-east-1
    [ "$status" -eq 1 ]
    [[ "$output" =~ "AWS parameters are unavailable when running in server mode" ]] || false

    run dolt backup add aws-backup2 aws://[table:bucket]/db --aws-creds-type=file
    [ "$status" -eq 1 ]
    [[ "$output" =~ "AWS parameters are unavailable when running in server mode" ]] || false

    stop_sql_server
}

@test "backup: restore works from non-dolt directory" {
    cd repo1
    backupFileUrl="file://$BATS_TEST_TMPDIR/backup"
    dolt sql<<EOF
create table t (i int);
insert into t values (1), (2);
call dolt_backup('sync-url', '$backupFileUrl');
EOF

    invalidRepo="$BATS_TEST_TMPDIR/invalidRepo"
    mkdir "$invalidRepo" && cd "$invalidRepo"

    run dolt backup restore "$backupFileUrl" new_db
    [ "$status" -eq 0 ]
    run dolt sql -r csv -q "select * from t"
    [[ "$output" =~ i.*2.*1 ]] || false
}

@test "backup: backup pushes table files when journal is small relative to store size" {
    get_table_files() {
        find "$1" | while read f; do
	    base=$(basename "$f")
	    if [ "$base" != "manifest" ] && [ "$base" != "LOCK" ] && [ "$base" != "journal.idx" ] && ! [ -d "$f" ]; then
	        echo "$base"
            fi
	done | sort
    }
    # Create many empty commits to make the journal slightly bigger.
    cd repo1
    dolt checkout -b newbranch
    command=""
    for i in $(seq 128); do command="$command call dolt_commit('--allow-empty', '-Am', 'empty commit');"; done
    dolt sql -q "$command" > /dev/null
    dolt checkout main
    dolt branch -D newbranch
    cd ..

    # At the beginning of the test, repo1 should only be a journal file.
    start_table_files=( $(get_table_files repo1/.dolt/noms) )
    [[ ${#start_table_files[*]} -eq 1 ]] || false
    [[ ${start_table_files[0]} == "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv" ]] || false
    start_journal_size=$( wc -c repo1/.dolt/noms/vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv | awk '{print $1}')

    # Take a file backup of repo1.
    cd repo1
    dolt backup sync-url file://../repo1_start_backup
    cd ..

    # We should have pushed a table file.
    backup_table_files=( $(get_table_files repo1_start_backup) )
    [[ ${#backup_table_files[*]} -eq 1 ]] || false
    [[ ${backup_table_files[0]} != "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv" ]] || false
    backup_tablefile_size=$( wc -c repo1_start_backup/"${backup_table_files[0]}" | awk '{print $1}')

    # And that table file should be much smaller than our journal.
    upper_limit=$(( ${start_journal_size} / 5 ))
    [[ "$backup_tablefile_size" -lt "$upper_limit" ]] || false

    # Add lots of commits again, this time to main, and GC repo1
    cd repo1
    dolt sql -q "$command" > /dev/null
    dolt gc
    cd ..

    postgc_table_files=( $(get_table_files repo1/.dolt/noms) )

    # Take another backup of repo1
    cd repo1
    dolt backup sync-url file://../repo1_postgc_backup
    cd ..

    postgc_backup_table_files=( $(get_table_files repo1_postgc_backup) )

    # Because we pushed table files, every table file in the source should be in the destination.
    [[ ${#postgc_backup_table_files[*]} -ge ${#postgc_table_files[*]} ]] || false
    for tfname in ${postgc_table_files[@]}; do
        found=0
        for backuptfname in ${postgc_backup_table_files[@]}; do
	    if [[ "$backuptfname" == "$tfname" ]]; then
	        found=1
	    fi
	done
	if [[ "$found" -eq 0 ]]; then
	    echo "expected to find $tfname in ${postgc_backup_table_files[*]}, but did not"
	    exit 1
	fi
    done
}

@test "backup: backup restore pulls table files" {
    get_table_files() {
        find "$1" | while read f; do
	    base=$(basename "$f")
	    if [ "$base" != "manifest" ] && [ "$base" != "LOCK" ] && [ "$base" != "journal.idx" ] && ! [ -d "$f" ]; then
	        echo "$base"
            fi
	done | sort
    }
    # Create many commits and then GC the repo.
    cd repo1
    command=""
    for i in $(seq 128); do command="$command call dolt_commit('--allow-empty', '-Am', 'empty commit');"; done
    dolt sql -q "$command" > /dev/null
    dolt gc
    expected_commits=$(dolt sql -r csv -q 'select count(1) from dolt_log()' | tail -n 1)
    dolt branch -v
    cd ..

    # Make a backup.
    cd repo1
    dolt backup sync-url file://../repo1_backup
    cd ..

    # Restore the backup.
    dolt backup restore file://./repo1_backup repo1_backup_restored

    backup_table_files=( $(get_table_files repo1_backup) )
    restored_table_files=( $(get_table_files repo1_backup_restored/.dolt/noms) )

    # Check that the restored backup is as expected.
    cd repo1_backup_restored
    restored_commits=$(dolt sql -r csv -q 'select count(1) from dolt_log()' | tail -n 1)
    echo $expected_commits $restored_commits
    echo ${restored_table_files[*]}
    find .
    echo
    echo
    find ../repo1_backup
    dolt branch -v
    echo
    cat ../repo1/.dolt/noms/manifest
    echo
    cat ../repo1_backup/manifest
    echo
    cat ./.dolt/noms/manifest
    [[ $expected_commits -eq $restored_commits ]] || false

    for tfname in ${backup_table_files[@]}; do
        found=0
        for restoredtfname in ${restored_table_files[@]}; do
	    if [[ "$restoredtfname" == "$tfname" ]]; then
	        found=1
	    fi
	done
	if [[ "$found" -eq 0 ]]; then
	    echo "expected to find $tfname in ${_table_files[*]}, but did not"
	    exit 1
	fi
    done
}