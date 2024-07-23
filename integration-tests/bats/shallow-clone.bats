#!/usr/bin/env bats
#
# Tests for shallow clone behavior. These tests use a remotesrv
# instance which hold databases with different commit history
# structures, then tests clone with different depths and validate
# behavior of other commands and stored procedures.

load $BATS_TEST_DIRNAME/helper/common.bash

remotesrv_pid=""
setup() {
    skiponwindows "tests are flaky on Windows"
    setup_no_dolt_init
}

teardown() {
    stop_remotesrv
    teardown_common
}

stop_remotesrv() {
    if [ -n "$remotesrv_pid" ]; then
        kill "$remotesrv_pid"
        wait "$remotesrv_pid" || :
        remotesrv_pid=""
    fi
}

# serial repository is 7 commits:
# (init) <- (table create) <- (val 1) <- (val 2) <- (val 3) <- (val 4) <- (val 5) [main]
seed_local_remote() {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table vals (i int primary key, s varchar(64));'
    dolt add vals
    dolt commit -m 'create table'

    for SEQ in $(seq 5); do
       dolt sql -q "insert into vals (i,s) values ($SEQ, \"val $SEQ\")"
       dolt commit -a -m "Added Val: $SEQ"
    done

    dolt tag nonheadtag HEAD~2
    cd ..
}


seed_and_start_serial_remote() {
    seed_local_remote
    cd remote

    remotesrv --http-port 1234 --repo-mode &
    remotesrv_pid=$!

    cd ..
}

@test "shallow-clone: dolt_clone depth 1" {
    seed_and_start_serial_remote

    mkdir clones
    cd clones

    dolt sql -q "call dolt_clone('--depth', '1','http://localhost:50051/test-org/test-repo')"

    run dolt log --oneline --decorate=no
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    run dolt sql -q "select count(*) = 1 from dolt_log()"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    # Verify that the table is complete.
    run dolt sql -q "select sum(i) from vals"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "15" ]] || false # 1+2+3+4+5 = 15.
}

@test "shallow-clone: dolt_clone depth 2" {
    seed_and_start_serial_remote

    mkdir clones
    cd clones
    run dolt sql -q "call dolt_clone('--depth', '2','http://localhost:50051/test-org/test-repo')"
    [ "$status" -eq 0 ]

    run dolt log --oneline --decorate=no
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]

    run dolt sql -q "select count(*) = 2 from dolt_log()"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    # Verify that the table is complete.
    run dolt sql -q "select sum(i) from vals"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "15" ]] || false # 1+2+3+4+5 = 15.
}

@test "shallow-clone: shallow clone with a file path" {
    seed_local_remote
    cd remote
    dolt remote add origin file://../file-remote
    dolt push origin main
    cd ..

    mkdir clones
    cd clones
    run dolt sql -q "call dolt_clone('--depth', '1','file://../file-remote')"
    [ "$status" -eq 0 ]

    run dolt log --oneline --decorate=no
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    run dolt sql -q "select count(*) = 1 from dolt_log()"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    # Verify that the table is complete.
    run dolt sql -q "select sum(i) from vals"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "15" ]] || false # 1+2+3+4+5 = 15.
}

@test "shallow-clone: push to a new remote should error" {
    seed_and_start_serial_remote

    mkdir clones
    cd clones

    dolt clone --depth 1 http://localhost:50051/test-org/test-repo
    cd test-repo

    dolt remote add altremote file://../file-remote-alt

    run dolt push altremote main
    [ "$status" -eq 1 ]
    # NM4 - give a better error message.
    [[ "$output" =~ "failed to get all chunks" ]] || false
}

@test "shallow-clone: depth 3 clone of serial history" {
    seed_and_start_serial_remote

    mkdir clones
    cd clones

    run dolt clone --depth 3 http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]

    cd test-repo

    run dolt log --oneline --decorate=no
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" -eq 3 ]] || false

    run dolt sql -q "select count(*) = 3 from dolt_log()"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false

#   NM4 - system table bug.
#    run dolt sql -q "select count(*) = 3 from dolt_log"
#    [ "$status" -eq 0 ]
#    [[ "$output" =~ "true" ]] || false
#

    # dolt_diff table will show two rows, because each row is a delta.
    run dolt sql -q "select * from dolt_diff"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Added Val: 4 " ]] || false
    ! [[ "$output" =~ "Added Val: 3 " ]] || false

    run dolt sql -q "select * from dolt_commits"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Added Val: 5 " ]] || false
    [[ "$output" =~ "Added Val: 4 " ]] || false
    [[ "$output" =~ "Added Val: 3 " ]] || false
    ! [[ "$output" =~ "Added Val: 2 " ]] || false

    # A full clone would have 5 commits with i=1, so if we have 3, we are looking good.
    run dolt sql -q "select count(*) = 3 from dolt_history_vals where i = 1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    # A full clone would have 2 commits with i=4, and our shallow clone has all the commits for that row.
    run dolt sql -q "select count(*) = 2 from dolt_history_vals where i = 4"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    run dolt sql -q "select count(distinct commit_hash) = 3 from dolt_history_vals"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    # Verify that the table is complete.
    run dolt sql -q "select sum(i) from vals"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "15" ]] # 1+2+3+4+5 = 15.

    run dolt show HEAD
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 5 | val 5" ]] || false
    ! [[ "$output" =~ "val 4" ]] || false

    run dolt show HEAD~1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 4 | val 4" ]] || false
    ! [[ "$output" =~ "val 3" ]] || false
    ! [[ "$output" =~ "val 5" ]] || false

    run dolt diff HEAD~2..HEAD
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/vals b/vals" ]] || false
    [[ "$output" =~ "+ | 5 | val 5" ]] || false
    [[ "$output" =~ "+ | 4 | val 4" ]] || false
    ! [[ "$output" =~ "val 3" ]] || false

    # reverse diff check.
    run dolt diff HEAD..HEAD~1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "- | 5 | val 5" ]] || false

    # Verify that the table is complete, with an as of query.
    run dolt sql -q "select sum(i) from vals as of 'HEAD~1'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "10" ]] # 1+2+3+4 = 10.
}

@test "shallow-clone: depth which exceeds history" {
    seed_and_start_serial_remote

    mkdir clones
    cd clones

    # Depth is far greater than actual history length.
    run dolt clone --depth 42 http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]

    cd test-repo

    run dolt log --oneline --decorate=no
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" -eq 7 ]] || false

    run dolt sql -q "select count(*) = 7 from dolt_log()"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    run dolt show HEAD~6
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Initialize data repository" ]] || false
}

@test "shallow-clone: as of gives decent error message" {
    seed_and_start_serial_remote

    mkdir clones
    cd clones

    run dolt clone --depth 3 http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]

    cd test-repo

    run dolt sql -q "select sum(i) from vals as of 'HEAD~4'"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Commit not found. You are using a shallow clone" ]] || false
}

@test "shallow-clone: hashof sql function gives an error message" {
    seed_and_start_serial_remote

    mkdir clones
    cd clones

    run dolt clone --depth 2 http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]

    cd test-repo

    run dolt sql -q "select hashof('HEAD~4')"

    [ "$status" -eq 1 ]
    [[ "$output" =~ "Commit not found. You are using a shallow clone" ]] || false
}

@test "shallow-clone: single depth clone of serial history" {
    seed_and_start_serial_remote

    mkdir clones
    cd clones

    dolt clone --depth 1 http://localhost:50051/test-org/test-repo
    cd test-repo

    run dolt log --oneline --decorate=no
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]


    run dolt sql -q "select count(*) = 1 from dolt_log()"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false

## not working at the moment.... NM4.
##    run dolt sql -q "select count(*) = 1 from dolt_log"
##    [ "$status" -eq 0 ]
##    [[ "$output" =~ "true" ]] || false

    # Verify that the table is complete.
    run dolt sql -q "select sum(i) from vals"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "15" ]] || false  # 1+2+3+4+5 = 15.


    run dolt diff HEAD~1..HEAD
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Commit not found. You are using a shallow clone" ]] || false

    # Dolt show can't show the diff because we only have one half of the delta.
    run dolt show
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Added Val: 5" ]] || false # We do print the message of the commit, even though we can't show the diff.
    [[ "$output" =~ "Commit not found. You are using a shallow clone" ]] || false

    run dolt tag tagfoo HEAD~1
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Commit not found. You are using a shallow clone" ]] || false

    run dolt revert HEAD~1
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Commit not found. You are using a shallow clone" ]] || false

    run dolt cherry-pick HEAD~1
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Commit not found. You are using a shallow clone" ]] || false
}

@test "shallow-clone: shallow clone can push" {
    seed_and_start_serial_remote

    mkdir clones
    cd clones

    dolt clone --depth 1 http://localhost:50051/test-org/test-repo
    cd test-repo

    dolt sql -q "insert into vals (i,s) values (42, \"val 42\")"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Your branch is up to date with 'origin/main'." ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "modified:         vals" ]] || false

    dolt commit -a -m "Added Val: 42"
    run dolt push origin main
    [ "$status" -eq 0 ]

    # Do a full clone and verify that the commit is there.
    cd ..
    dolt clone http://localhost:50051/test-org/test-repo full-clone
    cd full-clone

    run dolt show HEAD
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Added Val: 42" ]] || false

    run dolt sql -q "select sum(i) from vals"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "57" ]] || false # 1+2+3+4+5+42 = 57.
}

@test "shallow-clone: fetch new changes after shallow clone" {
    seed_and_start_serial_remote

    mkdir clones
    cd clones

    # initial clone
    dolt clone --depth 2 http://localhost:50051/test-org/test-repo

    # clone another copy, and push to remote srv.
    dolt clone http://localhost:50051/test-org/test-repo full-clone
    cd full-clone

    dolt sql -q "insert into vals (i,s) values (23, \"val 23\")"
    dolt commit -a -m "Added Val: 23"
    run dolt push origin main
    [ "$status" -eq 0 ]

    # Go to out of date clone, and fetch.
    cd ../test-repo
    run dolt pull
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]] || false

    run dolt log --oneline --decorate=no
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]

    dolt show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Added Val: 23" ]] || false
}

@test "shallow-clone: fetch connected new branch works after shallow clone" {
  seed_and_start_serial_remote

  mkdir clones
  cd clones

  # initial clone
  dolt clone --depth 2 http://localhost:50051/test-org/test-repo

  # clone another copy, and push new branch to remote srv.
  dolt clone http://localhost:50051/test-org/test-repo full-clone
  cd full-clone

  # Create two new commits on top of commit which exists in shallow clone.
  dolt sql -q "insert into vals (i,s) values (23, \"val 23\")"
  dolt commit -a -m "Added Val: 23"
  dolt sql -q "insert into vals (i,s) values (42, \"val 42\")"
  dolt commit -a -m "Added Val: 42"
  dolt push origin HEAD:refs/heads/brch

  cd ../test-repo

  dolt fetch # Should pull new branch, and it's history should be length 4.
  run dolt branch -a
  [ "$status" -eq 0 ]
  [[ "$output" =~ "remotes/origin/brch" ]] || false

  run dolt log --oneline --decorate=no origin/brch
  [ "$status" -eq 0 ]
  [ "${#lines[@]}" -eq 4 ]

  run dolt show origin/brch
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Added Val: 42" ]] || false

  run dolt show origin/brch~1
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Added Val: 23" ]] || falses
}

@test "shallow-clone: fetch disconnected new branch works after shallow clone" {
    seed_and_start_serial_remote

    mkdir clones
    cd clones

    # initial clone
    dolt clone --depth 2 http://localhost:50051/test-org/test-repo

    # clone another copy, and push new branch to remote srv.
    dolt clone http://localhost:50051/test-org/test-repo full-clone
    cd full-clone
    # Create two new commits rooted from a commit which doesn't exist in the
    dolt reset --hard HEAD~3 # HEAD~3 == (val 2)
    dolt sql -q "insert into vals (i,s) values (13, \"val 13\")"
    dolt commit -a -m "Added Val: 13"
    dolt sql -q "insert into vals (i,s) values (11, \"val 11\")"
    dolt commit -a -m "Added Val: 11"
    dolt push origin HEAD:refs/heads/brch

    cd ../test-repo

    dolt fetch # Should pull new branch, and it's history should be length 2.
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/origin/brch" ]] || false

    run dolt log --oneline --decorate=no origin/brch
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]

    run dolt show origin/brch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Added Val: 11" ]] || false

    # Verify that the table is complete.
    run dolt sql -q "select sum(i) from vals as of 'origin/brch'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "27" ]] # 1+2+11+13 = 27.
}

# complex repository is 14 commits with the following dag:
#
# (init) <- (table create) <- (val 1) <- (val 2) <- (val 3) <- (val 4) <- (val 5) <- (merge 2)                [main]
#                            \                          \                          /
#                             \- (val 6) <- (val 7) <- (merge 1) <- (val 8) <-----/  <- (val 9) <- (val 10)   [brch]
seed_and_start_complex_remote() {
    mkdir remote
    cd remote
    dolt init
    dolt sql -q 'create table vals (i int primary key, s varchar(64));'
    dolt add vals
    dolt commit -m 'create table'

    for SEQ in $(seq 5); do
       dolt sql -q "insert into vals (i,s) values ($SEQ, \"val $SEQ\")"
       dolt commit -a -m "Added Val: $SEQ"
    done

    dolt checkout -b brch HEAD~5
    for SEQ in $(seq 6 7); do
       dolt sql -q "insert into vals (i,s) values ($SEQ, \"val $SEQ\")"
       dolt commit -a -m "Added Val: $SEQ"
    done

    dolt merge main~2
    for SEQ in $(seq 8 10); do
       dolt sql -q "insert into vals (i,s) values ($SEQ, \"val $SEQ\")"
       dolt commit -a -m "Added Val: $SEQ"
    done

    dolt checkout main
    dolt merge brch~2

    remotesrv --http-port 1234 --repo-mode &
    remotesrv_pid=$!

    cd ..
}

@test "shallow-clone: single depth clone of a complex history" {
    seed_and_start_complex_remote

    mkdir clones
    cd clones

    dolt clone --depth 1 http://localhost:50051/test-org/test-repo
    cd test-repo

    run dolt log --oneline --decorate=no
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    run dolt sql -q "select count(*) = 1 from dolt_log()"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    ## not working at the moment.... NM4.
    ##    run dolt sql -q "select count(*) = 1 from dolt_log"
    ##    [ "$status" -eq 0 ]
    ##    [[ "$output" =~ "true" ]] || false

    # Verify that the table is complete.
    run dolt sql -q "select sum(i) from vals"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "36" ]] || false # 1+2+3+4+5+6+7+8 = 36.
}

@test "shallow-clone: depth 2 clone of a complex history" {
    seed_and_start_complex_remote

    mkdir clones
    cd clones

    # GHOST <- (val 5) <-\
    #                     (merge 2)     [main]
    # GHOST <- (val 8) <-/
    dolt clone --depth 2 http://localhost:50051/test-org/test-repo
    cd test-repo

    run dolt log --oneline --decorate=no
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]

    run dolt sql -q "select count(*) = 3 from dolt_log()"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    ## not working at the moment.... NM4.
    ##    run dolt sql -q "select count(*) = 1 from dolt_log"
    ##    [ "$status" -eq 0 ]
    ##    [[ "$output" =~ "true" ]] || false

    # compare the diff between the two parents of the merge commit.
    run dolt diff HEAD^..HEAD^2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "- | 4 | val 4" ]] || false
    [[ "$output" =~ "- | 5 | val 5" ]] || false
    [[ "$output" =~ "+ | 6 | val 6" ]] || false
    [[ "$output" =~ "+ | 7 | val 7" ]] || false
    [[ "$output" =~ "+ | 8 | val 8" ]] || false
}

@test "shallow-clone: clone alternate branch" {
    seed_and_start_complex_remote

    mkdir clones
    cd clones

    # Cloning depth 5 from brch should result in the following 6 commits:
    # GHOST <- (val 3) <-\
    #                     \
    # GHOST <- (val 7) <- (merge 1) <- (val 8) <- (val 9) <- (val 10)  [brch]
    dolt clone --depth 5 --branch brch http://localhost:50051/test-org/test-repo
    cd test-repo

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch brch" ]] || false
    [[ "$output" =~ "Your branch is up to date with 'origin/brch'" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false

    run dolt log --oneline --decorate=no
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]

    run dolt diff HEAD~3^..HEAD~3^2  # compare (val 7) --> (val 3) [parents of the first merge]
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+ | 1 | val 1" ]] || false
    [[ "$output" =~ "+ | 2 | val 2" ]] || false
    [[ "$output" =~ "+ | 3 | val 3" ]] || false
    [[ "$output" =~ "- | 6 | val 6" ]] || false
    [[ "$output" =~ "- | 7 | val 7" ]] || false

    run dolt merge-base HEAD~3^ HEAD~3^2 # (val 3) and (val 3) have a common ancestor in a full clone, should error.
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Commit not found. You are using a shallow clone" ]] || false

}

@test "shallow-clone: clone depth 5 on complex main" {
    seed_and_start_complex_remote
    mkdir clones
    cd clones

    # GHOST <- (val 2) <- (val 3) <- (val 4) <- (val 5) <- (merge 2)  [main]
    #                               \                     /
    # GHOST <- (val 6) <-(val 7) <- (merge 1) <- (val 8)
    dolt clone --depth 5 http://localhost:50051/test-org/test-repo
    cd test-repo

    run dolt log --oneline --decorate=no
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 9 ]

    run dolt merge-base HEAD^ HEAD^2 # (val 5) and (val 8) are the parents of the merge commit. Should return (val 3)
    [ "$status" -eq 0 ]
    commitid="$output"

    run dolt show "$commitid"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Added Val: 3" ]] || false
}



# Tests to write:
# - Fetch after initial clone
#   - Fetch when no changes have happened.
#   - Fetch when there are remote changes on main
#   - Fetch when there are remote changes on a branch
# - Pull after initial clone
#   - Pull when no changes have happened.
#   - Pull when there are remote changes on main
# - Sensible error when branching/checking out a commit which they don't have.
# - merge base errors
# - GC works? or gives a decent error message?
# - reset work to a commit we have, and errors when we don't have the commit.
# - Sensible error when we attempt to use HEAD~51 or something.
# - Don't serve from a shallow repository
#   - remotesrv
#   - sql-server
#   - file (stretch?)
# - Dump?
# - Rebase?
# - Stash?
# - Fetch tags which refer to commits we don't have. Punt on tags entirely?
