#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    skip_nbf_dolt_1
    setup_common
    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{rem1,repo1}

    # repo1 -> rem1 -> repo2
    cd $TMPDIRS/repo1
    dolt init
    dolt remote add origin file://../rem1
    dolt remote add test-remote file://../rem1
    dolt push origin main

    cd $TMPDIRS
    dolt clone file://rem1 repo2
    cd $TMPDIRS/repo2
    dolt branch feature
    dolt remote add test-remote file://../rem1

    # table and comits only present on repo1, rem1 at start
    cd $TMPDIRS/repo1
    dolt sql -q "create table t1 (a int primary key, b int)"
    dolt commit -am "First commit"
    dolt sql -q "insert into t1 values (0,0)"
    dolt commit -am "Second commit"
    dolt branch feature
    dolt push origin main
    cd $TMPDIRS
}

teardown() {
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "sql-fetch: dolt_fetch default" {
    cd repo2
    dolt sql -q "select dolt_fetch()"

    run dolt diff main origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: CALL dolt_fetch default" {
    cd repo2
    dolt sql -q "CALL dolt_fetch()"

    run dolt diff main origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: CALL dfetch default" {
    cd repo2
    dolt sql -q "CALL dfetch()"

    run dolt diff main origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch origin" {
    cd repo2
    dolt sql -q "select dolt_fetch('origin')"

    run dolt diff main origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: CALL dolt_fetch origin" {
    cd repo2
    dolt sql -q "CALL dolt_fetch('origin')"

    run dolt diff main origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch main" {
    cd repo2
    dolt sql -q "select dolt_fetch('origin', 'main')"

    run dolt diff main origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: CALL dolt_fetch main" {
    cd repo2
    dolt sql -q "CALL dolt_fetch('origin', 'main')"

    run dolt diff main origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch custom remote" {
    cd repo2
    dolt sql -q "select dolt_fetch('test-remote')"

   run dolt diff main test-remote/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('test-remote/main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: CALL dolt_fetch custom remote" {
    cd repo2
    dolt sql -q "CALL dolt_fetch('test-remote')"

   run dolt diff main test-remote/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('test-remote/main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch specific ref" {
    cd repo2
    dolt sql -q "select dolt_fetch('test-remote', 'refs/heads/main:refs/remotes/test-remote/main')"

    run dolt diff main test-remote/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('test-remote/main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: CALL dolt_fetch specific ref" {
    cd repo2
    dolt sql -q "CALL dolt_fetch('test-remote', 'refs/heads/main:refs/remotes/test-remote/main')"

    run dolt diff main test-remote/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('test-remote/main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch feature branch" {
    cd repo1
    dolt push origin feature

    cd ../repo2
    dolt sql -q "select dolt_fetch('origin', 'feature')"

    run dolt diff main origin/feature
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/feature')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: CALL dolt_fetch feature branch" {
    cd repo1
    dolt push origin feature

    cd ../repo2
    dolt sql -q "CALL dolt_fetch('origin', 'feature')"

    run dolt diff main origin/feature
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/feature')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch tag" {
    cd repo1
    dolt tag v1
    dolt push origin v1

    cd ../repo2
    dolt sql -q "select dolt_fetch('origin', 'main')"

    run dolt diff main v1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('v1')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: CALL dolt_fetch tag" {
    cd repo1
    dolt tag v1
    dolt push origin v1

    cd ../repo2
    dolt sql -q "CALL dolt_fetch('origin', 'main')"

    run dolt diff main v1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('v1')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch only tag" {
    skip "todo tag refspec support, and/or --tags option"
    cd repo1
    dolt tag v1
    dolt push origin v1

    cd ../repo2
    dolt sql -q "select dolt_fetch('origin', 'refs/tags/v1:refs/tags/v1')"

    run dolt diff main origin/v1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('v1')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: CALL dolt_fetch only tag" {
    skip "todo tag refspec support, and/or --tags option"
    cd repo1
    dolt tag v1
    dolt push origin v1

    cd ../repo2
    dolt sql -q "CALL dolt_fetch('origin', 'refs/tags/v1:refs/tags/v1')"

    run dolt diff main origin/v1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('v1')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch rename ref" {
    cd repo2
    dolt sql -q "select dolt_fetch('test-remote', 'refs/heads/main:refs/remotes/test-remote/other')"

    run dolt diff main test-remote/other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('test-remote/other')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: CALL dolt_fetch rename ref" {
    cd repo2
    dolt sql -q "CALL dolt_fetch('test-remote', 'refs/heads/main:refs/remotes/test-remote/other')"

    run dolt diff main test-remote/other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('test-remote/other')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch override local branch" {
    skip "todo more flexible refspec support"
    cd repo2
    dolt sql -q "select dolt_fetch('origin', 'main:refs/heads/main')"

    dolt diff main origin/main
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "removed table" ]] || false

    run dolt sql -q "show tables as of hashof('main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: CALL dolt_fetch override local branch" {
    skip "todo more flexible refspec support"
    cd repo2
    dolt sql -q "CALL dolt_fetch('origin', 'main:refs/heads/main')"

    dolt diff main origin/main
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "removed table" ]] || false

    run dolt sql -q "show tables as of hashof('main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch --force" {
    # reverse information flow for force fetch repo1->rem1->repo2
    cd repo2
    dolt sql -q "create table t2 (a int)"
    dolt commit -am "forced commit"
    dolt push --force origin main

    cd ../repo1
    run dolt sql -q "select dolt_fetch('origin', 'main')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fetch failed: can't fast forward merge" ]] || false

    dolt sql -q "select dolt_fetch('--force', 'origin', 'main')"

    run dolt diff main origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "deleted table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t2" ]] || false
}

@test "sql-fetch: CALL dolt_fetch --force" {
    # reverse information flow for force fetch repo1->rem1->repo2
    cd repo2
    dolt sql -q "create table t2 (a int)"
    dolt commit -am "forced commit"
    dolt push --force origin main

    cd ../repo1
    run dolt sql -q "CALL dolt_fetch('origin', 'main')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fetch failed: can't fast forward merge" ]] || false

    dolt sql -q "CALL dolt_fetch('--force', 'origin', 'main')"

    run dolt diff main origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "deleted table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/main')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t2" ]] || false
}

@test "sql-fetch: dolt_fetch unknown remote fails" {
    cd repo2
    dolt remote remove origin
    run dolt sql -q "select dolt_fetch('unknown')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
}

@test "sql-fetch: CALL dolt_fetch unknown remote fails" {
    cd repo2
    dolt remote remove origin
    run dolt sql -q "CALL dolt_fetch('unknown')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
}

@test "sql-fetch: dolt_fetch unknown remote with fetchspec fails" {
    cd repo2
    dolt remote remove origin
    run dolt sql -q "select dolt_fetch('unknown', 'main')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
}

@test "sql-fetch: CALL dolt_fetch unknown remote with fetchspec fails" {
    cd repo2
    dolt remote remove origin
    run dolt sql -q "CALL dolt_fetch('unknown', 'main')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
}

@test "sql-fetch: dolt_fetch unknown ref fails" {
    cd repo2
    run dolt sql -q "select dolt_fetch('origin', 'unknown')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid ref spec: 'unknown'" ]] || false
}

@test "sql-fetch: CALL dolt_fetch unknown ref fails" {
    cd repo2
    run dolt sql -q "CALL dolt_fetch('origin', 'unknown')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid ref spec: 'unknown'" ]] || false
}

@test "sql-fetch: dolt_fetch empty remote fails" {
    cd repo2
    dolt remote remove origin
    run dolt sql -q "select dolt_fetch('')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
}

@test "sql-fetch: CALL dolt_fetch empty remote fails" {
    cd repo2
    dolt remote remove origin
    run dolt sql -q "CALL dolt_fetch('')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
}

@test "sql-fetch: dolt_fetch empty ref fails" {
    cd repo2
    run dolt sql -q "select dolt_fetch('origin', '')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid fetch spec: ''" ]] || false
}

@test "sql-fetch: CALL dolt_fetch empty ref fails" {
    cd repo2
    run dolt sql -q "CALL dolt_fetch('origin', '')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid fetch spec: ''" ]] || false
}
