#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{rem1,tmp1}

    # tmp1 -> rem1 -> tmp2
    cd $TMPDIRS/tmp1
    dolt init
    dolt remote add origin file://../rem1
    dolt remote add test-remote file://../rem1
    dolt push origin master

    cd $TMPDIRS
    dolt clone file://rem1 tmp2
    cd $TMPDIRS/tmp2
    dolt branch feature
    dolt remote add test-remote file://../rem1

    # table and comits only present on tmp1, rem1 at start
    cd $TMPDIRS/tmp1
    dolt sql -q "create table t1 (a int primary key, b int)"
    dolt commit -am "First commit"
    dolt sql -q "insert into t1 values (0,0)"
    dolt commit -am "Second commit"
    dolt branch feature
    dolt push origin master
    cd $TMPDIRS
}

teardown() {
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "sql-fetch: dolt_fetch default" {
    cd tmp2
    dolt sql -q "select dolt_fetch()"

    run dolt diff master origin/master
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/master')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}


@test "sql-fetch: dolt_fetch origin" {
    cd tmp2
    dolt sql -q "select dolt_fetch('origin')"

    run dolt diff master origin/master
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/master')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch master" {
    cd tmp2
    dolt sql -q "select dolt_fetch('origin', 'master')"

    run dolt diff master origin/master
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/master')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch custom remote" {
    cd tmp2
    dolt sql -q "select dolt_fetch('test-remote')"

   run dolt diff master test-remote/master
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('test-remote/master')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch specific ref" {
    cd tmp2
    dolt sql -q "select dolt_fetch('test-remote', 'refs/heads/master:refs/remotes/test-remote/master')"

    run dolt diff master test-remote/master
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('test-remote/master')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch feature branch" {
    cd tmp1
    dolt push origin feature

    cd ../tmp2
    dolt sql -q "select dolt_fetch('origin', 'feature')"

    run dolt diff master origin/feature
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/feature')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch tag" {
    cd tmp1
    dolt tag v1
    dolt push origin v1

    cd ../tmp2
    dolt sql -q "select dolt_fetch('origin', 'master')"

    run dolt diff master v1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('v1')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch only tag" {
    skip "todo tag refspec support, and/or --tags option"
    cd tmp1
    dolt tag v1
    dolt push origin v1

    cd ../tmp2
    dolt sql -q "select dolt_fetch('origin', 'refs/tags/v1:refs/tags/v1')"

    run dolt diff master origin/v1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('v1')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch rename ref" {
    cd tmp2
    dolt sql -q "select dolt_fetch('test-remote', 'refs/heads/master:refs/remotes/test-remote/other')"

    run dolt diff master test-remote/other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added table" ]] || false

    run dolt sql -q "show tables as of hashof('test-remote/other')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch override local branch" {
    skip "todo more flexible refspec support"
    cd tmp2
    dolt sql -q "select dolt_fetch('origin', 'master:refs/heads/master')"

    dolt diff master origin/master
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "removed table" ]] || false

    run dolt sql -q "show tables as of hashof('master')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t1" ]] || false
}

@test "sql-fetch: dolt_fetch --force" {
    # reverse information flow for force fetch tmp1->rem1->tmp2
    cd tmp2
    dolt sql -q "create table t2 (a int)"
    dolt commit -am "forced commit"
    dolt push --force origin master

    cd ../tmp1
    run dolt sql -q "select dolt_fetch('origin', 'master')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "fetch failed: can't fast forward merge" ]] || false

    dolt sql -q "select dolt_fetch('--force', 'origin', 'master')"

    run dolt diff master origin/master
    [ "$status" -eq 0 ]
    [[ "$output" =~ "deleted table" ]] || false

    run dolt sql -q "show tables as of hashof('origin/master')" -r csv
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "Table" ]] || false
    [[ "$output" =~ "t2" ]] || false
}

@test "sql-fetch: dolt_fetch unknown remote fails" {
    cd tmp2
    dolt remote remove origin
    run dolt sql -q "select dolt_fetch('unknown', 'master')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
}

@test "sql-fetch: dolt_fetch unknown ref fails" {
    cd tmp2
    run dolt sql -q "select dolt_fetch('origin', 'unknown')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid ref spec: 'unknown'" ]] || false
}

@test "sql-fetch: dolt_fetch empty remote fails" {
    cd tmp2
    dolt remote remove origin
    run dolt sql -q "select dolt_fetch('')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unknown remote" ]] || false
}

@test "sql-fetch: dolt_fetch empty ref fails" {
    cd tmp2
    run dolt sql -q "select dolt_fetch('origin', '')"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid fetch spec: ''" ]] || false
}
