#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

remotesrv_pid=
setup() {
    setup_common
    cd $BATS_TMPDIR
    mkdir remotes-$$
    mkdir remotes-$$/empty
    echo remotesrv log available here $BATS_TMPDIR/remotes-$$/remotesrv.log
    remotesrv --http-port 1234 --dir ./remotes-$$ &> ./remotes-$$/remotesrv.log 3>&- &
    remotesrv_pid=$!
    cd dolt-repo-$$
    mkdir "dolt-repo-clones"
    dolt remote add test-remote http://localhost:50051/test-org/test-repo
}

teardown() {
    teardown_common
    kill $remotesrv_pid
    rm -rf $BATS_TMPDIR/remotes-$$
}

@test "dolt remotes server is running" {
    ps -p $remotesrv_pid | grep remotesrv
}

@test "gc on empty dir" {
    dolt gc
    dolt gc
    dolt gc -s
}

@test "dolt gc smoke test" {
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO test VALUES
    (1),(2),(3),(4),(5);
SQL
    run dolt sql -q 'select count(*) from test' -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "5" ]] || false

    dolt gc
    dolt gc
    run dolt gc
    [ "$status" -eq "0" ]
    run dolt status
    [ "$status" -eq "0" ]

    dolt sql <<SQL
CREATE TABLE test2 (pk int PRIMARY KEY);
INSERT INTO test2 VALUES
    (1),(2),(3),(4),(5);
SQL

    run dolt sql -q 'select count(*) from test' -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "5" ]] || false
    run dolt sql -q 'select count(*) from test2' -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "5" ]] || false

    run dolt gc
    [ "$status" -eq "0" ]
    run dolt status
    [ "$status" -eq "0" ]
}

@test "clone a remote" {
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO test VALUES (0),(1),(2);
SQL
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote master

    cd "dolt-repo-clones"
    run dolt clone http://localhost:50051/test-org/test-repo
    [ "$status" -eq 0 ]
    cd ../

    # running GC will update the manifest to version 5
    run dolt gc
    [ "$status" -eq 0 ]
    dolt sql <<SQL
INSERT INTO test VALUES (10),(11),(12);
SQL
    dolt add test
    dolt commit -m "test commit2"
    dolt push test-remote master

    # assert that the clone still works
    cd "dolt-repo-clones/test-repo"
    run dolt pull
    [ "$status" -eq 0 ]
    run dolt sql -q "select count (*) from test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6" ]] || false
}
