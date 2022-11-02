#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

remotesrv_pid=
setup() {
    skiponwindows "tests are flaky on Windows"
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

@test "garbage_collection: dolt remotes server is running" {
    ps -p $remotesrv_pid | grep remotesrv
}

@test "garbage_collection: gc on empty dir" {
    dolt gc
    dolt gc
    dolt gc -s
}

@test "garbage_collection: smoke test" {
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

@test "garbage_collection: blob types work after GC" {
    dolt sql -q "create table t(pk int primary key, val text)"
    dolt sql -q "insert into t values (1, 'one'), (2, 'two');"
    dolt add -A && dolt commit -am "added a table with blob encoding"

    dolt gc

    run dolt sql -q "select * from t"
    [ $status -eq 0 ]
    [[ $output =~ "one" ]] || false
    [[ $output =~ "two" ]] || false
}

@test "garbage_collection: clone a remote" {
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO test VALUES (0),(1),(2);
SQL
    dolt add test
    dolt commit -m "test commit"
    dolt push test-remote main

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
    dolt push test-remote main

    # assert that the clone still works
    cd "dolt-repo-clones/test-repo"
    run dolt pull
    [ "$status" -eq 0 ]
    run dolt sql -q "select count (*) from test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6" ]] || false
}

@test "garbage_collection: leave committed and uncommitted data" {
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO test VALUES
    (1),(2),(3),(4),(5);
SQL
    dolt add .
    dolt commit -m "added values 1 - 5"

    # make some garbage
    dolt sql -q "INSERT INTO test VALUES (6),(7),(8);"
    dolt reset --hard

    # leave data in the working set
    dolt sql -q "INSERT INTO test VALUES (11),(12),(13),(14),(15);"

    BEFORE=$(du -c .dolt/noms/ | grep total | sed 's/[^0-9]*//g')

    run dolt gc
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT sum(pk) FROM test;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "80" ]] || false

    AFTER=$(du -c .dolt/noms/ | grep total | sed 's/[^0-9]*//g')

    # assert space was reclaimed
    echo "$BEFORE"
    echo "$AFTER"
    [ "$BEFORE" -gt "$AFTER" ]
}

setup_merge() {
    dolt sql -q "CREATE TABLE test (pk int PRIMARY KEY, c0 TEXT);"
    dolt sql -q "CREATE TABLE quiz (pk int PRIMARY KEY, c0 TEXT);"
    dolt add . && dolt commit -m "created tables test & quiz"
    dolt branch other

    dolt sql -q "INSERT INTO test VALUES (0,'10'),(1,'11'),(2,'12');"
    dolt commit -am "added rows on main"

    dolt checkout other
    dolt sql -q "INSERT INTO test VALUES (0,'20'),(1,'21'),(2,'22');"
    dolt commit -am "added rows on other"

    dolt checkout main
}

setup_merge_with_cv() {
     dolt sql -q "CREATE TABLE parent (pk int PRIMARY KEY);"
     dolt sql -q "CREATE TABLE child (pk int PRIMARY KEY, fk int, FOREIGN KEY (fk) REFERENCES parent (pk));"
     dolt sql -q "INSERT into parent VALUES (1);"
     dolt commit -Am "create tables and add parent"

     dolt checkout -b other
     dolt sql -q "insert into child values (1, 1);"
     dolt commit -am "add child"

     dolt checkout main
     dolt sql -q "delete from parent where pk = 1;"
     dolt commit -am "remove parent"
}

@test "garbage_collection: leave conflicts" {
    setup_merge
    dolt merge other -m "merge"

    run dolt sql -r csv -q "select base_pk, base_c0, our_pk, our_c0, their_pk, their_c0 from dolt_conflicts_test;"
    [ $status -eq 0 ]
    [[ "$output" =~ ",,0,10,0,20" ]]
    [[ "$output" =~ ",,1,11,1,21" ]]
    [[ "$output" =~ ",,2,12,2,22" ]]

    dolt gc

    run dolt sql -r csv -q "select base_pk, base_c0, our_pk, our_c0, their_pk, their_c0 from dolt_conflicts_test;"
    [ $status -eq 0 ]
    [[ "$output" =~ ",,0,10,0,20" ]]
    [[ "$output" =~ ",,1,11,1,21" ]]
    [[ "$output" =~ ",,2,12,2,22" ]]
}

@test "garbage_collection: leave constraint violations" {
    setup_merge_with_cv
    dolt merge other -m "merge"

    run dolt sql -r csv -q "select pk, fk from dolt_constraint_violations_child;"
    [ $status -eq 0 ]
    [[ "$output" =~ "1,1" ]]

    dolt gc

    run dolt sql -r csv -q "select pk, fk from dolt_constraint_violations_child;"
    [ $status -eq 0 ]
    [[ "$output" =~ "1,1" ]]
}

@test "garbage_collection: leave merge commit" {
    setup_merge
    dolt merge other -m "merge"

    dolt gc

    dolt conflicts resolve --ours .
    dolt add .
    dolt commit -am "resolved conflicts with ours"

    run dolt sql -q "SELECT * FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,10" ]] || false
    [[ "${lines[2]}" =~ "1,11" ]] || false
    [[ "${lines[3]}" =~ "2,12" ]] || false
}

@test "garbage_collection: leave merge commit with stored procedure" {
    setup_merge
    dolt merge other -m "merge"

    dolt gc

    dolt sql -q "call dolt_conflicts_resolve('--ours', '.')"
    dolt add .
    dolt commit -am "resolved conflicts with ours"

    run dolt sql -q "SELECT * FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,10" ]] || false
    [[ "${lines[2]}" =~ "1,11" ]] || false
    [[ "${lines[3]}" =~ "2,12" ]] || false
}

@test "garbage_collection: leave working pre-merge" {
    setup_merge

    # make a dirty working set with table quiz
    dolt sql -q "INSERT INTO quiz VALUES (9,99)"

    dolt merge other -m "merge"
    dolt gc
    run dolt merge --abort
    [ "$status" -eq 0 ]

    dolt sql -q "SELECT * FROM test;" -r csv
    run dolt sql -q "SELECT * FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,10" ]] || false
    [[ "${lines[2]}" =~ "1,11" ]] || false
    [[ "${lines[3]}" =~ "2,12" ]] || false
    dolt sql -q "SELECT * FROM quiz;" -r csv
    run dolt sql -q "SELECT * FROM quiz;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "9,99" ]] || false
}
