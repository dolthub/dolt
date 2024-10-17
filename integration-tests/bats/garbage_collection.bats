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
    wait $remotesrv_pid || :
    remotesrv_pid=""
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

@test "garbage_collection: call GC in sql script" {
    dolt sql <<SQL
CREATE TABLE t (pk int primary key);
INSERT INTO t VALUES (1),(2),(3);
CALL dolt_commit('-Am', 'new table with three rows');
INSERT INTO t VALUES (11),(12),(13);
SQL
    dolt reset --hard
    dolt sql <<SQL
INSERT INTO t VALUES (21),(22),(23);
CALL dolt_commit('-Am', 'new table with three rows');
CALL dolt_gc();
SQL
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
    run dolt merge other -m "merge"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT (content):" ]] || false

    run dolt sql -r csv -q "select base_pk, base_c0, our_pk, our_c0, their_pk, their_c0 from dolt_conflicts_test;"
    [ $status -eq 0 ]
    [[ "$output" =~ ",,0,10,0,20" ]] || false
    [[ "$output" =~ ",,1,11,1,21" ]] || false
    [[ "$output" =~ ",,2,12,2,22" ]] || false

    dolt gc

    run dolt sql -r csv -q "select base_pk, base_c0, our_pk, our_c0, their_pk, their_c0 from dolt_conflicts_test;"
    [ $status -eq 0 ]
    [[ "$output" =~ ",,0,10,0,20" ]] || false
    [[ "$output" =~ ",,1,11,1,21" ]] || false
    [[ "$output" =~ ",,2,12,2,22" ]] || false
}

@test "garbage_collection: leave constraint violations" {
    setup_merge_with_cv
    run dolt merge other -m "merge"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONSTRAINT VIOLATION (content):" ]] || false

    run dolt sql -r csv -q "select pk, fk from dolt_constraint_violations_child;"
    [ $status -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false

    dolt gc

    run dolt sql -r csv -q "select pk, fk from dolt_constraint_violations_child;"
    [ $status -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false
}

@test "garbage_collection: leave merge commit" {
    setup_merge
    run dolt merge other -m "merge"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT (content):" ]] || false

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
    run dolt merge other -m "merge"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT (content):" ]] || false

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

    run dolt merge other -m "merge"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT (content):" ]] || false
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

create_many_commits() {
        dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
CALL DOLT_COMMIT('-Am', 'Create test table');
SQL
    
    # Create a lot of commits to create some conjoin garbage
    NUM_COMMITS=250

    for i in $(eval echo "{1..$NUM_COMMITS}")
    do
        dolt sql <<SQL
INSERT INTO test VALUES ($i);
CALL DOLT_COMMIT('-am', 'Add new val $i');
SQL
    done

    run dolt sql -q "select count(*) from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$NUM_COMMITS" ]] || false
}

skip_if_chunk_journal() {
    if test -f "./.dolt/noms/vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"; then
        skip "chunk journal doesn't generate enough garbage"
    fi
}

@test "garbage_collection: shallow gc" {
    skip_if_chunk_journal
    create_many_commits

    # leave data in the working set
    dolt sql -q "INSERT INTO test VALUES ($(($NUM_COMMITS+1))),($(($NUM_COMMITS+2))),($(($NUM_COMMITS+3)));"

    # write a garbage file which looks like an old table file
    for i in `seq 0 100`; do
        dolt --help >> .dolt/noms/b0f6n6b1ej7a9ovalt0rr80bsentq807
    done

    BEFORE=$(du -c .dolt/noms/ | grep total | sed 's/[^0-9]*//g')
    run dolt gc --shallow
    [ "$status" -eq 0 ]

    run dolt sql -q "select count(*) from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$(($NUM_COMMITS+3))" ]] || false

    AFTER=$(du -c .dolt/noms/ | grep total | sed 's/[^0-9]*//g')

    # assert space was reclaimed
    echo "$BEFORE"
    echo "$AFTER"
    [ "$BEFORE" -gt "$AFTER" ]
}

@test "garbage_collection: online gc" {
    skip "dolt_gc is currently disabled"

    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO test VALUES (1),(2),(3),(4),(5);
CALL DOLT_COMMIT('-Am', 'added values 1-5');
INSERT INTO test VALUES (6),(7),(8);
CALL DOLT_RESET('--hard');
INSERT INTO test VALUES (11),(12),(13),(14),(15);
SQL

    BEFORE=$(du -c .dolt/noms/ | grep total | sed 's/[^0-9]*//g')
    run dolt sql -q "call dolt_gc();"
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

@test "garbage_collection: online shallow gc" {
    skip "dolt_gc is currently disabled"

    skip_if_chunk_journal
    create_many_commits

    # leave data in the working set
    dolt sql -q "INSERT INTO test VALUES ($(($NUM_COMMITS+1))),($(($NUM_COMMITS+2))),($(($NUM_COMMITS+3)));"

    BEFORE=$(du -c .dolt/noms/ | grep total | sed 's/[^0-9]*//g')
    run dolt sql -q "call dolt_gc('--shallow');"
    [ "$status" -eq 0 ]

    run dolt sql -q "select count(*) from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$(($NUM_COMMITS+3))" ]] || false

    AFTER=$(du -c .dolt/noms/ | grep total | sed 's/[^0-9]*//g')

    # assert space was reclaimed
    echo "$BEFORE"
    echo "$AFTER"
    [ "$BEFORE" -gt "$AFTER" ]
}

@test "garbage_collection: dolt gc --full" {
    # Create a lot of data on a new branch.
    dolt checkout -b to_keep
    dolt sql -q "CREATE TABLE vals (val LONGTEXT);"
    str="hex(random_bytes(1024))"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    dolt sql -q "INSERT INTO vals VALUES (concat($str));"
    dolt sql -q "INSERT INTO vals VALUES (concat($str));"
    dolt sql -q "INSERT INTO vals VALUES (concat($str));"
    dolt sql -q "INSERT INTO vals VALUES (concat($str));"

    dolt commit -Am 'create some data on a new commit.'

    # Create a lot of data on another new branch.
    dolt checkout main
    dolt checkout -b to_delete
    dolt sql -q "CREATE TABLE vals (val LONGTEXT);"
    str="hex(random_bytes(1024))"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    str="$str,$str"
    dolt sql -q "INSERT INTO vals VALUES (concat($str));"
    dolt sql -q "INSERT INTO vals VALUES (concat($str));"
    dolt sql -q "INSERT INTO vals VALUES (concat($str));"
    dolt sql -q "INSERT INTO vals VALUES (concat($str));"

    dolt commit -Am 'create some data on a new commit.'

    # GC it into the old gen.
    dolt gc

    # Get repository size. Note, this is in 512 byte blocks.
    BEFORE=$(du -c .dolt/noms/ | grep total | sed 's/[^0-9]*//g')

    # Delete the branch with all the data.
    dolt checkout main
    dolt branch -D to_delete

    # Check that a regular GC does not delete this data.
    dolt gc
    AFTER=$(du -c .dolt/noms/ | grep total | sed 's/[^0-9]*//g')
    [ $(($BEFORE - $AFTER)) -lt 16 ]

    # Check that a full GC does delete this data.
    # NOTE: We create and drop the tmp table here to get around Dolt's "GC is
    # a no-op if there have been no writes since the last GC" check.
    dolt sql -q 'create table tmp (id int); drop table tmp;'
    dolt gc --full
    AFTER=$(du -c .dolt/noms/ | grep total | sed 's/[^0-9]*//g')
    [ $(($BEFORE - $AFTER)) -gt 8192 ] # Reclaim at least 4MBs, in 512-byte blocks.

    # Sanity check that the stuff on to_keep is still accessible.
    dolt checkout to_keep
    dolt sql -q 'select length(val) from vals;'
}
