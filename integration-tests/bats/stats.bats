#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    skiponwindows "tests are flaky on Windows"
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test tests remote connections directly, SQL_ENGINE is not needed."
    fi

    setup_common

    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{repo1,repo2}

    cd $TMPDIRS/repo1
    dolt init

    dolt sql <<SQL
create table ab (a int primary key, b int, key (b,a));
SQL

    cd $TMPDIRS/repo2
    dolt init
    dolt sql -q "SET @@PERSIST.dolt_stats_job_interval = 100"

    dolt sql <<SQL
create table xy (x int primary key, y int, key (y,x));
create table ab (a int primary key, b int, key (b,a));
SQL

    dolt sql -q "set @@PERSIST.dolt_stats_job_interval = 1;"

    cd $TMPDIRS
}

teardown() {
    teardown_common
    stop_sql_server 1
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "stats: dolt_stats_once" {
    # running once populates stats and returns valid json response
    cd repo2
    dolt sql -q "insert into xy values (0,0), (1,1)"

    run dolt sql -r csv -q "call dolt_stats_once()"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '{""dbCnt"":1,""bucketWrites"":2,""tablesProcessed"":2,""tablesSkipped"":0}"' ]] || false
}


@test "stats: second once does no work" {
    # running once populates stats and returns valid json response
    cd repo2
    dolt sql -q "insert into xy values (0,0), (1,1)"

    run dolt sql -r csv -q "call dolt_stats_once(); call dolt_stats_once()"
    [ "$status" -eq 0 ]
    [[ "${lines[3]}" =~ '{""dbCnt"":1,""bucketWrites"":0,""tablesProcessed"":0,""tablesSkipped"":2}"' ]] || false
}

@test "stats: once after reload does no incremental work" {
    # running once populates stats and returns valid json response
    cd repo2
    dolt sql -q "insert into xy values (0,0), (1,1)"

    dolt sql -r csv -q "call dolt_stats_once();"
    run dolt sql -r csv -q "call dolt_stats_once();"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ '{""dbCnt"":1,""bucketWrites"":0,""tablesProcessed"":2,""tablesSkipped"":0}"' ]] || false
}

@test "stats: dolt_stats_wait" {
    # wait stalls until stats are ready
    cd repo2
    dolt sql -q "insert into xy values (0,0), (1,1)"

    run dolt sql -r csv <<EOF
call dolt_stats_restart();
call dolt_stats_wait();
select count(*) from dolt_statistics
EOF
    [ "$status" -eq 0 ]
    [ "${lines[5]}" = "2" ]
}

@test "stats: dolt_stats_info" {
    cd repo2
    dolt sql -q "insert into xy values (0,0), (1,1)"

    run dolt sql -r csv -q "call dolt_stats_once(); call dolt_stats_info('--short')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":1,""active"":false,""storageBucketCnt"":2,""cachedBucketCnt"":2,""cachedBoundCnt"":2,""cachedTemplateCnt"":4,""statCnt"":2,""backing"":""repo2""}"' ]] || false
}

@test "stats: dolt_stats_server_wait" {
    # wait stalls until stats are ready
    cd repo2
    dolt sql -q "insert into xy values (0,0), (1,1)"

    start_sql_server

    dolt sql -r csv -q "call dolt_stats_wait()"

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "2" ]
}

@test "stats: dolt_stats_server_paused" {
    cd repo2
    dolt sql -q "insert into xy values (0,0), (1,1)"

    dolt sql -q "set @@PERSIST.dolt_stats_paused = 1;"

    start_sql_server

    dolt sql -q "call dolt_stats_info('--short')"

    run dolt sql -r "call dolt_stats_wait()"
    [ "$status" -eq 1 ]
    run dolt sql -r "call dolt_stats_gc()"
    [ "$status" -eq 1 ]

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "0" ]
}

@test "stats: dolt_stats_purge" {
    # running once populates stats and returns valid json response
    cd repo2
    dolt sql -q "insert into xy values (0,0), (1,1)"

    run dolt sql -r csv -q "call dolt_stats_once(); call dolt_stats_purge(); call dolt_stats_info('--short')"
    [ "$status" -eq 0 ]
    [[ "${lines[5]}" =~ '"{""dbCnt"":0,""active"":false,""storageBucketCnt"":0,""cachedBucketCnt"":0,""cachedBoundCnt"":0,""cachedTemplateCnt"":0,""statCnt"":0,""backing"":""repo2""}"' ]] || false
}

@test "stats: dolt_stats_purge server" {
    cd repo2

    start_sql_server

    dolt sql -q "insert into xy values (0,0), (1,1)"
    dolt sql -q "call dolt_stats_wait()"
    dolt sql -q "call dolt_stats_stop()"
    dolt sql -q "call dolt_stats_purge()"
    run dolt sql -r csv -q "call dolt_stats_info('--short')"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ '"{""dbCnt"":0,""active"":false,""storageBucketCnt"":0,""cachedBucketCnt"":0,""cachedBoundCnt"":0,""cachedTemplateCnt"":0,""statCnt"":0,""backing"":""repo2""}"' ]] || false
}

@test "stats: dolt_stats_gc fails in shell" {
    cd repo2
    dolt sql <<SQL
insert into xy values (0,0), (1,1);
call dolt_stats_once();
insert into xy values (2,2), (3,3);
call dolt_stats_once();
SQL

    run dolt sql -q "dolt_stats_gc()"
    [ "$status" -eq 1 ]

    run dolt sql -r csv -q "call dolt_stats_info('--short')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":0,""active"":false,""storageBucketCnt"":4,""cachedBucketCnt"":0,""cachedBoundCnt"":0,""cachedTemplateCnt"":0,""statCnt"":0,""backing"":""repo2""}"' ]] || false
}

@test "stats: dolt_stats_gc server" {
    cd repo2

    # only user-triggered GC's
    dolt sql -q "SET @@PERSIST.dolt_stats_gc_enabled = 0"

    start_sql_server

    dolt sql -r csv <<SQL
insert into xy values (0,0), (1,1);
create table toDelete(i int primary key);
insert into toDelete values (5), (6);

-- invalidate previous xy buckets
call dolt_stats_wait();
call dolt_stats_info('--short');
insert into xy values (2,2), (3,3);

call dolt_add('-A');
call dolt_commit('-m', 'main branch');

-- mirror main
call dolt_checkout('-b', 'feat1');
call dolt_checkout('-b', 'feat2');

create database other;
use other;
create table ot (i int primary key);
insert into ot values (0), (1), (2);

call dolt_stats_wait();
call dolt_stats_info('--short');
SQL

    # starting point
    # dbs: repo2/[main, feat1, feat2], other/main
    # stats: repo2:[xy,ab,toDelete]*3, other:[ot]*1
    run dolt sql -r csv -q "call dolt_stats_info('--short');"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":4,""active"":true,""storageBucketCnt"":6,""cachedBucketCnt"":6,""cachedBoundCnt"":6,""cachedTemplateCnt"":6,""statCnt"":10,""backing"":""repo2""}"' ]] || false

    # clear invalid xy
    dolt sql -q "call dolt_stats_gc()"
    dolt sql -q "call dolt_stats_info('--short')"
    run dolt sql -r csv -q "call dolt_stats_info('--short')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":4,""active"":true,""storageBucketCnt"":4,""cachedBucketCnt"":4,""cachedBoundCnt"":4,""cachedTemplateCnt"":6,""statCnt"":10,""backing"":""repo2""}"' ]] || false

    # remove toDelete table from 2/3 branches and gc
    dolt sql -q "use repo2; call dolt_checkout('feat1'); drop table toDelete"
    dolt sql -q "use repo2; call dolt_checkout('main'); drop table toDelete"
    dolt sql -q "call dolt_stats_gc()"
    dolt sql -q "call dolt_stats_info('--short')"
    run dolt sql -r csv -q "call dolt_stats_info('--short')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":4,""active"":true,""storageBucketCnt"":4,""cachedBucketCnt"":4,""cachedBoundCnt"":4,""cachedTemplateCnt"":6,""statCnt"":8,""backing"":""repo2""}"' ]] || false

    # remove branch stats and gc
    dolt sql -q "use repo2; call dolt_branch('-D', 'feat1', 'feat2')"
    dolt sql -q "call dolt_stats_wait()"
    dolt sql -q "call dolt_stats_gc()"
    dolt sql -q "call dolt_stats_info('--short')"
    run dolt sql -r csv -q "call dolt_stats_info('--short')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":2,""active"":true,""storageBucketCnt"":3,""cachedBucketCnt"":3,""cachedBoundCnt"":3,""cachedTemplateCnt"":5,""statCnt"":3,""backing"":""repo2""}"' ]] || false

    # delete whole db and gc
    dolt sql -q "drop database other;"
    dolt sql -q "call dolt_stats_wait()"
    dolt sql -q "call dolt_stats_gc()"
    dolt sql -r csv -q "call dolt_stats_info('--short')"
    run dolt sql -r csv -q "call dolt_stats_info('--short')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":1,""active"":true,""storageBucketCnt"":2,""cachedBucketCnt"":2,""cachedBoundCnt"":2,""cachedTemplateCnt"":4,""statCnt"":2,""backing"":""repo2""}"' ]] || false
}

@test "stats: delete database clean swap" {
    # only user-triggered GC's
    dolt sql -q "SET @@PERSIST.dolt_stats_gc_enabled = 0"

    # don't start server in repo2, the shell->server access
    # breaks when you delete the primary database
    start_sql_server

    dolt sql -r csv <<SQL
use repo2;
insert into xy values (0,0), (1,1);

create database other;
use other;
create table ot (i int primary key);
insert into ot values (0), (1), (2);

call dolt_stats_wait();

use other;
drop database repo2;
drop database repo1;
call dolt_stats_gc();
SQL

    # other still exists
    dolt sql -q "call dolt_stats_info('--short');"
    run dolt sql -r csv -q "call dolt_stats_info('--short');"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":1,""active"":true,""storageBucketCnt"":1,""cachedBucketCnt"":1,""cachedBoundCnt"":1,""cachedTemplateCnt"":1,""statCnt"":1,""backing"":""other""}"' ]] || false
}

@test "stats: multiple stats dbs at start is OK" {
    cd repo2
    dolt sql -q "insert into xy values (0,0)"
    dolt sql -q "insert into ab values (0,0)"
    dolt sql -q "call dolt_stats_once()"

    cd ../repo1
    dolt sql -q "insert into ab values (0,0)"
    dolt sql -q "call dolt_stats_once()"

    cd ..
    start_sql_server

    dolt sql -q "call dolt_stats_wait();"
    dolt sql -q "call dolt_stats_info('--short');"
    run dolt sql -r csv -q "call dolt_stats_info('--short');"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":2,""active"":true,""storageBucketCnt"":2,""cachedBucketCnt"":2,""cachedBoundCnt"":2,""cachedTemplateCnt"":4,""statCnt"":3,""backing"":""repo1""}"' ]] || false
}

@test "stats: dolt_stats_stop_restart" {
    cd repo2
    dolt sql -q "insert into xy values (0,0), (1,1)"

    start_sql_server

    dolt sql -r csv -q "call dolt_stats_wait()"

    # server running stats by default
    dolt sql -q "call dolt_stats_info('--short')"
    run dolt sql -r csv -q "call dolt_stats_info('--short')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":1,""active"":true,""storageBucketCnt"":2,""cachedBucketCnt"":2,""cachedBoundCnt"":2,""cachedTemplateCnt"":4,""statCnt"":2,""backing"":""repo2""}"' ]] || false

    # stop turns stats off
    dolt sql -q "call dolt_stats_stop()"
    dolt sql -r csv -q "call dolt_stats_info('--short')"
    run dolt sql -r csv -q "call dolt_stats_info('--short')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":1,""active"":false,""storageBucketCnt"":2,""cachedBucketCnt"":2,""cachedBoundCnt"":2,""cachedTemplateCnt"":4,""statCnt"":2,""backing"":""repo2""}"' ]] || false


    # don't pick up changes when stopped
    dolt sql -q "insert into xy values (2,2), (4,4)"

    run dolt sql -r csv -q "call dolt_stats_wait()"
    [ "$status" -eq 1 ]

    run dolt sql -r csv -q "call dolt_stats_info('--short')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":1,""active"":false,""storageBucketCnt"":2,""cachedBucketCnt"":2,""cachedBoundCnt"":2,""cachedTemplateCnt"":4,""statCnt"":2,""backing"":""repo2""}"' ]] || false

    dolt sql -r csv -q "call dolt_stats_restart()"
    dolt sql -r csv -q "call dolt_stats_wait()"
    dolt sql -q "call dolt_stats_info('--short')"
    run dolt sql -r csv -q "call dolt_stats_info('--short')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":1,""active"":true,""storageBucketCnt"":4,""cachedBucketCnt"":4,""cachedBoundCnt"":4,""cachedTemplateCnt"":4,""statCnt"":2,""backing"":""repo2""}"' ]] || false
}

@test "stats: memory only doesn't write to disk" {
    cd repo2
    dolt sql -q "set @@PERSIST.dolt_stats_memory_only = 1"

    start_sql_server

    dolt sql -q "insert into xy values (0,0), (1,1)"
    dolt sql -q "call dolt_stats_once()"

    dolt sql -q "call dolt_stats_info('--short')"
    run dolt sql -r csv -q "call dolt_stats_info('--short')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":1,""active"":true,""storageBucketCnt"":0,""cachedBucketCnt"":2,""cachedBoundCnt"":2,""cachedTemplateCnt"":4,""statCnt"":2,""backing"":""memory""}"' ]] || false

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "2" ]

    stop_sql_server

    run dolt sql -r csv -q "call dolt_stats_once(); call dolt_stats_info('--short')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '"{""dbCnt"":1,""active"":false,""storageBucketCnt"":0,""cachedBucketCnt"":2,""cachedBoundCnt"":2,""cachedTemplateCnt"":4,""statCnt"":2,""backing"":""memory""}"' ]] || false
}


@test "stats: waiters error for closed stats queue" {
    cd repo2

    dolt sql -q "insert into xy values (0,0), (1,1)"
    dolt sql -q "analyze table xy"

    run dolt sql -q "call dolt_stats_gc()"
    [ "$status" -eq 1 ]

    run dolt sql -q "call dolt_stats_wait()"
    [ "$status" -eq 1 ]

    run dolt sql -q "call dolt_stats_flush()"
    [ "$status" -eq 1 ]
}

@test "stats: encode/decode loop is delimiter safe" {
    cd repo2

dolt sql <<EOF
create table uv (u varbinary(255) primary key);
insert into uv values ('hello, world');
EOF

    run dolt sql -r csv -q "call dolt_stats_once(); select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[3]}" = "1" ]
}

@test "stats: correct stats directory location, issue#8324" {
    cd repo2

    dolt sql -q "insert into xy values (0,0), (1,1)"

    dolt sql -q "call dolt_stats_restart()"

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]

    run stat .dolt/repo2
    [ "$status" -eq 1 ]

    run stat .dolt/stats/.dolt
    [ "$status" -eq 0 ]
}

@test "stats: restart in shell doesn't drop db, issue#8345" {
    cd repo2

    dolt sql -q "insert into xy values (0,0), (1,1), (2,2), (3,3), (4,4)"
    dolt sql -q "insert into ab values (0,0), (1,1), (2,2), (3,3), (4,4)"
    run dolt sql -r csv <<EOF
call dolt_stats_once();
select count(*) from dolt_statistics;
call dolt_stats_restart();
select count(*) from dolt_statistics;
call dolt_stats_wait();
select count(*) from dolt_statistics;
EOF
    [ "${lines[3]}" = "4" ]
    [ "${lines[7]}" = "4" ]
    [ "${lines[11]}" = "4" ]
    [ "$status" -eq 0 ]
}

@test "stats: most common values" {
    cd repo2

    dolt sql -q "alter table xy add index y2 (y)"
    dolt sql -q "insert into xy values (0,0), (1,0), (2,0), (3,0), (4,0), (5,0), (6,1), (7,1), (8,1), (9,1),(10,3),(11,4),(12,5),(13,6),(14,7),(15,8),(16,9),(17,10),(18,11)"

    run dolt sql -r csv -q "call dolt_stats_once(); select mcv1, mcv2 from dolt_statistics where index_name = 'y2'"
    [ "$status" -eq 0 ]
    [ "${lines[3]}" = "1,0" ]
}

@test "stats: stats delete index schema change" {
    cd repo2

    dolt sql -q "insert into xy values (0,0), (1,1)"

    # stats OK after analyze
    run dolt sql -r csv -q "call dolt_stats_once(); select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[3]}" = "2" ]

    dolt sql -q "alter table xy drop index y"

    # load after schema change should purge
    run dolt sql -r csv -q "call dolt_stats_once(); select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[3]}" = "1" ]
}

@test "stats: stats recreate table without index" {
    cd repo2

    dolt sql -q "insert into xy values (0,0), (1,1)"

    dolt sql -q "call dolt_stats_once()"

    dolt sql -q "drop table xy"
    dolt sql -q "create table xy (x int primary key, y int)"
    dolt sql -q "insert into xy values (0,0), (1,1)"

    # make sure no stats
    run dolt sql -r csv -q "call dolt_stats_once(); select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[3]}" = "1" ]
}
