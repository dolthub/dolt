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

    dolt sql <<SQL
create table xy (x int primary key, y int, key (y,x));
create table ab (a int primary key, b int, key (b,a));
SQL

    cd $TMPDIRS
}

teardown() {
    teardown_common
    stop_sql_server 1
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "stats: empty initial stats" {
    cd repo2

    # disable bootstrap, can only make stats with ANALYZE or background thread
    dolt sql -q "set @@PERSIST.dolt_stats_bootstrap_enabled = 0;"

    dolt sql -q "insert into xy values (0,0), (1,1)"

    start_sql_server
    sleep 1
    stop_sql_server

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "0" ]

    # setting variables doesn't hang or error
    dolt sql -q "set @@PERSIST.dolt_stats_auto_refresh_enabled = 1;"
    dolt sql -q "set @@PERSIST.dolt_stats_auto_refresh_threshold = .5"
    dolt sql -q "set @@PERSIST.dolt_stats_auto_refresh_interval = 1;"

    # auto refresh initialize at server startup
    start_sql_server

    # need to trigger at least one refresh cycle
    sleep 1

    # only statistics for non-empty tables are collected
    run dolt sql -r csv -q "select database_name, table_name, index_name from dolt_statistics order by index_name"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "database_name,table_name,index_name" ]
    [ "${lines[1]}" = "repo2,xy,primary" ]
    [ "${lines[2]}" = "repo2,xy,y" ]

    # appending new chunks picked up
    dolt sql -q "insert into xy select x, 1 from (with recursive inputs(x) as (select 4 union select x+1 from inputs where x < 1000) select * from inputs) dt;"

    sleep 1

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "8" ]

    # updates picked up
    dolt sql -q "update xy set y = 2 where x between 100 and 800"

    sleep 1

    dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "8" ]
}

@test "stats: bootrap on server startup" {
    cd repo2

    dolt sql -q "insert into xy values (0,0), (1,1)"

    start_sql_server
    stop_sql_server

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "2" ]
}

@test "stats: only bootrap server startup" {
    cd repo2

    dolt sql -q "insert into xy values (0,0), (1,1)"

    dolt gc

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "0" ]
}

@test "stats: encode/decode loop is delimiter safe" {
    cd repo2

dolt sql <<EOF
create table uv (u varbinary(255) primary key);
insert into uv values ('hello, world');
analyze table uv;
EOF

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "1" ]
}

@test "stats: correct stats directory location, issue#8324" {
    cd repo2

    dolt sql -q "insert into xy values (0,0), (1,1)"

    dolt sql -q "call dolt_stats_restart()"

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]

    run stat .dolt/repo2
    [ "$status" -eq 1 ]
}

@test "stats: restart in shell doesn't drop db, issue#8345" {
    cd repo2

    dolt sql -q "insert into xy values (0,0), (1,1), (2,2), (3,3), (4,4)"
    dolt sql -q "insert into ab values (0,0), (1,1), (2,2), (3,3), (4,4)"
    dolt sql -q "ANALYZE table xy, ab"
    run dolt sql -r csv <<EOF
select count(*) from dolt_statistics;
set @@GLOBAL.dolt_stats_auto_refresh_interval = 2;
call dolt_stats_restart();
select count(*) from dolt_statistics;
select sleep(3);
select count(*) from dolt_statistics;
EOF
    [ "${lines[1]}" = "4" ]
    [ "${lines[5]}" = "4" ]
    [ "${lines[9]}" = "4" ]
    [ "$status" -eq 0 ]
}

@test "stats: stats roundtrip restart" {
    cd repo2

    dolt sql -q "set @@PERSIST.dolt_stats_bootstrap_enabled = 0;"
    dolt sql -q "set @@PERSIST.dolt_stats_auto_refresh_interval = 1;"

    dolt sql -q "insert into xy values (0,0), (1,1)"

    # make sure no stats
    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "0" ]

    # add stats while server is running
    start_sql_server
    dolt sql -q "call dolt_stats_restart()"

    sleep 1

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "2" ]
    stop_sql_server

    # make sure restarted server sees same stats
    start_sql_server
    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "2" ]
    stop_sql_server
}

@test "stats: deletes refresh" {
    cd repo2

    dolt sql -q "insert into xy select x, 1 from (with recursive inputs(x) as (select 4 union select x+1 from inputs where x < 1000) select * from inputs) dt;"

    # setting variables doesn't hang or error
    dolt sql -q "set @@persist.dolt_stats_auto_refresh_enabled = 1;"
    dolt sql -q "set @@persist.dolt_stats_auto_refresh_threshold = .5"
    dolt sql -q "set @@persist.dolt_stats_auto_refresh_interval = 1;"

    start_sql_server

    sleep 1

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "8" ]

    # delete >50% of rows
    dolt sql -q "delete from xy where x > 600"

    sleep 1

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "4" ]
}

@test "stats: add/delete table" {
    cd repo1

    dolt sql -q "insert into ab values (0,0), (1,0), (2,0)"

    # setting variables doesn't hang or error
    dolt sql -q "SET @@persist.dolt_stats_auto_refresh_enabled = 1;"
    dolt sql -q "SET @@persist.dolt_stats_auto_refresh_threshold = .5"
    dolt sql -q "SET @@persist.dolt_stats_auto_refresh_interval = 1;"

    start_sql_server

    sleep 1

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "2" ]

    # add table
    dolt sql -q "create table xy (x int primary key, y int)"
    # schema changes don't impact the table hash
    dolt sql -q "insert into xy values (0,0)"

    sleep 1

    run dolt sql -r csv -q "select count(*) from dolt_statistics where table_name = 'xy'"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "1" ]

    dolt sql -q "truncate table xy"

    sleep 1

    dolt sql -q "select * from xy"

    dolt sql -q "select * from dolt_statistics where table_name = 'xy'"

    run dolt sql -r csv -q "select count(*) from dolt_statistics where table_name = 'xy'"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "0" ]

    dolt sql -q "drop table xy"

    run dolt sql -r csv -q "select count(*) from dolt_statistics where table_name = 'xy'"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "0" ]
}

@test "stats: add/delete index" {
    cd repo2

    dolt sql -q "insert into xy values (0,0), (1,0), (2,0)"

    # setting variables doesn't hang or error
    dolt sql -q "SET @@persist.dolt_stats_auto_refresh_enabled = 1;"
    dolt sql -q "SET @@persist.dolt_stats_auto_refresh_threshold = .5"
    dolt sql -q "SET @@persist.dolt_stats_auto_refresh_interval = 1;"

    start_sql_server

    sleep 1

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "2" ]

    # delete secondary
    dolt sql -q "alter table xy drop index y"
    # schema changes don't impact the table hash
    dolt sql -q "insert into xy values (3,0)"

    sleep 1

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "1" ]

    dolt sql -q "alter table xy add index yx (y,x)"
    # row change to impact table hash
    dolt sql -q "insert into xy values (4,0)"

    sleep 1

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "2" ]
}

@test "stats: most common values" {
    cd repo2

    dolt sql -q "alter table xy add index y2 (y)"
    dolt sql -q "insert into xy values (0,0), (1,0), (2,0), (3,0), (4,0), (5,0), (6,1), (7,1), (8,1), (9,1),(10,3),(11,4),(12,5),(13,6),(14,7),(15,8),(16,9),(17,10),(18,11)"

    dolt sql -q "analyze table xy"

    run dolt sql -r csv -q "select mcv1, mcv2 from dolt_statistics where index_name = 'y2'"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "1,0" ]
}

@test "stats: multi db" {
    cd repo1

    dolt sql -q "insert into ab values (0,0), (1,1)"

    cd ../repo2

    dolt sql -q "insert into ab values (0,0), (1,1)"
    dolt sql -q "insert into xy values (0,0), (1,1)"

    cd ..

    dolt sql -q "SET @@persist.dolt_stats_auto_refresh_enabled = 1;"
    dolt sql -q "SET @@persist.dolt_stats_auto_refresh_threshold = 0.5"
    dolt sql -q "SET @@persist.dolt_stats_auto_refresh_interval = 1;"

    start_sql_server
    sleep 1

    dolt sql -q "use repo1"
    run dolt sql -r csv -q "select database_name, table_name, index_name from dolt_statistics order by index_name"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "database_name,table_name,index_name" ]
    [ "${lines[1]}" = "repo1,ab,b" ]
    [ "${lines[2]}" = "repo1,ab,primary" ]

    run dolt sql -r csv -q "select database_name, table_name, index_name from repo2.dolt_statistics order by index_name"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "database_name,table_name,index_name" ]
    [ "${lines[1]}" = "repo2,ab,b" ]
    [ "${lines[2]}" = "repo2,ab,primary" ]
    [ "${lines[3]}" = "repo2,xy,primary" ]
    [ "${lines[4]}" = "repo2,xy,y" ]
}

@test "stats: add/delete database" {
    cd repo1

    # setting variables doesn't hang or error
    dolt sql -q "SET @@persist.dolt_stats_auto_refresh_enabled = 1;"
    dolt sql -q "SET @@persist.dolt_stats_auto_refresh_threshold = .5"
    dolt sql -q "SET @@persist.dolt_stats_auto_refresh_interval = 1;"

    start_sql_server

    dolt sql -q "insert into ab values (0,0), (1,0), (2,0)"
    dolt sql <<SQL
create database repo2;
create table repo2.xy (x int primary key, y int, key(y,x));
insert into repo2.xy values (0,0), (1,0), (2,0);
SQL

    sleep 1

    # specify database_name filter even though can only see active db stats
    run dolt sql -r csv <<SQL
use repo2;
select count(*) from dolt_statistics where database_name  = 'repo2';
SQL
    [ "$status" -eq 0 ]
    [ "${lines[2]}" = "2" ]

    # drop repo2
    dolt sql -q "drop database repo2"

    sleep 1

    # we can't access repo2 stats, but still try
    run dolt sql -r csv <<SQL
select count(*) from dolt_statistics where database_name = 'repo2';
SQL
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "0" ]

    dolt sql <<SQL
create database repo2;
create table repo2.xy (x int primary key, y int, key(y,x));
SQL

    sleep 1

    # no rows yet
    run dolt sql -r csv <<SQL
use repo2;
select count(*) from dolt_statistics where database_name = 'repo2';
SQL
    [ "$status" -eq 0 ]
    [ "${lines[2]}" = "0" ]

    dolt sql <<SQL
use repo2;
insert into xy values (0,0);
analyze table xy;
SQL

    sleep 1

    # insert initializes stats
    run dolt sql -r csv <<SQL
use repo2;
select count(*) from dolt_statistics where database_name = 'repo2';
SQL
    [ "$status" -eq 0 ]
    [ "${lines[2]}" = "2" ]
}

# bats test_tags=no_lambda
@test "stats: boostrap abort over 1mm rows" {
    cat <<EOF > data.py
import random
import os

rows = 2*1000*1000+1

def main():
    f = open("data.csv","w+")
    f.write("id,hostname\n")

    for i in range(rows):
        hostname = random.getrandbits(100)
        f.write(f"{i},{hostname}\n")
        if i % (500*1000) == 0:
            print("row :", i)
            f.flush()

    f.close()

if __name__ == "__main__":
    main()
EOF

    mkdir repo3
    cd repo3
    python3 ../data.py

    dolt init
    dolt sql -q "create table f (id int primary key, hostname int)"
    dolt table import -u --continue f data.csv

    dolt sql -q "set @@PERSIST.dolt_stats_bootstrap_enabled = 1;"

    run dolt sql -r csv -q "select count(*) from dolt_statistics"
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "stats bootstrap aborted" ]] || false
    [ "${lines[2]}" = "0" ]
}
