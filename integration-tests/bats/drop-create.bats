#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "drop-create: same schema and data" {
    dolt sql --disable-batch <<SQL
create table test(a int primary key, b int null);
insert into test values (1,1), (2,2);
select dolt_commit("-am", "table with two rows");
SQL

    dolt sql -q "drop table test"

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false

    dolt sql --disable-batch <<SQL
create table test(a int primary key, b int null);
insert into test values (1,1), (2,2);
SQL

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "drop-create: same schema and data, commit after drop" {
    dolt sql --disable-batch <<SQL
create table test(a int primary key, b int null);
insert into test values (1,1), (2,2);
select dolt_commit("-am", "table with two rows");
SQL

    dolt sql -q "drop table test"
    dolt commit -am "deleted table"
    
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false

    dolt sql --disable-batch <<SQL
create table test(a int primary key, b int null);
insert into test values (1,1), (2,2);
SQL

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table" ]] || false
    [[ "$output" =~ "test" ]] || false

    run dolt diff HEAD~
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "added table" ]] || false
    [[ ! "$output" =~ "deleted table" ]] || false
}

@test "drop-create: added column" {
    dolt sql --disable-batch <<SQL
create table test(a int primary key, b int null);
insert into test values (1,1), (2,2);
select dolt_commit("-am", "table with two rows");
SQL

    dolt sql -q "drop table test"

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false

    dolt sql --disable-batch <<SQL
create table test(a int primary key, b int null, c int null);
insert into test(a,b) values (1,1), (2,2);
SQL

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}
