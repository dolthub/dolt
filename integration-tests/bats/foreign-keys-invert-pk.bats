#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql <<SQL
create table a (x int, y int, primary key (y,x));
create table b (x int, y int, primary key (y,x), foreign key (y) references a(y) on update cascade on delete cascade);
insert into a values (4,0), (3,1), (2,2);
insert into b values (2,1), (4,2), (3,0);
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "foreign-keys-invert-pk: test commit check pass" {
    dolt commit -am "cm"
}

@test "foreign-keys-invert-pk: check referential integrity on merge" {
    dolt commit -am "main"
    dolt checkout -b feat
    dolt sql <<SQL
set FOREIGN_KEY_CHECKS = 0;
insert into b values (1,3);
SQL
    dolt commit -am "feat"

    dolt checkout main

    run dolt merge feat
    run dolt constraints verify --all
    [ "$status" -eq "1" ]

    run dolt sql -q "SELECT * FROM dolt_constraint_violations" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "table,num_violations" ]] || false
    [[ "$output" =~ "b,1" ]] || false
}

@test "foreign-keys-invert-pk: test bad insert" {
    run dolt sql -q "insert into b values (1,3)"
    [ "$status" -eq 1 ]
}

@test "foreign-keys-invert-pk: test update" {
    dolt sql -q "update a set y = -1 where y = 0"
    run dolt sql -q "select * from b" -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "x,y" ]] || false
    [[ "$output" =~ "3,-1" ]] || false
    [[ "$output" =~ "2,1" ]] || false
    [[ "$output" =~ "4,2" ]] || false
}

@test "foreign-keys-invert-pk: test delete" {
    dolt sql -q "delete from a where y = 0"
    run dolt sql -q "select * from b" -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "x,y" ]] || false
    [[ "$output" =~ "2,1" ]] || false
    [[ "$output" =~ "4,2" ]] || false
}
