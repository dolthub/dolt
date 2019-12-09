#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "create a single view" {
    run dolt sql <<< '
create view four as select 2+2 as res from dual;
select * from four'
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "${lines[1]}" =~ ' res ' ]] || false
    [[ "${lines[3]}" =~ ' 4 ' ]] || false
}

@test "drop a single view" {
    run dolt sql <<< '
create view four as select 2+2 as res from dual;
drop view four'
    [ "$status" -eq 0 ]
}

@test "join two views" {
    run dolt sql <<< '
create view four as select 2+2 as res from dual;
create view now as select now() from dual;
select * from four, now'
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "${lines[1]}" =~ ' res ' ]] || false
    [[ "${lines[3]}" =~ ' 4 ' ]] || false
}

@test "cannot create view referencing non-existant table" {
    run dolt sql <<< '
create view broken as select id from my_users'
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found: my_users" ]] || false
}

@test "can create view referencing table" {
    run dolt sql <<< '
create table my_users (id int primary key);
create view will_be_broken as select id from my_users'
    [ "$status" -eq 0 ]
}

@test "can drop table with view referencing it" {
    run dolt sql <<< '
create table my_users (id int primary key);
create view will_be_broken as select id from my_users;
drop table my_users'
    [ "$status" -eq 0 ]
}

@test "view referencing table selects values present when it was created" {
    run dolt sql <<< '
create table my_users (id int primary key);
insert into my_users values (1), (2), (3);
create view my_users_view as select id from my_users order by id asc;
select * from my_users_view'
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    [[ "${lines[1]}" =~ ' id ' ]] || false
    [[ "${lines[3]}" =~ ' 1 ' ]] || false
    [[ "${lines[4]}" =~ ' 2 ' ]] || false
    [[ "${lines[5]}" =~ ' 3 ' ]] || false
}

@test "view referencing table selects values inserted after it was created" {
    run dolt sql <<< '
create table my_users (id int primary key);
insert into my_users values (1), (2), (3);
create view my_users_view as select id from my_users order by id asc;
insert into my_users values (4), (5), (6);
select * from my_users_view'
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[1]}" =~ ' id ' ]] || false
    [[ "${lines[3]}" =~ ' 1 ' ]] || false
    [[ "${lines[4]}" =~ ' 2 ' ]] || false
    [[ "${lines[5]}" =~ ' 3 ' ]] || false
    [[ "${lines[6]}" =~ ' 4 ' ]] || false
    [[ "${lines[7]}" =~ ' 5 ' ]] || false
    [[ "${lines[8]}" =~ ' 6 ' ]] || false
}

@test "selecting from broken view fails" {
    run dolt sql <<< '
create table my_users (id int primary key);
create view will_be_broken as select id from my_users;
drop table my_users;
select * from will_be_broken'
    [ "$status" -eq 1 ]
}
