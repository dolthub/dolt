#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "create-views: create a single view" {
    run dolt sql <<SQL
create view four as select 2+2 as res from dual;
select * from four;
SQL
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "${lines[1]}" =~ ' res ' ]] || false
    [[ "${lines[3]}" =~ ' 4 ' ]] || false
}

@test "create-views: drop a single view" {
    run dolt sql <<SQL
create view four as select 2+2 as res from dual;
drop view four;
SQL
    [ "$status" -eq 0 ]
}

@test "create-views: can't drop dolt_schemas" {
    run dolt sql -q "create view four as select 2+2 as res from dual;"
    [ "$status" -eq 0 ]
    run dolt sql -q "select name from dolt_schemas" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ 'four' ]] || false
    run dolt sql -q "drop table dolt_schemas"
    skip "dropping dolt_schemas is currently unprotected"
    [ "$status" -ne 0 ]
    run dolt sql -q "select name from dolt_schemas" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ 'four' ]] || false
}

@test "create-views: join two views" {
    run dolt sql <<SQL
create view four as select 2+2 as res from dual;
create view now as select now() from dual;
select * from four, now;
SQL
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "${lines[1]}" =~ ' res ' ]] || false
    [[ "${lines[3]}" =~ ' 4 ' ]] || false
}

@test "create-views: cannot create view referencing non-existant table" {
    run dolt sql <<SQL
create view broken as select id from my_users;
SQL
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found: my_users" ]] || false
}

@test "create-views: can create view with escaped name" {
    run dolt sql -q 'create table `my-users` (id int primary key);'
    [ "$status" -eq 0 ]

    dolt sql -q 'insert into `my-users` (id) values (0);'

    run dolt sql -q 'create view `will-work` as select id from `my-users`;'
    [ "$status" -eq 0 ]

    run dolt sql -q 'select * from `will-work`;'
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "panic" ]] || false
}

@test "create-views: can create view referencing table" {
    run dolt sql <<SQL
create table my_users (id int primary key);
create view will_work as select id from my_users;
SQL
    [ "$status" -eq 0 ]
}

@test "create-views: can drop table with view referencing it" {
    run dolt sql <<SQL
create table my_users (id int primary key);
create view will_be_broken as select id from my_users;
drop table my_users;
SQL
    [ "$status" -eq 0 ]
}

@test "create-views: view referencing table selects values present when it was created" {
    run dolt sql <<SQL
create table my_users (id int primary key);
insert into my_users values (1), (2), (3);
create view my_users_view as select id from my_users order by id asc;
select * from my_users_view;
SQL
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    [[ "${lines[1]}" =~ ' id ' ]] || false
    [[ "${lines[3]}" =~ ' 1 ' ]] || false
    [[ "${lines[4]}" =~ ' 2 ' ]] || false
    [[ "${lines[5]}" =~ ' 3 ' ]] || false
}

@test "create-views: view referencing table selects values inserted after it was created" {
    run dolt sql <<SQL
create table my_users (id int primary key);
insert into my_users values (1), (2), (3);
create view my_users_view as select id from my_users order by id asc;
insert into my_users values (4), (5), (6);
select * from my_users_view;
SQL
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

@test "create-views: select view with alias" {
    run dolt sql <<SQL
create table my_users (id int primary key);
insert into my_users values (1), (2), (3);
create view my_users_view as select id from my_users order by id asc;
insert into my_users values (4), (5), (6);
select v.* from my_users_view as V;
SQL
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

@test "create-views: selecting from broken view fails" {
    run dolt sql <<SQL
create table my_users (id int primary key);
create view will_be_broken as select id from my_users;
drop table my_users;
select * from will_be_broken;
SQL
    [ "$status" -eq 1 ]
}

@test "create-views: creating view creates creates dolt_schemas table" {
    run dolt sql -q 'create view testing as select 2+2 from dual'
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "${lines[3]}" =~ 'new table:' ]] || false
    [[ "${lines[3]}" =~ ' dolt_schemas' ]] || false
    run dolt sql -q "select * from dolt_schemas"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "select 2+2 from dual" ]] || false
}

@test "create-views: created view is queryable from next session" {
    run dolt sql -q 'create view testing as select 2+2 from dual'
    [ "$status" -eq 0 ]
    run dolt sql -q 'select * from testing'
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "${lines[1]}" =~ '2 + 2' ]] || false
    [[ "${lines[3]}" =~ ' 4 ' ]] || false
}

@test "create-views: created view is droppable from next session" {
    run dolt sql -q 'create view testing as select 2+2 from dual'
    [ "$status" -eq 0 ]
    run dolt sql -q 'drop view testing'
    [ "$status" -eq 0 ]
    run dolt sql -q 'select * from testing'
    [ "$status" -eq 1 ]
}

@test "create-views: database with broken view can be used" {
    run dolt sql -q 'create table users (id longtext primary key)'
    [ "$status" -eq 0 ]
    run dolt sql -q 'create view all_users as select * from users'
    [ "$status" -eq 0 ]
    run dolt sql -q 'select * from all_users'
    [ "$status" -eq 0 ]
    run dolt sql -q 'drop table users'
    [ "$status" -eq 0 ]
    run dolt sql -q 'select 2+2 from dual'
    [ "$status" -eq 0 ]
    run dolt sql -q 'select * from all_users'
    [ "$status" -eq 1 ]
    [[ "${lines[0]}" =~ "table not found: users" ]] || false
    run dolt sql -q 'drop view all_users'
    [ "$status" -eq 0 ]
}

@test "create-views: Use a committed view" {
    dolt sql -q "create view four as select 2+2 as res from dual"
    dolt add .
    dolt commit -m "Checked in a view"
    run dolt sql -q "select * from four"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "${lines[1]}" =~ ' res ' ]] || false
    [[ "${lines[3]}" =~ ' 4 ' ]] || false
    run dolt sql -q "select * from dolt_schemas"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "four" ]] || false
    dolt sql -q "create view five as select 2+3 as res from dual"
    run dolt sql -q "select * from dolt_schemas"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "five" ]] || false
    dolt reset --hard
    run dolt sql -q "select * from dolt_schemas"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ ! "$output" =~ "five" ]] || false
    run dolt sql -q "select * from five"
    [ "$status" -eq 1 ]
    [[ "${lines[0]}" =~ "table not found: five" ]] || false
}
