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
    [ "${#lines[@]}" -eq 5 ]
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
    [ "$status" -ne 0 ]
    run dolt sql -q "select name from dolt_schemas" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ 'four' ]] || false
}

@test "create-views: can't alter dolt_schemas" {
    run dolt sql -q "create view four as select 2+2 as res from dual;"
    [ "$status" -eq 0 ]
    run dolt sql -q "select name from dolt_schemas" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ 'four' ]] || false

    run dolt sql -q "alter table dolt_schemas add column newcol int"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "cannot be altered" ]] || false
    
    run dolt sql -q "select name from dolt_schemas" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ 'four' ]] || false
}

@test "create-views: drop and create same view" {
    dolt sql -q "create view four as select 2+2 as res from dual;"
    dolt sql -q "create view six as select 3+3 as res from dual;"
    
    run dolt sql -q "select name from dolt_schemas order by name" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ 'four' ]] || false
    [[ "${lines[2]}" =~ 'six' ]] || false

    dolt commit -Am "new views"
    dolt sql -q "drop view four"

    run dolt sql -q "create view four as select 2+2 as res from dual;"
    [ "$status" -eq 0 ]

    run dolt diff
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

@test "create-views: drop all views" {
    dolt sql -q "create view four as select 2+2 as res from dual;"
    dolt sql -q "create view six as select 3+3 as res from dual;"
    
    run dolt sql -q "select name from dolt_schemas order by name" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ 'four' ]] || false
    [[ "${lines[2]}" =~ 'six' ]] || false

    dolt commit -Am "new views"
    dolt sql -q "drop view four"

    run dolt ls --all
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt_schemas" ]] || false

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "-create view four as select 2+2 as res from dual" ]] || false
    [[ ! "$output" =~ "dolt_schemas" ]] || false

    dolt commit -Am "dropped a view"
    dolt sql -q "drop view six"

    # Dropping all views should result in the dolt_schemas table deleting itself
    run dolt ls --all
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "dolt_schemas" ]] || false

    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "-create view six as select 3+3 as res from dual" ]] || false
    [[ ! "$output" =~ "deleted table" ]] || false

    dolt commit -Am "no views left"

    # Creating and then dropping a bunch of views should produce no diff
    dolt sql -q "create view four as select 2+2 as res from dual;"
    dolt sql -q "create view six as select 3+3 as res from dual;"
    dolt sql -q "drop view four"
    dolt sql -q "drop view six"

    run dolt diff
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]    
}

@test "create-views: join two views" {
    run dolt sql <<SQL
create view four as select 2+2 as res from dual;
create view curr as select now() from dual;
select * from four, curr;
SQL
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "${lines[1]}" =~ ' res ' ]] || false
    [[ "${lines[3]}" =~ ' 4 ' ]] || false
}

@test "create-views: cannot create view referencing non-existent table" {
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
    [ "${#lines[@]}" -eq 7 ]
    [[ "${lines[1]}" =~ ' id ' ]] || false
    [[ "${lines[3]}" =~ ' 1 ' ]] || false
    [[ "${lines[4]}" =~ ' 2 ' ]] || false
    [[ "${lines[5]}" =~ ' 3 ' ]] || false

    # check information_schema.VIEWS table
    # TODO: view_definition should be "select `mybin`.`my_users`.`id` AS `id` from `mybin`.`my_users` order by `mybin`.`my_users`.`id`"
    run dolt sql -q "select * from information_schema.VIEWS;" -r csv
    [[ "$output" =~ "def,dolt-repo-$$,my_users_view,select id from my_users order by id asc,NONE,YES,root@localhost,DEFINER,utf8mb4,utf8mb4_0900_bin" ]] || false
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
    [ "${#lines[@]}" -eq 10 ]
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
    [ "${#lines[@]}" -eq 10 ]
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
    [[ "${lines[1]}" =~ '2+2' ]] || false
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
    run dolt sql -q 'create table users (id varchar(20) primary key)'
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
    [[ "${lines[0]}" =~ "View 'dolt-repo-$$.all_users' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them" ]] || false
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

@test "create-views: AS OF" {
    dolt sql <<SQL
create table t1 (a int primary key, b int);
call dolt_add('.');
insert into t1 values (1,1);
call dolt_commit('-am', 'table with one row');
call dolt_branch('onerow');
insert into t1 values (2,2);
call dolt_commit('-am', 'table with two rows');
call dolt_branch('tworows');
create view v1 as select * from t1;
call dolt_add('.');
call dolt_commit('-am', 'view with select *');
call dolt_branch('view');
insert into t1 values (3,3);
call dolt_add('.');
call dolt_commit('-am', 'table with three rows');
call dolt_branch('threerows');
drop view v1;
create view v1 as select a+10, b+10 from t1;
SQL

    # should show the original view definition
    run dolt sql -r csv -q "select * from dolt_schemas as of 'view' order by 1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "select * from t1" ]] || false

    # should use the view definition from branch named, data from branch named
    run dolt sql -r csv -q "select * from \`dolt-repo-$$/view\`.v1 order by 1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "${lines[1]}" =~ "1,1" ]] || false
    [[ "${lines[2]}" =~ "2,2" ]] || false

    # should use the view definition from HEAD, data from branch named
    run dolt sql -r csv -q "select * from v1 as of 'view' order by 1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "${lines[1]}" =~ "11,11" ]] || false
    [[ "${lines[2]}" =~ "12,12" ]] || false
}   

@test "create-views: describe correctly works with complex views" {
    dolt sql -q "create table t(pk int primary key, val int)"
    dolt sql -q "create view view1 as select * from t"

    run dolt sql -r csv -q "describe view1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "${lines[1]}" =~ 'pk,int,NO,"",,""' ]] || false
    [[ "${lines[2]}" =~ 'val,int,YES,"",,""' ]] || false

    dolt sql -q "create view view2 as select pk from t"
    run dolt sql -r csv -q "describe view2"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[1]}" =~ 'pk,int,NO,"",,""' ]] || false

    dolt sql -q "create table t2(pk int primary key, val int)"
    dolt sql -q "insert into t values (1,1)"
    dolt sql -q "insert into t2 values (1,2)"

    dolt sql -q "create view view3 as select t.val as v1, t2.val as v2 from t inner join t2 on t.pk=t2.pk"
    run dolt sql -r csv -q "describe view3"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "${lines[1]}" =~ 'v1,int,YES,"",,""' ]] || false
    [[ "${lines[2]}" =~ 'v2,int,YES,"",,""' ]] || false
}

@test "create-views: can correctly alter a view" {
    skip "ALTER VIEW is unsupported"
    dolt sql -q "create table t(pk int primary key, val int)"
    dolt sql -q "create view view1 as select * from t"

    dolt sql -q "alter view view1 as select val from t"
}

@test "create-views: views get properly formatted in the information schema table" {
    skip "views are not correctly formatted right now"
    dolt sql -q "create table t(pk int primary key, val int)"
    dolt sql -q "create view view1 as select pk from t"

    DATABASE=$(dolt sql -r csv -q "SELECT DATABASE()" | sed -n 2p)
    run dolt sql -r csv -q "SELECT VIEW_DEFINITION FROM information_schema.views where TABLE_NAME='view1'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "VIEW_DEFINITION" ]] || false
    [[ "$output" =~ "select $DATABASE.t from $DATABASE.t" ]] || false
}
