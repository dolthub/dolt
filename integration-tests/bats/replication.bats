#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    TMPDIRS=$(pwd)/tmpdirs
    mkdir -p $TMPDIRS/{bac1,rem1,repo1}

    # repo1 -> bac1 -> repo2
    cd $TMPDIRS/repo1
    dolt init
    dolt branch feature
    dolt remote add backup1 file://../bac1
    dolt remote add remote1 file://../rem1
    dolt push remote1 main
    cd $TMPDIRS
}

teardown() {
    teardown_common
    rm -rf $TMPDIRS
    cd $BATS_TMPDIR
}

@test "replication: configuration errors" {
    cd repo1
    dolt sql -q "SET @@persist.dolt_read_replica_remote = 'doesNotExist';"
    
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "replication disabled" ]] || false

    dolt sql -q "SET @@persist.dolt_read_replica_remote = 'remote1';"
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "replication disabled" ]] || false

    dolt sql -q "SET @@persist.dolt_replicate_all_heads = 1";
    dolt sql -q "SET @@persist.dolt_replicate_heads = 'main';";
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "replication disabled" ]] || false

    dolt sql -q "SET @@persist.dolt_replicate_heads = '';";
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "replication disabled" ]] || false
}

@test "replication: default no replication" {
    cd repo1
    dolt sql -q "create table t1 (a int primary key)"
    dolt add .
    dolt commit -am "cm"

    [ ! -d "../bac1/.dolt" ] || false
}

@test "replication: push on cli commit" {

    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote backup1
    dolt sql -q "create table t1 (a int primary key)"
    dolt add .
    dolt commit -am "cm"

    cd ..
    run dolt clone file://./bac1 repo2
    [ "$status" -eq 0 ]

    cd repo2
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "replication: push on cli engine commit" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote backup1
    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "call dolt_add('.')"
    dolt sql -q "call dolt_commit('-am', 'cm')"

    cd ..
    dolt clone file://./bac1 repo2
    cd repo2
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "replication: push branch delete" {
    cd repo1
    dolt push remote1 feature
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt sql -q "call dolt_branch('-df', 'feature');"

    cd ..
    dolt clone file://./rem1 repo2
    cd repo2
    run dolt sql -q "select name from dolt_branches" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "main" ]] || false
    [[ ! "$output" =~ "feature" ]] || false
}

@test "replication: push tag delete, pull delete on read" {
    cd repo1
    dolt tag tag1
    dolt tag tag2
    dolt push remote1 tag1
    dolt push remote1 tag2

    cd ..
    dolt clone file://./rem1 repo2
    cd repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote origin
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1

    run dolt sql -q "select tag_name from dolt_tags" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tag1" ]] || false
    [[ "$output" =~ "tag2" ]] || false

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt tag -d tag1

    cd ../repo2
    run dolt sql -q "select tag_name from dolt_tags" -r csv
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "tag1" ]] || false
    [[ "$output" =~ "tag2" ]] || false
}

@test "replication: pull branch delete on read" {
    cd repo1
    dolt push remote1 feature

    cd ..
    dolt clone file://./rem1 repo2

    cd repo2
    dolt push origin :feature

    cd ../repo1
    run dolt sql -q "select name from dolt_branches" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "feature" ]] || false

    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    run dolt sql -q "select name from dolt_branches" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "main" ]] || false
    [[ ! "$output" =~ "feature" ]] || false
}

# Asserts that when a branch is deleted and a tag is created and they have the same name,
# the replica is able to correctly apply both changes.
@test "replication: pull branch delete and tag create with same name on read" {
    # Configure repo1 to push changes on commit and create branch b1
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt sql -q "call dolt_branch('b1');"

    # Configure repo2 to pull changes on read and assert the b1 branch exists
    cd ..
    dolt clone file://./rem1 repo2
    cd repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote origin
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    run dolt sql -q "select name from dolt_branches" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "name" ]] || false
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "b1" ]] || false

    # Delete branch b1 in repo1 and create tag b1
    cd ../repo1
    dolt sql -q "call dolt_branch('-D', 'b1'); call dolt_tag('b1');"

    # Confirm that branch b1 is deleted and tag b1 is created in repo2
    cd ../repo2
    run dolt sql -q "select name from dolt_branches" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "main" ]] || false
    run dolt sql -q "select tag_name from dolt_tags" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "tag_name" ]] || false
    [[ "$output" =~ "b1" ]] || false
}

@test "replication: pull branch delete current branch" {
    skip "broken by latest transaction changes"

    cd repo1
    dolt push remote1 feature

    cd ..
    dolt clone file://./rem1 repo2

    cd repo2
    dolt push origin :feature

    cd ../repo1
    run dolt sql -q "select name from dolt_branches" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "feature" ]] || false

    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt checkout feature
    run dolt sql -q "select name from dolt_branches" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "main" ]] || false
    [[ ! "$output" =~ "feature" ]] || false
}

@test "replication: table functions work" {
    cd repo1
    dolt sql <<SQL
create table t1 (a int primary key);
call dolt_commit("-Am", "new table");
insert into t1 values (1);
call dolt_commit("-Am", "first row");
insert into t1 values (2);
call dolt_commit("-Am", "second row");
SQL

    dolt push remote1 main

    cd ..
    dolt clone file://./rem1 repo2

    cd repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote origin
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1

    dolt sql -q "select * from t1"
    dolt sql -q "select count(*) from dolt_diff('HEAD~', 'HEAD', 't1')"
    dolt sql -q "select count(*) from dolt_diff_stat('HEAD', 'HEAD~', 't1')"
    dolt sql -q "select count(*) from dolt_log()"
}

@test "replication: pull on read" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt sql -q "create table t1 (a int primary key)"
    dolt add .
    dolt commit -am "new commit"
    dolt push origin main

    cd ../repo1
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ ! "$output" =~ "t1" ]] || false

    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    run dolt sql -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "replication: push on call dolt_branch(..." {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote backup1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main,new_branch
    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "call dolt_add('.')"
    dolt sql -q "call dolt_commit('-am', 'commit')"
    dolt sql -q "call dolt_branch('new_branch')"

    cd ..
    dolt clone file://./bac1 repo2
    cd repo2
    run dolt branch -av
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/origin/new_branch" ]] || false
}

@test "replication: push on call dolt_branch(-c..." {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote backup1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main,new_branch
    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "call dolt_add('.')"
    dolt sql -q "call dolt_commit('-am', 'commit')"
    dolt sql -q "call dolt_branch('-c', 'main', 'new_branch')"

    cd ..
    dolt clone file://./bac1 repo2
    cd repo2
    run dolt branch -av
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/origin/new_branch" ]] || false
}


@test "replication: push on call dolt_checkout(-b..." {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote backup1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main,new_branch
    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "call dolt_add('.')"
    dolt sql -q "call dolt_commit('-am', 'commit')"
    dolt sql -q "call dolt_checkout('-b', 'new_branch')"

    cd ..
    dolt clone file://./bac1 repo2
    cd repo2
    run dolt branch -av
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "remotes/origin/main" ]] || false
    [[ "$output" =~ "remotes/origin/new_branch" ]] || false
}

@test "replication: push on call dolt_merge, fast-forward merge" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    dolt sql <<SQL 
create table t1 (a int primary key);
call dolt_add('.');
call dolt_commit('-am', 'commit');
call dolt_checkout('-b', 'new_branch');
create table t2 (b int primary key);
call dolt_add('.');
call dolt_commit('-am', 'commit');
call dolt_checkout('main');
call dolt_merge('new_branch');
SQL

    cd ..
    dolt clone file://./rem1 repo2
    cd repo2
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
}

@test "replication: pull non-main head" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt checkout -b new_feature
    dolt sql -q "create table t1 (a int)"
    dolt add .
    dolt commit -am "cm"
    dolt push origin new_feature

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_heads new_feature
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    run dolt sql -q "show tables as of hashof('new_feature')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "Table" ]] || false
    [[ "${lines[1]}" =~ "t1" ]] || false
}

@test "replication: pull non-current head" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt sql << SQL
create table t1 (a int);
insert into t1 values (1);
SQL
    dolt add .
    dolt commit -am "cm"
    dolt push origin main

    dolt checkout -b "b1"
    dolt sql -q "update t1 set a = 2"
    dolt commit -am "new values"
    dolt push origin b1

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    run dolt sql -q 'select * from `repo1/b1`.t1' -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[1]}" =~ "2" ]] || false
}

@test "replication: pull on sql checkout" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt checkout -b new_feature
    dolt sql -q "create table t1 (a int)"
    dolt add .
    dolt commit -am "cm"
    dolt push origin new_feature

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_heads new_feature
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    run dolt sql -q "call dolt_checkout('new_feature'); show tables" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "${lines[2]}" =~ "Table" ]] || false
    [[ "${lines[3]}" =~ "t1" ]] || false
}

@test "replication: pull multiple heads" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt checkout -b new_feature
    dolt sql -q "create table t1 (a int)"
    dolt add .
    dolt commit -am "cm"
    dolt push origin new_feature
    dolt checkout main
    dolt sql -q "create table t2 (a int)"
    dolt add .
    dolt commit -am "cm"
    dolt push origin main

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main,new_feature
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1

    run dolt sql -q "show tables as of hashof('new_feature')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "Table" ]] || false
    [[ "${lines[1]}" =~ "t1" ]] || false

    run dolt sql -q "show tables as of hashof('main')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "Table" ]] || false
    [[ "${lines[1]}" =~ "t2" ]] || false
}

@test "replication: pull with wildcard branch heads" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt checkout -b feature1
    dolt sql -q "create table feature1 (a int)"
    dolt commit -Am "cm"
    dolt push origin feature1
    dolt checkout main
    dolt checkout -b feature22
    dolt sql -q "create table feature22 (a int)"
    dolt commit -Am "cm"
    dolt push origin feature22
    dolt checkout main
    dolt checkout -b myfeature
    dolt sql -q "create table myfeature (a int)"
    dolt commit -Am "cm"
    dolt push origin myfeature
    dolt checkout main
    dolt checkout -b releases
    dolt sql -q "create table releases (a int)"
    dolt commit -Am "cm"
    dolt push origin releases
    dolt checkout main
    dolt sql -q "create table main (a int)"
    dolt commit -Am "cm"
    dolt push origin main

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main,*feature*
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1

    run dolt sql -q "show tables as of hashof('feature1')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "Table" ]] || false
    [[ "${lines[1]}" =~ "feature1" ]] || false

    run dolt sql -q "show tables as of hashof('feature22')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "Table" ]] || false
    [[ "${lines[1]}" =~ "feature22" ]] || false

    run dolt sql -q "show tables as of hashof('myfeature')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "Table" ]] || false
    [[ "${lines[1]}" =~ "myfeature" ]] || false

    run dolt sql -q "show tables as of hashof('main')" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "${lines[0]}" =~ "Table" ]] || false
    [[ "${lines[1]}" =~ "main" ]] || false

    run dolt sql -q "select count(*) from dolt_branches where name='releases'" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ "count(*)" ]] || false
    [[ "${lines[1]}" =~ "0" ]] || false

    run dolt sql -q "show tables as of hashof('releases')" -r csv
    [ $status -ne 0 ]
    [[ "${lines[0]}" =~ "invalid ref spec" ]] || false
}

@test "replication: pull with unknown head" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt branch new_feature
    dolt push origin new_feature

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main,unknown
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    run dolt sql -q "show tables"
   
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ 'unable to find "unknown" on "remote1"; branch not found' ]] || false
}

@test "replication: pull multiple heads, one invalid branch name" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt branch new_feature
    dolt push origin new_feature

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main,unknown
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    run dolt sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "branch not found" ]] || false
}

@test "replication: replica pull multiple heads quiet warnings" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt branch new_feature
    dolt push origin new_feature

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors 1
    dolt config --local --add sqlserver.global.dolt_replicate_heads unknown
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "branch not found" ]] || false

    run dolt checkout new_feature
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]] || false
}

@test "replication: pull all heads from feat" {
    cd repo1
    dolt branch -c main feat
    dolt push remote1 feat
    cd ..
    # clone repo2 from repo1
    dolt clone file://./rem1 repo2
    cd repo2
    dolt checkout feat
    dolt checkout main
    # add tables to repo1
    cd ../repo1
    dolt checkout feat
    dolt sql -q "create table t1 (a int)"
    dolt sql -q "create table t2 (a int)"
    dolt add .
    dolt commit -am "cm"
    # remote1 has tables
    dolt push remote1 feat
    # repo2 replication config
    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote origin

    # repo2 pulls on read
    run dolt sql -r csv -b <<SQL
call dolt_checkout('feat');
show tables;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Tables_in_repo2" ]] || false
    [[ "$output" =~ "t1" ]] || false
    [[ "$output" =~ "t2" ]] || false
}

@test "replication: pull all heads" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt checkout -b new_feature
    dolt sql -q "create table t1 (a int)"
    dolt add .
    dolt commit -am "cm"
    dolt push origin new_feature

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    run dolt sql -q "call dolt_checkout('new_feature'); show tables" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[2]}" =~ "Tables_in_repo1" ]] || false
    [[ "${lines[3]}" =~ "t1" ]] || false
}

@test "replication: pull all heads pulls tags" {

    dolt clone file://./rem1 repo2
    cd repo2
    dolt checkout -b new_feature
    dolt tag v1
    dolt push origin new_feature
    dolt push origin v1

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt sql -q "START TRANSACTION"
    run dolt tag
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "$output" =~ "v1" ]] || false
}

@test "replication: tag is pushed" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt tag tag1

    cd ../
    dolt clone file://./rem1 repo2
    cd repo2

    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote origin
    run dolt tag
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tag1" ]] || false

    cd ../repo1
    dolt sql -q "call dolt_tag('tag2')"

    cd ../repo2
    run dolt sql -q "select * from dolt_tags"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "tag1" ]] || false
    [[ "$output" =~ "tag2" ]] || false
}

@test "replication: pull creates remote tracking branches" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt sql -q "create table t1 (a int primary key);"
    dolt commit -Am "new table"
    dolt branch b1
    dolt branch b2
    dolt push origin b1
    dolt push origin b2

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1

    run dolt sql -q 'USE `repo1/b2`; show tables;' -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false

    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/remote1/b1" ]] || false
    [[ "$output" =~ "remotes/remote1/b2" ]] || false    
}

@test "replication: connect to a branch not on the remote" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt sql -q "create table t1 (a int primary key);"
    dolt commit -Am "new table"
    dolt branch b1
    dolt push origin b1

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1

    run dolt sql -q 'USE `repo1/B1`; show tables;' -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false

    run dolt sql -q 'USE `repo1/notfound`;' -r csv
    [ "$status" -ne 0 ]
    [[ "$output" =~ "database not found" ]] || false
}

@test "replication: push feature head" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt checkout -b new_feature
    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "call dolt_add('.')"
    dolt sql -q "call dolt_commit('-am', 'cm')"

    cd ..
    dolt clone file://./rem1 repo2
    cd repo2
    dolt fetch origin new_feature
}

@test "replication: push to unknown remote error" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown

    run dolt sql -q "create table t1 (a int primary key)"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
    [[ "$output" =~ "replication disabled" ]] || false
}

@test "replication: quiet push to unknown remote warnings" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors 1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown
    run dolt sql -q "create table t1 (a int primary key)"
    [ "$status" -eq 0 ]

    dolt add .

    run dolt sql -q "call dolt_commit('-am', 'cm')"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
}

@test "replication: bad source doesn't error during non-transactional commands" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main

    run dolt status
    [ "$status" -eq 0 ]
}

@test "replication: non-fast-forward pull fails with force turned off" {
    dolt clone file://./rem1 clone1
    cd clone1
    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "insert into t1 values (1), (2), (3);"
    dolt add .
    dolt commit -am "new commit"
    dolt push origin main

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    dolt sql -q "set @@persist.dolt_read_replica_force_pull = off"

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false

    cd ../clone1
    dolt checkout -b new-main HEAD~
    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "insert into t1 values (1);"
    dolt add .
    dolt commit -am "new commit"
    dolt push -f origin new-main:main

    cd ../repo1
    
    # with dolt_read_replica_force_pull set to false (not default), this fails with a replication
    # error
    run dolt sql -q "show tables"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "replication" ]] || false
}

@test "replication: non-fast-forward pull with force pull setting succeeds replication" {
    dolt clone file://./rem1 clone1
    cd clone1
    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "insert into t1 values (1), (2), (3);"
    dolt add .
    dolt commit -am "new commit"
    dolt push origin main

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main

    run dolt sql -q "select sum(a) from t1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6" ]] || false

    cd ../clone1
    dolt checkout -b new-main HEAD~
    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "insert into t1 values (4), (5), (6);"
    dolt add .
    dolt commit -am "new commit"
    dolt push -f origin new-main:main

    cd ../repo1
    run dolt sql -q "select sum(a) from t1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "15" ]] || false
}

@test "replication: pull bad remote quiet warning" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors 1

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "panic" ]] || false
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
}

@test "replication: use database syntax fetches missing branch" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt checkout -b feature-branch
    dolt sql -q "create table t1 (a int primary key)"
    dolt add .
    dolt commit -am "new commit"
    dolt push origin feature-branch

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    run dolt sql -b -q "USE \`repo1/feature-branch\`; show tables" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "Table" ]] || false
    [[ "${lines[2]}" =~ "t1" ]] || false
}

@test "replication: database autofetch doesn't change replication heads setting" {
    dolt clone file://./rem1 repo2
    cd repo2
    dolt branch feature-branch
    dolt push origin feature-branch

    cd ../repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    run dolt sql -q "use \`repo1/feature-branch\`"

    cd ../repo2
    dolt checkout feature-branch
    dolt sql -q "create table t1 (a int primary key)"
    dolt add .
    dolt commit -am "new commit"
    dolt push origin feature-branch

    cd ../repo1
    run dolt sql -b -q "show tables" -r csv
    [ "$status" -eq 0 ]
    [[ ! "output" =~ "t1" ]] || false
}

@test "replication: async push on cli engine commit" {
    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt config --local --add sqlserver.global.dolt_async_replication 1
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1

    dolt sql -q "create table t1 (a int primary key)"
    dolt sql -q "call dolt_add('.')"
    dolt sql -q "call dolt_commit('-am', 'cm')"

    cd ..
    dolt clone file://./rem1 repo2
    cd repo2
    
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "replication: local clone" {
    run dolt clone file://./repo1/.dolt/noms repo2
    [ "$status" -eq 0 ]
    cd repo2
    run dolt ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
}

@test "replication: commit --amend" {
    mkdir test_commit_amend_replication_primary
    dolt init --fun

    dolt remote add origin file://./test_commit_amend_replication
    dolt push origin main

    dolt sql -q "set @@persist.dolt_replicate_all_heads = 1"
    dolt sql -q "set @@persist.dolt_replicate_to_remote = 'origin'"

    dolt sql << SQL
create table foo (pk int primary key, c1 int);
insert into foo values (1,1);
SQL
    
    dolt commit -Am "Created Table"

    mkdir clone && cd clone
    dolt clone file://../test_commit_amend_replication
    cd test_commit_amend_replication
    dolt sql -q "set @@persist.dolt_replicate_heads = 'main'"
    dolt sql -q "set @@persist.dolt_read_replica_remote = 'origin'"
    dolt sql -q "select * from foo"

    cd ../../
    dolt commit --amend -m "inserted 0,0. amended"
    dolt push origin main

    cd clone/test_commit_amend_replication
    run dolt sql -q "select * from foo" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,1" ]] || false 
}
