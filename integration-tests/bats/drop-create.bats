#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    skip_nbf_dolt_1
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "drop-create: same schema and data" {
    dolt sql  <<SQL
create table test(a int primary key, b int null);
insert into test values (1,1), (2,2);
select dolt_commit("-am", "table with two rows");
SQL

    dolt sql -q "drop table test"

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false

    dolt sql  <<SQL
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
    dolt sql  <<SQL
create table test(a int primary key, b int null);
insert into test values (1,1), (2,2);
select dolt_commit("-am", "table with two rows");
SQL

    dolt sql -q "drop table test"
    dolt commit -am "deleted table"
    
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false

    dolt sql  <<SQL
create table test(a int primary key, b int null);
insert into test values (1,1), (2,2);
SQL

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    skip "Dolt can't look into previous commits to reuse table data, only the current branch head"
    
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table" ]] || false
    [[ "$output" =~ "test" ]] || false

    dolt diff HEAD~
    run dolt diff HEAD~
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "added table" ]] || false
    [[ ! "$output" =~ "deleted table" ]] || false
}

@test "drop-create: added column" {
    dolt sql  <<SQL
create table test(a int primary key, b int null);
insert into test values (1,1), (2,2);
select dolt_commit("-am", "table with two rows");
SQL

    dolt sql -q "drop table test"

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false

    dolt sql  <<SQL
create table test(a int primary key, b int null, c int null);
insert into test(a,b) values (1,1), (2,2);
SQL

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ modified:[[:space:]]*test ]] || false

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  \`c\` int," ]] || false
    [[ "$output" =~ "|  <  | a | b |   |" ]] || false
    [[ "$output" =~ "|  >  | a | b | c |" ]] || false
}

@test "drop-create: added column with data modifications" {
    dolt sql  <<SQL
create table test(a int primary key, b int null);
insert into test values (1,1), (2,2);
select dolt_commit("-am", "table with two rows");
SQL

    dolt sql -q "drop table test"

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false

    dolt sql  <<SQL
create table test(a int primary key, b int null, c int null);
insert into test(a,b,c) values (1,2,1), (2,3,2), (3,3,3);
SQL

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ modified:[[:space:]]*test ]] || false

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "+  \`c\` int," ]] || false
    [[ "$output" =~ "|  <  | a | b |      |" ]] || false
    [[ "$output" =~ "|  >  | a | b | c    |" ]] || false
    [[ "$output" =~ "|  <  | 1 | 1 | NULL |" ]] || false
    [[ "$output" =~ "|  <  | 2 | 2 | NULL |" ]] || false
    [[ "$output" =~ "|  >  | 1 | 2 | 1    |" ]] || false
    [[ "$output" =~ "|  >  | 2 | 3 | 2    |" ]] || false
    [[ "$output" =~ "|  +  | 3 | 3 | 3    |" ]] || false
}


@test "drop-create: dropped column" {
    dolt sql  <<SQL
create table test(a int primary key, b int null, c int null);
insert into test values (1,2,3), (4,5,6);
select dolt_commit("-am", "table with two rows");
SQL

    dolt sql -q "drop table test"

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false

    dolt sql  <<SQL
create table test(a int primary key, b int null);
insert into test(a,b) values (1,2), (4,5);
SQL

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ modified:[[:space:]]*test ]] || false

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "-  \`c\` int," ]] || false
    [[ "$output" =~ "|  <  | a | b | c    |" ]] || false
    [[ "$output" =~ "|  >  | a | b |      |" ]] || false
    [[ "$output" =~ "|  <  | 1 | 2 | 3    |" ]] || false
    [[ "$output" =~ "|  >  | 1 | 2 | NULL |" ]] || false
    [[ "$output" =~ "|  <  | 4 | 5 | 6    |" ]] || false
    [[ "$output" =~ "|  >  | 4 | 5 | NULL |" ]] || false
}

@test "drop-create: dropped column with data modifications" {
    dolt sql  <<SQL
create table test(a int primary key, b int null, c int null);
insert into test values (1,2,3), (4,5,6);
select dolt_commit("-am", "table with two rows");
SQL

    dolt sql -q "drop table test"

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false

    dolt sql  <<SQL
create table test(a int primary key, b int null);
insert into test(a,b) values (1,7), (4,8), (9,10);
SQL

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ modified:[[:space:]]*test ]] || false

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "-  \`c\` int," ]] || false
    [[ "$output" =~ "|  <  | a | b  | c    |" ]] || false
    [[ "$output" =~ "|  >  | a | b  |      |" ]] || false
    [[ "$output" =~ "|  <  | 1 | 2  | 3    |" ]] || false
    [[ "$output" =~ "|  >  | 1 | 7  | NULL |" ]] || false
    [[ "$output" =~ "|  <  | 4 | 5  | 6    |" ]] || false
    [[ "$output" =~ "|  >  | 4 | 8  | NULL |" ]] || false
    [[ "$output" =~ "|  +  | 9 | 10 | NULL |" ]] || false
}

@test "drop-create: added column, modified column" {
    dolt sql  <<SQL
create table test(a int primary key, b int null);
insert into test values (1,1), (2,2);
select dolt_commit("-am", "table with two rows");
SQL

    dolt sql -q "drop table test"

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false

    dolt sql  <<SQL
create table test(a bigint primary key, b tinyint null, c int null);
insert into test(a,b) values (1,1), (2,2);
SQL

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ modified:[[:space:]]*test ]] || false

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "-  \`a\` int NOT NULL," ]] || false
    [[ "$output" =~ "-  \`b\` int," ]] || false
    [[ "$output" =~ "+  \`a\` bigint NOT NULL," ]] || false
    [[ "$output" =~ "+  \`b\` tinyint," ]] || false
    [[ "$output" =~ "+  \`c\` int," ]] || false
    [[ "$output" =~ "|  <  | a | b |   |" ]] || false
    [[ "$output" =~ "|  >  | a | b | c |" ]] || false
}

@test "drop-create: constraint changes" {
    dolt sql  <<SQL
create table test(a int primary key, b int null);
insert into test values (1,1), (2,2);
select dolt_commit("-am", "table with two rows");
SQL

    dolt sql -q "drop table test"

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false

    dolt sql  <<SQL
create table test(a bigint primary key, b tinyint not null check (b > 0), c varchar(10));
insert into test(a,b) values (1,1), (2,2);
SQL

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ modified:[[:space:]]*test ]] || false

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]

    [[ "$output" =~ "-  \`a\` int NOT NULL," ]] || false
    [[ "$output" =~ "-  \`b\` int," ]] || false
    [[ "$output" =~ "-  PRIMARY KEY (\`a\`)" ]] || false
    [[ "$output" =~ "+  \`a\` bigint NOT NULL," ]] || false
    [[ "$output" =~ "+  \`b\` tinyint NOT NULL," ]] || false
    [[ "$output" =~ "+  \`c\` varchar(10)," ]] || false
    [[ "$output" =~ "+  PRIMARY KEY (\`a\`)," ]] || false
    [[ "$output" =~ "+  CONSTRAINT \`chk_vk8cbuqc\` CHECK ((\`b\` > 0))" ]] || false
    [[ "$output" =~ "|  <  | a | b |   |" ]] || false
    [[ "$output" =~ "|  >  | a | b | c |" ]] || false
}

@test "drop-create: default changes" {
    dolt sql  <<SQL
create table test(a int primary key, b int null default 10);
insert into test values (1,1), (2,2);
select dolt_commit("-am", "table with two rows");
SQL

    dolt sql -q "drop table test"

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "test" ]] || false

    dolt sql  <<SQL
create table test(a bigint primary key, b tinyint not null default 50, c varchar(10));
insert into test(a,b) values (1,1), (2,2);
SQL

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ modified:[[:space:]]*test ]] || false

    dolt diff
    run dolt diff
    [ "$status" -eq 0 ]

    [[ "$output" =~ "-  \`a\` int NOT NULL," ]] || false
    [[ "$output" =~ "-  \`b\` int DEFAULT 10," ]] || false
    [[ "$output" =~ "+  \`a\` bigint NOT NULL," ]] || false
    [[ "$output" =~ "+  \`b\` tinyint NOT NULL DEFAULT 50," ]] || false
    [[ "$output" =~ "+  \`c\` varchar(10)" ]] || false
    [[ "$output" =~ "|  <  | a | b |   |" ]] || false
    [[ "$output" =~ "|  >  | a | b | c |" ]] || false
}

@test "drop-create: drop table from different database" {
    dolt sql  <<SQL
create table test (currentId int primary key, currentText text);
insert into test values (1, 'text1'), (2, 'text2');
create schema common;
create table common.test (commonId integer, commonText text);
insert into common.test values (999, 'common database text1');
SQL

    run dolt sql -q "select * from test"
    currenttest=$output

    run dolt sql -q "select * from common.test"
    [[ "$output" =~ "common database text1" ]] || false

    dolt sql -q "drop table common.test"

    run dolt sql -q "select * from test"
    [ "$output" = "$currenttest" ]

    run dolt sql -q "select * from common.test"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found: test" ]] || false
}
