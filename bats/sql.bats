#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE one_pk (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt sql <<SQL
CREATE TABLE two_pk (
  pk1 BIGINT NOT NULL,
  pk2 BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk1,pk2)
);
SQL
    dolt sql <<SQL
CREATE TABLE has_datetimes (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  date_created DATETIME COMMENT 'tag:1',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q "insert into one_pk (pk,c1,c2,c3,c4,c5) values (0,0,0,0,0,0),(1,10,10,10,10,10),(2,20,20,20,20,20),(3,30,30,30,30,30)"
    dolt sql -q "insert into two_pk (pk1,pk2,c1,c2,c3,c4,c5) values (0,0,0,0,0,0,0),(0,1,10,10,10,10,10),(1,0,20,20,20,20,20),(1,1,30,30,30,30,30)"
    dolt sql -q "insert into has_datetimes (pk, date_created) values (0, '2020-02-17 00:00:00')"
}

teardown() {
    teardown_common
}

@test "sql select from multiple tables" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 20 ]
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    run dolt sql -q "select pk,pk1,pk2,one_pk.c1 as foo,two_pk.c1 as bar from one_pk,two_pk where one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ foo ]] || false
    [[ "$output" =~ bar ]] || false
    [ "${#lines[@]}" -eq 8 ]
}

@test "sql AS OF queries" {
    dolt add .
    dolt commit -m "Initial master commit" --date "2020-03-01T12:00:00Z"

    master_commit=`dolt log | head -n1 | cut -d' ' -f2`
    dolt sql -q "update one_pk set c1 = c1 + 1"
    dolt sql -q "drop table two_pk"
    dolt checkout -b new_branch
    dolt add .
    dolt commit -m "Updated a table, dropped a table" --date "2020-03-01T13:00:00Z"
    new_commit=`dolt log | head -n1 | cut -d' ' -f2`
    
    run dolt sql -r csv -q "select pk,c1 from one_pk order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false
    
    run dolt sql -r csv -q "select pk,c1 from one_pk as of 'master' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of '$master_commit' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false
    
    run dolt sql -r csv -q "select count(*) from two_pk as of 'master'"
    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false

    run dolt sql -r csv -q "select count(*) from two_pk as of '$master_commit'"
    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of 'HEAD~' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of 'new_branch^' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false
    
    dolt checkout master
    run dolt sql -r csv -q "select pk,c1 from one_pk as of 'new_branch' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of '$new_commit' order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false

    dolt checkout new_branch
    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 12:00:00', DATETIME) order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 12:15:00', DATETIME) order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,10" ]] || false
    [[ "$output" =~ "2,20" ]] || false
    [[ "$output" =~ "3,30" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 13:00:00', DATETIME) order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 13:15:00', DATETIME) order by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ "0,1" ]] || false
    [[ "$output" =~ "1,11" ]] || false
    [[ "$output" =~ "2,21" ]] || false
    [[ "$output" =~ "3,31" ]] || false

    run dolt sql -r csv -q "select pk,c1 from one_pk as of CONVERT('2020-03-01 11:59:59', DATETIME) order by c1"
    [ $status -eq 1 ]
    [[ "$output" =~ "not found" ]] || false
}

@test "sql output formats" {
    dolt sql <<SQL
    CREATE TABLE test (
    a int primary key,
    b float,
    c varchar(80),
    d datetime
);
SQL
    dolt sql <<SQL
    insert into test values (1, 1.5, "1", "2020-01-01");
    insert into test values (2, 2.5, "2", "2020-02-02");
    insert into test values (3, NULL, "3", "2020-03-03");
    insert into test values (4, 4.5, NULL, "2020-04-04");
    insert into test values (5, 5.5, "5", NULL);
SQL

    run dolt sql -r csv -q "select * from test order by a"
    [ $status -eq 0 ]
    [[ "$output" =~ "a,b,c,d" ]] || false
    [[ "$output" =~ '1,1.5,1,2020-01-01 00:00:00 +0000 UTC' ]] || false
    [[ "$output" =~ '2,2.5,2,2020-02-02 00:00:00 +0000 UTC' ]] || false
    [[ "$output" =~ '3,,3,2020-03-03 00:00:00 +0000 UTC' ]] || false
    [[ "$output" =~ '4,4.5,,2020-04-04 00:00:00 +0000 UTC' ]] || false
    [[ "$output" =~ '5,5.5,5,' ]] || false
    [ "${#lines[@]}" -eq 6 ]

    run dolt sql -r json -q "select * from test order by a"
    [ $status -eq 0 ]
    [ "$output" == '{"rows": [{"a":1,"b":1.5,"c":"1","d":{}},{"a":2,"b":2.5,"c":"2","d":{}},{"a":3,"c":"3","d":{}},{"a":4,"b":4.5,"d":{}},{"a":5,"b":5.5,"c":"5"}]}' ]
}

@test "sql ambiguous column name" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk where c1=0"
    [ "$status" -eq 1 ]
    [ "$output" = "ambiguous column name \"c1\", it's present in all these tables: one_pk, two_pk" ]
}

@test "sql select with and and or clauses" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk where pk=0 and pk1=0 or pk2=1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 13 ]
}

@test "sql select the same column twice using column aliases" {
    run dolt sql -q "select pk,c1 as foo,c1 as bar from one_pk"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "<NULL>" ]] || false
    [[ "$output" =~ "foo" ]] || false
    [[ "$output" =~ "bar" ]] || false
}

@test "sql select same column twice using table aliases" {
    run dolt sql -q "select pk,foo.c1,bar.c1 from one_pk as foo, one_pk as bar"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "<NULL>" ]] || false
    [[ "$output" =~ "c1" ]] || false
}

@test "sql select make a new column as a function of aliases" {
    run dolt sql -q "select count(*) as a, count(*) as b, a/b as percent from dual"
    skip "This fails right now with a column a not in any table error"
    [ "$status" -eq 0 ]
    [[ "$output" =~ " percent " ]]
}

@test "sql basic inner join" {
    run dolt sql -q "select pk,pk1,pk2 from one_pk join two_pk on one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    first_join_output=$output
    run dolt sql -q "select pk,pk1,pk2 from two_pk join one_pk on one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    [ "$output" = "$first_join_output" ]
    run dolt sql -q "select pk,pk1,pk2 from one_pk join two_pk on one_pk.c1=two_pk.c1 where pk=1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select pk,pk1,pk2,one_pk.c1 as foo,two_pk.c1 as bar from one_pk join two_pk on one_pk.c1=two_pk.c1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ foo ]] || false
    [[ "$output" =~ bar ]] || false
    [ "${#lines[@]}" -eq 8 ]
    run dolt sql -q "select pk,pk1,pk2,one_pk.c1 as foo,two_pk.c1 as bar from one_pk join two_pk on one_pk.c1=two_pk.c1  where one_pk.c1=10"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "10" ]] || false
}

@test "select two tables and join to one" {
    run dolt sql -q "select op.pk,pk1,pk2 from one_pk,two_pk join one_pk as op on op.pk=pk1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 20 ]
}

@test "non unique table alias" {
    run dolt sql -q "select pk from one_pk,one_pk"
    skip "This should be an error. MySQL gives: Not unique table/alias: 'one_pk'"
    [ $status -eq 1 ]
}

@test "sql is null and is not null statements" {
    dolt sql -q "insert into one_pk (pk,c1,c2) values (11,0,0)"
    run dolt sql -q "select pk from one_pk where c3 is null"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "11" ]] || false
    run dolt sql -q "select pk from one_pk where c3 is not null"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    [[ ! "$output" =~ "11" ]] || false
}

@test "sql addition and subtraction" {
    dolt sql -q "insert into one_pk (pk,c1,c2,c3,c4,c5) values (11,0,5,10,15,20)"
    run dolt sql -q "select pk from one_pk where c2-c1>=5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "11" ]] || false
    run dolt sql -q "select pk from one_pk where c3-c2-c1>=5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "11" ]] || false
    run dolt sql -q "select pk from one_pk where c2+c1<=5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "11" ]] || false
}

@test "sql order by and limit" {
    run dolt sql -q "select * from one_pk order by pk limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ " 0 " ]] || false
    [[ ! "$output" =~ " 10 " ]] || false
    run dolt sql -q "select * from one_pk order by pk limit 0,1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ " 0 " ]] || false
    [[ ! "$output" =~ " 10 " ]] || false
    run dolt sql -q "select * from one_pk order by pk limit 1,1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ " 10 " ]] || false
    [[ ! "$output" =~ " 0 " ]] || false
    run dolt sql -q "select * from one_pk order by pk limit 1,0"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ ! "$output" =~ " 0 " ]] || false
    run dolt sql -q "select * from one_pk order by pk desc limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "30" ]] || false
    [[ ! "$output" =~ "10" ]] || false
    run dolt sql -q "select * from two_pk order by pk1, pk2 desc limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "10" ]] || false
    run dolt sql -q "select pk,c2 from one_pk order by c1 limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "0" ]] || false
    [[ ! "$output" =~ "10" ]] || false
    run dolt sql -q "select * from one_pk,two_pk order by pk1,pk2,pk limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "0" ]] || false
    [[ ! "$output" =~ "10" ]] || false
    dolt sql -q "select * from one_pk join two_pk order by pk1,pk2,pk limit 1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "0" ]] || false
    [[ ! "$output" =~ "10" ]] || false
    run dolt sql -q "select * from one_pk order by limit 1"
    [ $status -eq 1 ]
    run dolt sql -q "select * from one_pk order by bad limit 1"
    [ $status -eq 1 ]
    [[ "$output" =~ "column \"bad\" could not be found" ]] || false
    run dolt sql -q "select * from one_pk order pk by limit"
    [ $status -eq 1 ]
}

@test "sql limit less than zero" {
    run dolt sql -q "select * from one_pk order by pk limit -1"
    [ $status -eq 1 ]
    [[ "$output" =~ "unsupported syntax: LIMIT must be >= 0" ]] || false
    run dolt sql -q "select * from one_pk order by pk limit -2"
    [ $status -eq 1 ]
    [[ "$output" =~ "unsupported syntax: LIMIT must be >= 0" ]] || false
    run dolt sql -q "select * from one_pk order by pk limit -1,1"
    [ $status -eq 1 ]
    [[ "$output" =~ "unsupported syntax: OFFSET must be >= 0" ]] || false
}

@test "addition on both left and right sides of comparison operator" {
    dolt sql -q "insert into one_pk (pk,c1,c2,c3,c4,c5) values (11,5,5,10,15,20)"
    run dolt sql -q "select pk from one_pk where c2+c1<=5+5"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ 0 ]] || false
    [[ "$output" =~ 11 ]] || false
}

@test "select with in list" {
    run dolt sql -q "select pk from one_pk where c1 in (10,20)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "2" ]] || false
    run dolt sql -q "select pk from one_pk where c1 in (11,21)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    run dolt sql -q "select pk from one_pk where c1 not in (10,20)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "0" ]] || false
    [[ "$output" =~ "3" ]] || false
    run dolt sql -q "select pk from one_pk where c1 not in (10,20) and c1 in (30)"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "3" ]] || false
}

@test "sql parser does not support empty list" {
    run dolt sql -q "select pk from one_pk where c1 not in ()"
    [ $status -eq 1 ]
    [[ "$output" =~ "Error parsing SQL" ]] || false
}

@test "sql addition in join statement" {
    run dolt sql -q "select * from one_pk join two_pk on pk1-pk>0 and pk2<1"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "$output" =~ "20" ]] || false
}

@test "leave off table name in select" {
    dolt sql -q "insert into one_pk (pk,c1,c2) values (11,0,0)"
    run dolt sql -q "select pk where c3 is null"
    [ $status -eq 1 ]
    [[ "$output" =~ "column \"c3\" could not be found in any table in scope" ]] || false
}

@test "sql show tables" {
    run dolt sql -q "show tables"
    [ $status -eq 0 ]
    echo ${#lines[@]}
    [ "${#lines[@]}" -eq 7 ]
    [[ "$output" =~ "one_pk" ]] || false
    [[ "$output" =~ "two_pk" ]] || false
    [[ "$output" =~ "has_datetimes" ]] || false
}

@test "sql describe" {
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 10 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ "c5" ]] || false
}

@test "sql decribe bad table name" {
    run dolt sql -q "describe poop"
    [ $status -eq 1 ]
    [[ "$output" =~ "table not found: poop" ]] || false
}

@test "sql alter table to add and delete a column" {
    run dolt sql -q "alter table one_pk add (c6 int)"
    [ $status -eq 0 ]
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [[ "$output" =~ "c6" ]] || false
    run dolt schema show one_pk
    [[ "$output" =~ "c6" ]] || false
    run dolt sql -q "alter table one_pk drop column c6"
    [ $status -eq 0 ]
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "c6" ]] || false
    run dolt schema show one_pk
    [[ ! "$output" =~ "c6" ]] || false
}

@test "sql alter table to rename a column" {
    dolt sql -q "alter table one_pk add (c6 int)"
    run dolt sql -q "alter table one_pk rename column c6 to c7"
    [ $status -eq 0 ]
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [[ "$output" =~ "c7" ]] || false
    [[ ! "$output" =~ "c6" ]] || false
}

@test "sql alter table without parentheses" {
    run dolt sql -q "alter table one_pk add c6 int"
    [ $status -eq 0 ]
    run dolt sql -q "describe one_pk"
    [ $status -eq 0 ]
    [[ "$output" =~ "c6" ]] || false
}

@test "sql alter table to change column type not supported" {
    run dolt sql -q "alter table one_pk modify column c5 varchar(80)"
    [ $status -eq 1 ]
    [[ "$output" =~ "unsupported feature: column types cannot be changed" ]] || false
}

@test "sql drop table" {
    dolt sql -q "drop table one_pk"
    run dolt ls
    [[ ! "$output" =~ "one_pk" ]] || false
    run dolt sql -q "drop table poop"
    [ $status -eq 1 ]
    [ "$output" = "table not found: poop" ]
}

@test "explain simple select query" {
    run dolt sql -q "explain select * from one_pk"
    [ $status -eq 0 ]
    [[ "$output" =~ "plan" ]] || false
    [[ "$output" =~ "one_pk" ]] || false
}

@test "explain simple query with where clause" {
    run dolt sql -q "explain select * from one_pk where pk=0"
    [ $status -eq 0 ]
    [[ "$output" =~ "Filter" ]] || false
}

@test "explain simple join" {
    run dolt sql -q "explain select op.pk,pk1,pk2 from one_pk,two_pk join one_pk as op on op.pk=pk1"
    [ $status -eq 0 ]
    [[ "$output" =~ "InnerJoin" ]] || false
}

@test "sql replace count" {
    skip "right now we always count a replace as a delete and insert when we shouldn't"
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v BIGINT);"
    run dolt sql -q "REPLACE INTO test VALUES (1, 1);"
    [ $status -eq 0 ]
    [[ "${lines[3]}" =~ " 1 " ]] || false
    run dolt sql -q "REPLACE INTO test VALUES (1, 2);"
    [ $status -eq 0 ]
    [[ "${lines[3]}" =~ " 2 " ]] || false
}

@test "sql unix_timestamp function" {
    run dolt sql -q "SELECT UNIX_TIMESTAMP(NOW()) FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
}

@test "sql select union all" {
    run dolt sql -q "SELECT 2+2 FROM dual UNION ALL SELECT 2+2 FROM dual UNION ALL SELECT 2+3 FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 7 ]
}

@test "sql select union" {
    run dolt sql -q "SELECT 2+2 FROM dual UNION SELECT 2+2 FROM dual UNION SELECT 2+3 FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 7 ]
    run dolt sql -q "SELECT 2+2 FROM dual UNION DISTINCT SELECT 2+2 FROM dual UNION SELECT 2+3 FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    run dolt sql -q "(SELECT 2+2 FROM dual UNION DISTINCT SELECT 2+2 FROM dual) UNION SELECT 2+3 FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    run dolt sql -q "SELECT 2+2 FROM dual UNION DISTINCT (SELECT 2+2 FROM dual UNION SELECT 2+3 FROM dual);"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
}

@test "sql greatest/least with a timestamp" {
    run dolt sql -q "SELECT GREATEST(NOW(), DATE_ADD(NOW(), INTERVAL 2 DAY)) FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "SELECT LEAST(NOW(), DATE_ADD(NOW(), INTERVAL 2 DAY)) FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
}

@test "sql greatest with converted null" {
    run dolt sql -q "SELECT GREATEST(CAST(NOW() AS CHAR), CAST(NULL AS CHAR)) FROM dual;"
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    [[ "${lines[3]}" =~ " <NULL> " ]] || false
}

@test "sql date_format function" {
    skip "date_format() not supported" 
    dolt sql -q "select date_format(date_created, '%Y-%m-%d') from has_datetimes"
}

@test "sql DATE_ADD and DATE_SUB in where clause" {
    skip "DATE_ADD and DATE_SUB in the where clause causes panic"
    run dolt sql -q "select * from has_datetimes where date_created > DATE_SUB('2020-02-18 00:00:00', INTERVAL 2 DAY)"
    [ $status -eq 0 ]
    [[ "$output" =~ "17 " ]] || false
    run dolt sql -q "select * from has_datetimes where date_created > DATE_ADD('2020-02-14 00:00:00', INTERVAL 2 DAY)"
    [ $status -eq 0 ]
    [[ "$output" =~ "17 " ]] || false
}

@test "sql update a datetime column" {
    dolt sql -q "insert into has_datetimes (pk) values (1)"
    run dolt sql -q "update has_datetimes set date_created='2020-02-11 00:00:00' where pk=1"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "Expected GetField expression" ]] || false
}

@test "sql group by statements" {
    dolt sql -q "insert into one_pk (pk,c1,c2,c3,c4,c5) values (4,0,0,0,0,0),(5,0,0,0,0,0)"
    run dolt sql -q "select max(pk) from one_pk group by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ " 5 " ]] || false
    [[ ! "$output" =~ " 4 " ]] || false
    run dolt sql -q "select max(pk), min(c2) from one_pk group by c1"
    [ $status -eq 0 ]
    [[ "$output" =~ " 5 " ]] || false
    [[ "$output" =~ " 0 " ]] || false
    [[ ! "$output" =~ " 4 " ]] || false
    run dolt sql -q "select max(pk),c2 from one_pk group by c1"
    [ $status -eq 1 ]
    [[ "$output" =~ "doesn't appear in the grouping columns" ]] || false
    skip "group by behavior is inconsistent"
    run dolt sql -q "select max(pk),c2 from one_pk group by c1 limit 1"
    [ $status -eq 1 ]
    [[ "$output" =~ "doesn't appear in the grouping columns" ]] || false
    run dolt sql -q "select max(pk) as broke_group_by, c2 from one_pk group by c1"
    [ $status -eq 1 ]
    [[ "$output" =~ "doesn't appear in the grouping columns" ]] || false
}

@test "sql substr() and cast() functions" {
    run dolt sql -q "select substr(cast(date_created as char), 1, 4) from has_datetimes"
    [ $status -eq 0 ]
    [[ "$output" =~ " 2020 " ]] || false
    [[ ! "$output" =~ "17" ]] || false
}

@test "sql divide by zero does not panic" {
    run dolt sql -q "select 1/0 from dual"
    [ $status -ne 0 ]
    skip "Divide by zero panics dolt right now"
    [[ ! "$output" =~ "panic: " ]] || false
}

@test "sql delete all rows in table" {
    run dolt sql <<SQL
DELETE FROM one_pk;
SELECT count(*) FROM one_pk;
SQL
    [ $status -eq 0 ]
    [[ "$output" =~ "0" ]] || false
}

@test "sql shell works after failing query" {
    skiponwindows "Need to install expect and make this script work on windows."
    $BATS_TEST_DIRNAME/sql-works-after-failing-query.expect
}

@test "sql insert on duplicate key inserts data by column" {
    run dolt sql -q "CREATE TABLE test (col_a varchar(2) not null, col_b varchar(2), col_c varchar(2), primary key(col_a));"
    [ $status -eq 0 ]
    run dolt sql -q "INSERT INTO test (col_a,col_b) VALUES('a', 'b');"
    [ $status -eq 0 ]
    skip run dolt sql -q "INSERT INTO test (col_a,col_b,col_c) VALUES ('a','','b') ON DUPLICATE KEY UPDATE col_a = col_a, col_b = col_b, col_c = VALUES(col_c);"
    [ $status -eq 0 ]
    [[ ! "$output" =~ 'unsupported feature' ]] || false
}
