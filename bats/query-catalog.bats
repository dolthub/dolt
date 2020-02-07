#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE one_pk (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql <<SQL
CREATE TABLE two_pk (
  pk1 BIGINT NOT NULL COMMENT 'tag:0',
  pk2 BIGINT NOT NULL COMMENT 'tag:1',
  c1 BIGINT COMMENT 'tag:2',
  c2 BIGINT COMMENT 'tag:3',
  c3 BIGINT COMMENT 'tag:4',
  c4 BIGINT COMMENT 'tag:5',
  c5 BIGINT COMMENT 'tag:6',
  PRIMARY KEY (pk1,pk2)
);
SQL
    dolt sql -q "insert into one_pk (pk,c1,c2,c3,c4,c5) values (0,0,0,0,0,0),(1,10,10,10,10,10),(2,20,20,20,20,20),(3,30,30,30,30,30)"
    dolt sql -q "insert into two_pk (pk1,pk2,c1,c2,c3,c4,c5) values (0,0,0,0,0,0,0),(0,1,10,10,10,10,10),(1,0,20,20,20,20,20),(1,1,30,30,30,30,30)"
}

teardown() {
    teardown_common
}

@test "save query" {
    run dolt sql -q "desc dolt_query_catalog"
    [ "$status" -eq 1 ]
    run dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1" -s "my name" -m "my message"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
    run dolt sql -q "desc dolt_query_catalog"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from dolt_query_catalog" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [[ "$output" =~ "id,display_order,name,query,description" ]] || false
    [[ "$output" =~ "my message" ]] || false
    [[ "$output" =~ "my name" ]] || false
    [[ "$output" =~ "select pk,pk1,pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt_query_catalog" ]] || false

    run dolt add dolt_query_catalog
    [ "$status" -eq 0 ]

    run dolt commit -m "Added query catalog"
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "dolt_query_catalog" ]] || false

    run dolt sql -q "select * from dolt_query_catalog" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
}

@test "query catalog conflict" {
    dolt sql -q "select pk,pk1,pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1" -s "name1" -m "my message"
    dolt add .
    dolt commit -m 'Added a test query'

    dolt checkout -b edit_a
    dolt sql -q "update dolt_query_catalog set name='name_a'"
    dolt add .
    dolt commit -m 'Changed name to edit_a'

    dolt checkout master
    dolt checkout -b edit_b
    dolt sql -q "update dolt_query_catalog set name='name_b'"
    dolt add .
    dolt commit -m 'Changed name to edit_b'

    dolt checkout master
    dolt merge edit_a
    run dolt merge edit_b
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Merge conflict in dolt_query_catalog" ]] || false

    run dolt conflicts cat .
    [ "$status" -eq 0 ]
    [[ "$output" =~ "name_a" ]] || false
    [[ "$output" =~ "name_b" ]] || false
}
