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

@test "executed saved" {
    Q1="select pk, pk1, pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1"
    Q1_UPDATED="select pk, pk1, pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1 and pk < 3"
    Q2="select pk from one_pk"
    dolt sql -q "$Q1" -s name1
    dolt sql -q "$Q2" -s name2

    # executed Q1 and verify output
    EXPECTED=$(echo -e "pk,pk1,pk2\n0,0,0\n1,0,1\n2,1,0\n3,1,1")
    run dolt sql -r csv -x name1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    # executed Q2 and verify output
    EXPECTED=$(echo -e "pk\n0\n1\n2\n3")
    run dolt sql -r csv -x name2
    echo $output
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    # execute list-saved and verify output
    EXPECTED=$(echo -e "id,display_order,name,query,description\nname1,1,name1,\"$Q1\",\nname2,2,name2,$Q2,")
    run dolt sql --list-saved -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    # update an existing verify output, and verify query catalog is updated
    dolt sql -q "$Q1_UPDATED" -s name1

    EXPECTED=$(echo -e "pk,pk1,pk2\n2,1,0\n1,0,1\n0,0,0")
    run dolt sql -r csv -x name1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
    EXPECTED=$(echo -e "id,display_order,name,query,description\nname1,1,name1,\"$Q1_UPDATED\",\nname2,2,name2,$Q2,")
    run dolt sql --list-saved -r csv
    [ "$status" -eq 0 ]
    echo $output
    echo $EXPECTED
    [[ "$output" =~ "$EXPECTED" ]] || false
}