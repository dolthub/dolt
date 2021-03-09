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
    assert_feature_version
    teardown_common
}

@test "query-catalog: save query" {
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

@test "query-catalog: conflict" {
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

@test "query-catalog: executed saved" {
    Q1="select pk, pk1, pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1 order by 1"
    Q2="select pk from one_pk"
    dolt sql -q "$Q1" -s name1
    dolt sql -q "$Q2" -s name2

    # save Q1 and verify output
    EXPECTED=$(cat <<'EOF'
pk,pk1,pk2
0,0,0
1,0,1
2,1,0
3,1,1
EOF
)

    run dolt sql -r csv -x name1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    # save Q2 and verify output
    EXPECTED=$(cat <<'EOF'
pk
0
1
2
3
EOF
)

    run dolt sql -r csv -x name2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    # execute list-saved and verify output. I have no idea why the
    # query on the second line isn't quoted, assuming it's a bash
    # interpretation thing. Has quotes when run by hand.
    EXPECTED=$(cat <<'EOF'
id,display_order,name,query,description
name1,1,name1,"select pk, pk1, pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1 order by 1",""
name2,2,name2,select pk from one_pk,""
EOF
)

    run dolt sql --list-saved -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    # update an existing query, and verify query catalog is updated
    Q1_UPDATED="select pk, pk1, pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1 and pk < 3 order by 1 desc"
    dolt sql -q "$Q1_UPDATED" -s name1

    # execute list-saved and verify output
    EXPECTED=$(cat <<'EOF'
id,display_order,name,query,description
name1,1,name1,"select pk, pk1, pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1 and pk < 3 order by 1 desc",""
name2,2,name2,select pk from one_pk,""
EOF
)

    run dolt sql --list-saved -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
    
    EXPECTED=$(cat <<'EOF'
pk,pk1,pk2
2,1,0
1,0,1
0,0,0
EOF
)

    # Execute updated saved query and verify once output
    run dolt sql -r csv -x name1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}
