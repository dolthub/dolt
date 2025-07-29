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

@test "query-catalog: can describe dolt_query_catalog" {
    run dolt sql -q "desc dolt_query_catalog" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "id" ]] || false
    [[ "$output" =~ "query" ]] || false
}

@test "query-catalog: save query without message" {
    EXPECTED=$(cat <<'EOF'
pk
0
1
2
3
EOF
)
    run dolt sql -q "select pk from one_pk" -s "select" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "query-catalog: save query with message" {
    dolt sql -q "select pk from one_pk" -s "select" -m "my message"
    run dolt sql -l -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "select" ]] || false
    [[ "$output" =~ "my message" ]] || false
}

@test "query-catalog: can list saved queries" {
    Q1="select pk, pk1, pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1 order by 1"
    Q2="select pk from one_pk order by pk"
    dolt sql -q "$Q1" -s name1
    dolt sql -q "$Q2" -s name2

    EXPECTED=$(cat <<'EOF'
id,display_order,name,query,description
name1,1,name1,"select pk, pk1, pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1 order by 1",""
name2,2,name2,select pk from one_pk order by pk,""
EOF
)

    run dolt sql --list-saved -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "query-catalog: can execute saved queries" {
    Q1="select pk, pk1, pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1 order by 1"
    dolt sql -q "$Q1" -s name1

    EXPECTED=$(cat <<'EOF'
pk,pk1,pk2
0,0,0
1,0,1
2,1,0
3,1,1
EOF
)

    run dolt sql -x name1 -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "query-catalog: can update saved query with --save" {
    Q1="select pk, pk1, pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1 order by 1"
    Q2="select pk from one_pk order by pk"
    dolt sql -q "$Q1" -s name1

    EXPECTED=$(cat <<'EOF'
id,display_order,name,query,description
name1,1,name1,"select pk, pk1, pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1 order by 1",""
EOF
)

    run dolt sql --list-saved -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    dolt sql -q "$Q2" -s name1

    # execute list-saved and verify output. I have no idea why the
    # query isn't quoted, but I assume it's a bash
    # interpretation thing. Has quotes when run by hand.
    EXPECTED=$(cat <<'EOF'
id,display_order,name,query,description
name1,1,name1,select pk from one_pk order by pk,""
EOF
)

    run dolt sql --list-saved -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}