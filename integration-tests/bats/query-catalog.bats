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
    run dolt sql -q "select pk from one_pk" -s "select"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]
}

@test "query-catalog: save query with message" {
    run dolt sql -q "select pk from one_pk" -s "select" -m "my message"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 8 ]

    run dolt sql -l -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "select" ]] || false
    [[ "$output" =~ "my message" ]] || false
}

@test "query-catalog: empty directory" {
    mkdir empty && cd empty

    run dolt sql -q "show databases" --save name
    [ "$status" -ne 0 ]
    [[ ! "$output" =~ panic ]] || false
    [[ "$output" =~ "The current directory is not a valid dolt repository." ]] || false

    run dolt sql --list-saved
    [ "$status" -ne 0 ]
    [[ ! "$output" =~ panic ]] || false
    [[ "$output" =~ "The current directory is not a valid dolt repository." ]] || false

    run dolt sql --execute name
    [ "$status" -ne 0 ]
    [[ ! "$output" =~ panic ]] || false
    [[ "$output" =~ "The current directory is not a valid dolt repository." ]] || false
}

@test "query-catalog: executed saved" {
    Q1="select pk, pk1, pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1 order by 1"
    Q2="select pk from one_pk order by pk"
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
    echo "$output"
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
name2,2,name2,select pk from one_pk order by pk,""
EOF
)

    run dolt sql --list-saved -r csv
    echo "$output"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false

    # update an existing query, and verify query catalog is updated
    Q1_UPDATED="select pk, pk1, pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1 and pk < 3 order by 1 desc"
    dolt sql -q "$Q1_UPDATED" -s name1

    # execute list-saved and verify output
    EXPECTED=$(cat <<'EOF'
id,display_order,name,query,description
name1,1,name1,"select pk, pk1, pk2 from one_pk,two_pk where one_pk.c1=two_pk.c1 and pk < 3 order by 1 desc",""
name2,2,name2,select pk from one_pk order by pk,""
EOF
)

    run dolt sql --list-saved -r csv
    echo "$output"
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
