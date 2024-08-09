#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

extract_value() {
    key="$1"
    input="$2"
    echo "$input" | awk "
        BEGIN { in_value = 0 }
        /$key: {/ { in_value = 1; next }
        match("'$0'", /$key: /) { print substr("'$0'", RSTART+RLENGTH) }
        /}/ { if (in_value) { in_value = 0 } }
        in_value { gsub(/^[ \t]+/, \"\"); print }
    "
}

assert_has_key() {
    key="$1"
    input="$2"
    extracted=$(extract_value "$key" "$input")
    if [[ -z $extracted ]]; then
        echo "Expected to find key $key"
        return 1
    else
        return 0
    fi
}

assert_has_key_value() {
    key="$1"
    value="$2"
    input="$3"
    extracted=$(extract_value "$key" "$input")
    if [[ "$extracted" != "$value" ]]; then
        echo "Expected key $key to have value $value, instead found $extracted"
        return 1
    else
        return 0
    fi
}

@test "show: on initialized repo" {
    run dolt show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Initialize data repository" ]] || false
}

@test "show: log zero refs" {
    dolt commit --allow-empty -m "Commit One"
    dolt tag v1
    run dolt show
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit One" ]] || false
    [[ "$output" =~ "tag: v1" ]] || false

    dolt commit --allow-empty -m "Commit Two"
    dolt tag v2
    run dolt show
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit Two" ]] || false
    [[ "$output" =~ "tag: v2" ]] || false
}

@test "show: log one ref" {
    dolt commit --allow-empty -m "Commit One"
    dolt tag v1

    dolt commit --allow-empty -m "Commit Two"
    dolt tag v2

    run dolt show v1
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit One" ]] || false
    [[ "$output" =~ "tag: v1" ]] || false
}

@test "show: log two refs" {
    dolt commit --allow-empty -m "Commit One"
    dolt tag v1

    dolt commit --allow-empty -m "Commit Two"
    dolt tag v2

    run dolt show v1 v2
    [ $status -eq 0 ]
    [[ "$output" =~ "Commit One" ]] || false
    [[ "$output" =~ "tag: v1" ]] || false
    [[ "$output" =~ "Commit Two" ]] || false
    [[ "$output" =~ "tag: v2" ]] || false
}

@test "show: log and diff" {
    dolt sql -q "create table testtable (pk int PRIMARY KEY)"
    dolt add .
    dolt commit -m "commit: add table"

    run dolt show
    [ $status -eq 0 ]
    [[ "$output" =~ "commit: add table" ]] || false
    [[ "$output" =~ "diff --dolt a/testtable b/testtable" ]] || false
    [[ "$output" =~ "added table" ]] || false
    [[ "$output" =~ "+CREATE TABLE \`testtable\` (" ]] || false
    [[ "$output" =~ "+  \`pk\` int NOT NULL," ]] || false
    [[ "$output" =~ "+  PRIMARY KEY (\`pk\`)" ]] || false

    dolt sql -q 'insert into testtable values (4)'
    dolt add .
    dolt commit -m "commit: add values"

    run dolt show
    [ $status -eq 0 ]
    [[ "$output" =~ "commit: add values" ]] || false
    [[ "$output" =~ "|   | pk |" ]] || false
    [[ "$output" =~ "| + | 4  |" ]] || false
}

@test "show: --no-pretty" {
    dolt commit --allow-empty -m "commit: initialize table1"
    run dolt show --no-pretty
    [ $status -eq 0 ]
    [[ "$output" =~ "SerialMessage" ]] || false
    assert_has_key "Name" "$output"
    assert_has_key_value "Name" "Bats Tests" "$output"
    assert_has_key_value "Desc" "commit: initialize table1" "$output"
    assert_has_key_value "Name" "Bats Tests" "$output"
    assert_has_key_value "Email" "bats@email.fake" "$output"
    assert_has_key "Timestamp" "$output"
    assert_has_key "UserTimestamp" "$output"
    assert_has_key_value "Height" "2" "$output"
    assert_has_key "RootValue" "$output"
    assert_has_key "Parents" "$output"
    assert_has_key "ParentClosure" "$output"
}

@test "show: --no-pretty commit hash" {
    dolt commit --allow-empty -m "commit: initialize table1"
    hash=$(dolt sql -q "select dolt_hashof('head');" -r csv | tail -n 1)
    run dolt show --no-pretty $hash
    [ $status -eq 0 ]
    [[ "$output" =~ "SerialMessage" ]] || false
    assert_has_key "Name" "$output"
    assert_has_key_value "Name" "Bats Tests" "$output"
    assert_has_key_value "Desc" "commit: initialize table1" "$output"
    assert_has_key_value "Name" "Bats Tests" "$output"
    assert_has_key_value "Email" "bats@email.fake" "$output"
    assert_has_key "Timestamp" "$output"
    assert_has_key "UserTimestamp" "$output"
    assert_has_key_value "Height" "2" "$output"
    assert_has_key "RootValue" "$output"
    assert_has_key "Parents" "$output"
    assert_has_key "ParentClosure" "$output"
}

@test "show: HEAD root" {
    dolt sql -q "create table table1 (pk int PRIMARY KEY)"
    dolt sql -q "insert into table1 values (1), (2), (3)"
    dolt add .
    dolt commit -m "commit: initialize table1"
    dolt sql -q "create table table2 (pk int PRIMARY KEY)"
    dolt sql -q "insert into table1 values (4), (5), (6)"
    dolt add .
    dolt sql -q "create table table3 (pk int PRIMARY KEY)"
    dolt sql -q "insert into table1 values (7), (8), (9)"
    head=$(dolt show --no-pretty)
    rootValue=$(extract_value RootValue "$head")
    echo rootValue=$rootValue
    [[ ! -z $rootValue ]] || false

    run dolt show $rootValue
    [ $status -eq 0 ]
    [[ "$output" =~ "table1" ]] || false
    [[ ! "$output" =~ "table2" ]] || false
    [[ ! "$output" =~ "table3" ]] || false
}

@test "show: WORKING" {
    dolt sql -q "create table table1 (pk int PRIMARY KEY)"
    dolt sql -q "insert into table1 values (1), (2), (3)"
    dolt add .
    dolt commit -m "commit: initialize table1"
    dolt sql -q "create table table2 (pk int PRIMARY KEY)"
    dolt sql -q "insert into table1 values (4), (5), (6)"
    dolt add .
    dolt sql -q "create table table3 (pk int PRIMARY KEY)"
    dolt sql -q "insert into table1 values (7), (8), (9)"
    run dolt show WORKING
    [ $status -eq 0 ]
    [[ "$output" =~ "table1" ]] || false
    [[ "$output" =~ "table2" ]] || false
    [[ "$output" =~ "table3" ]] || false
}

@test "show: STAGED" {
    dolt sql -q "create table table1 (pk int PRIMARY KEY)"
    dolt sql -q "insert into table1 values (1), (2), (3)"
    dolt add .
    dolt commit -m "commit: initialize table1"
    dolt sql -q "create table table2 (pk int PRIMARY KEY)"
    dolt sql -q "insert into table1 values (4), (5), (6)"
    dolt add .
    dolt sql -q "create table table3 (pk int PRIMARY KEY)"
    dolt sql -q "insert into table1 values (7), (8), (9)"
    run dolt show STAGED
    [ $status -eq 0 ]
    [[ "$output" =~ "table1" ]] || false
    [[ "$output" =~ "table2" ]] || false
    [[ ! "$output" =~ "table3" ]] || false
}

@test "show: table" {
    dolt sql -q "create table table1 (pk int PRIMARY KEY)"
    dolt sql -q "insert into table1 values (1), (2), (3)"
    dolt add .
    dolt commit -m "commit: initialize table1"
    dolt sql -q "create table table2 (pk int PRIMARY KEY)"
    dolt sql -q "insert into table1 values (4), (5), (6)"
    dolt add .
    dolt sql -q "create table table3 (pk int PRIMARY KEY)"
    dolt sql -q "insert into table1 values (7), (8), (9)"
    workingRoot=$(dolt show WORKING)
    tableAddress=$(extract_value table1 "$workingRoot")

    run dolt show $tableAddress
    assert_has_key Schema "$output"
    assert_has_key Violations "$output"
    assert_has_key Autoinc "$output"
    assert_has_key "Primary index" "$output"
    assert_has_key "Secondary indexes" "$output"

}

@test "show: pretty commit from hash" {
    dolt tag v0
    dolt commit --allow-empty -m "commit1"

    head=$(dolt show --no-pretty)
    parentHash=$(extract_value Parents "$head")

    run dolt show "$parentHash"
    [[ "$output" =~ "tag: v0" ]] || false
}

@test "show: non-existent branch" {
    run dolt show branch1
    [ $status -eq 1 ]
    [[ "$output" =~ "branch not found: branch1" ]] || false
}

@test "show: primary index leaf" {
    dolt sql <<EOF
create table test(pk int primary key, t text, j json);
insert into test values (0, "Hello", "{}"), (1, "World", "[]");
EOF
    run dolt show "#9heeqrj6idph7snnko484sqnobu2r46i"
    [ $status -eq 0 ]
    [[ "$output" =~ "SerialMessage" ]] || false
    [[ "$output" =~ "{ key: 00000000 value:  #0isi5776c0lu0d7rvsnfl80gsdisilsa,  #e6sucun84ck3bgc1p9lorkibp30mvd2f }" ]] || false
    [[ "$output" =~ "{ key: 01000000 value:  #8scr7d6rtnafqovoa7d06em7jkpil9gg,  #8arugs9qup4pvpmqbf64lpkm9f6cdv74 }" ]] || false
}

@test "show: blob" {
    dolt sql <<EOF
create table test(pk int primary key, t text, j json);
insert into test values (0, "Hello", "{}"), (1, "World", "[]");
EOF
    run dolt show "#0isi5776c0lu0d7rvsnfl80gsdisilsa"
    [ $status -eq 0 ]
    [[ "$output" =~ "SerialMessage" ]] || false
    [[ "$output" =~ "Blob - Hello" ]] || false
}

@test "show: primary index non-leaf" {
    dolt sql <<EOF
create table test(pk int primary key);
insert into test values (1);
EOF
    for i in {1..10}; do dolt sql -q "insert ignore into test select pk*2+$i from test;"; done
    run dolt show "#r9g8a8r570drje8lbnn72jftu49gkq9t"
    [ $status -eq 0 ]
    [[ "$output" =~ "SerialMessage" ]] || false
    [[ "$output" =~ "{ key: d5010000 ref: #5eiul2kmip341rse0besd0bv0u07jhf1 }" ]] || false
    [[ "$output" =~ "{ key: f40b0000 ref: #vpuf5ccvph0i6stls48a0bj7as5k2aka }" ]] || false
}

@test "show: schema" {
    dolt sql <<EOF
create table test(pk int primary key);
insert into test values (1);
EOF
    for i in {1..10}; do dolt sql -q "insert ignore into test select pk*2+$i from test;"; done
    run dolt show "#svtgrd12uf2u1q492it1aod9i5cqssv9"
    [ $status -eq 0 ]
    [[ "$output" =~ "SerialMessage" ]] || false
    [[ "$output" =~ "Columns: [" ]] || false
    [[ "$output" =~ "Primary Index: {" ]] || false
    [[ "$output" =~ "Secondary Indexes: [" ]] || false
    [[ "$output" =~ "Comment: " ]] || false
    [[ "$output" =~ "Checks: [" ]] || false
    [[ "$output" =~ "Collation: utf8mb4_0900_bin" ]] || false
}