#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    skip "unimplemented"

    dolt sql <<SQL
CREATE TABLE keyless (
    c0 int,
    c1 int
);
INSERT INTO keyless VALUES (0,0),(2,2),(1,1),(1,1);
SQL
    dolt commit -am "init"
}

teardown() {
    teardown_common
}

@test "create keyless table" {
    # created in setup()

    run dolt ls
    [ $status -eq 0 ]
    [[ "$lines[@]" = "keyless" ]] || false

    run dolt sql -q "SHOW CREATE TABLE keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "CREATE TABLE \`keyless\` (" ]] || false
    [[ "$lines[@]" = "\`c0\` int," ]] || false
    [[ "$lines[@]" = "\`c1\` int" ]] || false
    [[ "$lines[@]" = ");" ]] || false

    run dolt sql -q "SELECT sum(c0),sum(c1) FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "4,4" ]] || false
}

@test "delete from keyless" {
    run dolt sql -q "DELETE FROM keyless WHERE c0 = 2;"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM keyless ORDER BY c0;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "0,0" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
}

# order will differ without 'ORDER BY' clause
@test "update keyless" {
    run dolt sql -q "UPDATE keyless SET c0 = 9 WHERE c0 = 2;"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM keyless ORDER BY c0;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "0,0" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "9,2" ]] || false
}

# keyless tables allow duplicate rows
@test "keyless table import" {
    cat <<CSV > data.csv
c0,c1
0,0
2,2
1,1
1,1
CSV
    dolt table import -c imported data.csv
    run dolt sql -q "SELECT count(*) FROM imported;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "4" ]] || false
    run dolt sql -q "SELECT sum(c0),sum(c1) FROM imported;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "4,4" ]] || false
}

# updates are always appends
@test "keyless table update" {
    cat <<CSV > data.csv
c0,c1
0,0
2,2
1,1
1,1
CSV
    dolt table import -u keyless data.csv
    run dolt sql -q "SELECT count(*) FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "8" ]] || false
    run dolt sql -q "SELECT sum(c0),sum(c1) FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "8,8" ]] || false
}

@test "keyless diff against working set" {
    dolt sql -q "INSERT INTO keyless VALUES (9,9);"
    run dolt diff
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  +  | 9  | 9  |" ]] || false
}

@test "keyless merge fast-forward" {
    dolt checkout -b other
    dolt sql -q "INSERT INTO keyless VALUES (9,9);"
    dolt commit -am "9,9"
    dolt checkout master
    run dolt merge other
    [ $status -eq 0 ]
    run dolt sql -q "SELECT * FROM keyless WHERE c0 > 6;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "9,9" ]] || false
}

@test "keyless diff branches with identical mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on other"

    run dolt diff master
    [ $status -eq 0 ]
    [ "$lines[@]" = "" ]
}

@test "keyless merge branches with identical mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on other"

    run dolt merge master
    [ $status -eq 0 ]
    run dolt sql -q "SELECT * FROM keyless WHERE c0 > 6 ORDER BY c0;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "7,7" ]] || false
    [[ "$lines[@]" = "8,8" ]] || false
    [[ "$lines[@]" = "9,9" ]] || false
}

@test "keyless diff deletes from two branches" {
    dolt branch left
    dolt checkout -b right

    dolt sql -q "DELETE FROM keyless WHERE c0 = 0;"
    dolt commit -am "deleted ones on right"

    run dolt diff master
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  -  | 0  | 0  |" ]] || false

    dolt checkout left
    dolt sql -q "DELETE FROM keyless WHERE c0 = 2;"
    dolt commit -am "deleted twos on left"

    run dolt diff master
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  -  | 2  | 2  |" ]] || false
}

@test "keyless merge deletes from two branches" {
    dolt branch left
    dolt checkout -b right

    dolt sql -q "DELETE FROM keyless WHERE c0 = 0;"
    dolt commit -am "deleted ones on right"

    dolt checkout left
    dolt sql -q "DELETE FROM keyless WHERE c0 = 2;"
    dolt commit -am "deleted twos on left"

    run dolt merge right
    [ $status -eq 0 ]
    run dolt diff master
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  -  | 0  | 0  |" ]] || false
    [[ "$lines[@]" = "|  -  | 2  | 2  |" ]] || false
}

function make_dupe_table() {
    dolt sql <<SQL
CREATE TABLE dupe (
    a int,
    b int
);
INSERT INTO dupe (a,b) VALUES
    (1,1),(1,1),(1,1),(1,1),(1,1),
    (1,1),(1,1),(1,1),(1,1),(1,1);
SQL
    dolt commit -am "created table dupe"
}

@test "keyless diff duplicate deletes" {
    make_dupe_table

    dolt branch left
    dolt checkout -b right

    dolt sql -q "DELETE FROM dupe LIMIT 2;"
    dolt commit -am "deleted two rows on right"

    run dolt diff master
    [ $status -eq 0 ]
    [[ "$output" = "|  -  | 1  | 1  |" ]] || false
    [[ "$output" = "|  -  | 1  | 1  |" ]] || false

    dolt checkout left
    dolt sql -q "DELETE FROM dupe LIMIT 4;"
    dolt commit -am "deleted four rows on left"

    run dolt diff master
    [ $status -eq 0 ]
    [[ "$output" = "|  -  | 1  | 1  |" ]] || false
    [[ "$output" = "|  -  | 1  | 1  |" ]] || false
    [[ "$output" = "|  -  | 1  | 1  |" ]] || false
    [[ "$output" = "|  -  | 1  | 1  |" ]] || false
}

@test "keyless merge duplicate deletes" {
    make_dupe_table

    dolt branch left
    dolt checkout -b right

    dolt sql -q "DELETE FROM dupe LIMIT 2;"
    dolt commit -am "deleted two rows on right"

    dolt checkout left
    dolt sql -q "DELETE FROM dupe LIMIT 4;"
    dolt commit -am "deleted four rows on left"

    run dolt merge right
    [ $status -eq 0 ]
    run dolt diff master
    [ $status -eq 0 ]
    [[ "$output" = "|  -  | 1  | 1  |" ]] || false
    [[ "$output" = "|  -  | 1  | 1  |" ]] || false
    [[ "$output" = "|  -  | 1  | 1  |" ]] || false
    [[ "$output" = "|  -  | 1  | 1  |" ]] || false
}

@test "keyless diff duplicate updates" {
    make_dupe_table

    dolt branch left
    dolt checkout -b right

    dolt sql -q "UPDATE dupe SET c1 = 2 LIMIT 2;"
    dolt commit -am "updated two rows on right"

    run dolt diff master
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]

    dolt checkout left
    dolt sql -q "UPDATE dupe SET c1 = 2 LIMIT 4;"
    dolt commit -am "updated four rows on left"

    run dolt diff master
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
}

# order will differ without 'ORDER BY' clause
@test "keyless merge duplicate updates" {
    make_dupe_table

    dolt branch left
    dolt checkout -b right

    dolt sql -q "UPDATE dupe SET c1 = 2 LIMIT 2;"
    dolt commit -am "updated two rows on right"

    dolt checkout left
    dolt sql -q "UPDATE dupe SET c1 = 2 LIMIT 4;"
    dolt commit -am "updated four rows on left"

    run dolt merge right
    [ $status -eq 0 ]
    run dolt sql -q "SELECT * FROM dupe ORDER BY c0" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,2" ]] || false
    [[ "$lines[@]" = "1,2" ]] || false
    [[ "$lines[@]" = "1,2" ]] || false
    [[ "$lines[@]" = "1,2" ]] || false
}

@test "keyless sql diff" {
    dolt sql <<SQL
DELETE FROM keyless WHERE c0 = 2;
INSERT INTO keyless VALUES (3,3);
SQL
    dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$lines[@]" = "DELETE FROM keyless WHERE c0=2 AND c1=2 LIMIT 1" ]] || false
    [[ "$lines[@]" = "INSERT INTO keyless VALUES (3,3)" ]] || false

    dolt commit -am "made changes"

    dolt sql -q "UPDATE keyless SET c1 = 13 WHERE c1 = 3;"
    dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$lines[@]" = "DELETE FROM keyless WHERE c0=3 AND c1=3" ]] || false
    [[ "$lines[@]" = "INSERT INTO keyless VALUES (3,13)" ]] || false
}

@test "keyless sql diff as a patch" {
    dolt branch left
    dolt checkout -b right

    dolt sql -q "INSERT INTO keyless VALUES (3,3);"
    dolt commit -am "inserted values (3,3)"

    dolt diff left -r sql
    [ $status -eq 0 ]
    [[ "$lines[@]" = "INSERT INTO keyless VALUES (3,3)" ]] || false

    dolt diff left -r sql > patch.sql
    dolt checkout left
    dolt sql < patch.sql
    run dolt diff right
    [ $status -eq 0 ]
    [ "$output" = "" ]
}
