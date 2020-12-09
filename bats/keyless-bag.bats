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

@test "keyless tables read in sorted order" {
    run dolt sql -q "SELECT * FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "0,0" ]] || false
    [[ "$output" = "1,1" ]] || false
    [[ "$output" = "1,1" ]] || false
    [[ "$output" = "2,2" ]] || false
}

# tables read in sorted order
@test "keyless table import" {
    cat <<CSV > data.csv
c0,c1
0,0
2,2
1,1
1,1
CSV
    dolt table import -c tbl data.csv
    run dolt sql -q "SELECT count(*) FROM tbl;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "4" ]] || false
    run dolt sql -q "SELECT * FROM tbl;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "0,0" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "2,2" ]] || false
}

# updates are appends to the end of the table
# tables read in sorted order
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
    run dolt sql -q "SELECT * FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "0,0" ]] || false
    [[ "$lines[@]" = "0,0" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "2,2" ]] || false
    [[ "$lines[@]" = "2,2" ]] || false
}

# tables are read/stored in sorted order
@test "keyless table replace" {
    cat <<CSV > data.csv
c0,c1
0,0
2,2
1,1
1,1
CSV
    run dolt table import -r keyless data.csv
    [ $status -eq 0 ]
    run dolt diff
    [ $status -eq 0 ]
    [ "$output" = "" ]

    cat <<CSV > data2.csv
c0,c1
0,0
1,1
1,1
2,2
CSV
    run dolt table import -r keyless data2.csv
    [ $status -eq 0 ]
    run dolt diff
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

# in-place updates create become drop/add
@test "keyless diff with in-place updates (working set)" {
    dolt sql -q "UPDATE keyless SET c1 = 9 where c0 = 2;"
    run dolt diff
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  -  | 2  | 2  |" ]] || false
    [[ "$lines[@]" = "|  +  | 2  | 9  |" ]] || false
}

# in-place updates create become drop/add
@test "keyless sql diff with in-place updates (working set)" {
    dolt sql -q "UPDATE keyless SET c1 = 9 where c0 = 2;"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$lines[@]" = "DELETE FROM keyless WHERE c0 = 2 AND c1 = 2 LIMIT 1" ]] || false
    [[ "$lines[@]" = "INSERT INTO keyless (c0,c1) VALUES (2,9);" ]] || false
}

# update patch always recreates identical branches
@test "keyless updates as a sql diff patch" {
    dolt branch left
    dolt checkout -b right

    dolt sql -q "UPDATE keyless SET c1 = 22 WHERE c1 = 9;"
    dolt commit -am "updates (2,2) -> (2,9)"

    dolt diff left -r sql
    [ $status -eq 0 ]
    [[ "$lines[@]" = "DELETE FROM keyless WHERE c0 = 2 AND c1 = 2 LIMIT 1" ]] || false
    [[ "$lines[@]" = "INSERT INTO keyless (c0,c1) VALUES (2,9);" ]] || false

    dolt diff left -r sql > patch.sql
    dolt checkout left
    dolt sql < patch.sql

    run dolt diff right
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

# in-place updates diff as drop/add
@test "keyless diff with in-place updates (branches)" {
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "added rows"
    dolt branch other

    dolt sql -q "UPDATE keyless SET c1 = c1+10 WHERE c0 > 6"
    dolt commit -am "updated on master"

    dolt checkout other
    dolt sql -q "UPDATE keyless SET c1 = c1+20 WHERE c0 > 6"
    dolt commit -am "updated on other"

    run dolt diff master
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  -  | 7  | 17  |" ]] || false
    [[ "$lines[@]" = "|  +  | 7  | 27  |" ]] || false
    [[ "$lines[@]" = "|  -  | 8  | 18  |" ]] || false
    [[ "$lines[@]" = "|  +  | 8  | 28  |" ]] || false
    [[ "$lines[@]" = "|  -  | 9  | 19  |" ]] || false
    [[ "$lines[@]" = "|  +  | 9  | 29  |" ]] || false
}

# where in-place updates are divergent, both versions are kept on merge
# same for hidden key and bag semantics
@test "keyless merge with in-place updates (branches)" {
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "added rows"
    dolt branch other

    dolt sql -q "UPDATE keyless SET c1 = c1+10 WHERE c0 > 6"
    dolt commit -am "updated on master"

    dolt checkout other
    dolt sql -q "UPDATE keyless SET c1 = c1+20 WHERE c0 > 6"
    dolt commit -am "updated on other"

    run dolt merge master
    [ $status -ne 0 ]
    [[ "$output" = "conflict" ]] || false
}

# bag semantics diffs membership, not order
@test "keyless diff branches with reordered mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (9,9),(8,8),(7,7);"
    dolt commit -am "inserted on other"

    run dolt diff master
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

# bag semantics diffs membership, not order
@test "keyless merge branches with reordered mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (9,9),(8,8),(7,7);"
    dolt commit -am "inserted on other"

    run dolt merge master
    [ $status -eq 0 ]
     run dolt sql -q "SELECT count(*) FROM keyless WHERE c0 > 6;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "3" ]] || false
    run dolt sql -q "SELECT * FROM keyless WHERE c0 > 6;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "7,7" ]] || false
    [[ "$lines[@]" = "8,8" ]] || false
    [[ "$lines[@]" = "9,9" ]] || false
}

# convergent row data history with convergent data has convergent storage representation
@test "keyless diff branches with convergent mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql <<SQL
INSERT INTO keyless VALUES (9,19),(8,18),(7,17);
UPDATE keyless SET (c0,c1) = (7,7) WHERE c1 = 19;
UPDATE keyless SET (c0,c1) = (9,9) WHERE c1 = 17;
SQL
    dolt commit -am "inserted on other"

    run dolt diff master
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

# convergent row data has convergent storage representation
@test "keyless merge branches with convergent mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql <<SQL
INSERT INTO keyless VALUES (9,19),(8,18),(7,17);
UPDATE keyless SET (c0,c1) = (7,7) WHERE c1 = 19;
UPDATE keyless SET (c0,c1) = (8,8) WHERE c1 = 18;
UPDATE keyless SET (c0,c1) = (9,9) WHERE c1 = 17;
SQL
    dolt commit -am "inserted on other"

    run dolt merge master
    [ $status -ne 0 ]
    [[ "$output" = "conflict" ]] || false
}

# bag semantics give minimal diff
@test "keyless diff branches with offset mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(7,7),(8,8),(9,9);"
    dolt commit -am "inserted on other"

    run dolt diff master
    [ $status -eq 0 ]
    # todo: assert line-length == 1
    [[ "$lines[@]" = "|  +  | 7  | 7  |" ]] || false
}

@test "keyless merge branches with offset mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(7,7),(8,8),(9,9);"
    dolt commit -am "inserted on other"

    run dolt merge master
    [ $status -ne 0 ]
    [[ "$output" = "conflict" ]] || false
}

@test "keyless diff delete+add against working" {
    dolt sql <<SQL
DELETE FROM keyless WHERE c0 = 2;
INSERT INTO keyless VALUES (2,2)
SQL
    run dolt diff
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

@test "keyless diff delete+add on two branches" {
    dolt branch left
    dolt checkout -b right

    dolt sql -q "DELETE FROM keyless WHERE c0 = 2;"
    dolt commit -am "deleted ones on right"

    run dolt diff master
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  -  | 2  | 2  |" ]] || false

    dolt checkout left
    dolt sql -q "INSERT INTO keyless VALUES (2,2);"
    dolt commit -am "deleted twos on left"

    run dolt diff master
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  +  | 2  | 2  |" ]] || false
}

# row gets deleted from the middle and added to the end
@test "keyless merge delete+add on two branches" {
    dolt branch left
    dolt checkout -b right

    dolt sql -q "DELETE FROM keyless WHERE c0 = 2;"
    dolt commit -am "deleted twos on right"

    dolt checkout left
    dolt sql -q "INSERT INTO keyless VALUES (2,2);"
    dolt commit -am "inserted twos on left"

    run dolt merge right
    [ $status -eq 0 ]
    run dolt diff master
    [ $status -eq 0 ]
    [ "$output" = "" ]
}
