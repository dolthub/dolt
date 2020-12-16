#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt --keyless sql <<SQL
CREATE TABLE keyless (
    c0 int,
    c1 int
);
INSERT INTO keyless VALUES (0,0),(2,2),(1,1),(1,1);
SQL
    dolt --keyless commit -am "init"
}

teardown() {
    teardown_common
}

@test "create keyless table" {
    # created in setup()

    run dolt --keyless ls
    [ $status -eq 0 ]
    [[ "$output" =~ "keyless" ]] || false

    dolt --keyless sql -q "SHOW CREATE TABLE keyless;"
    run dolt --keyless sql -q "SHOW CREATE TABLE keyless;"
    [ $status -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`keyless\` (" ]] || false
    [[ "$output" =~ "\`c0\` int," ]] || false
    [[ "$output" =~ "\`c1\` int" ]] || false
    [[ "$output" =~ ")" ]] || false

    dolt --keyless sql -q "SELECT sum(c0),sum(c1) FROM keyless;" -r csv
    run dolt --keyless sql -q "SELECT sum(c0),sum(c1) FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" =~ "4,4" ]] || false
}

@test "delete from keyless" {
    run dolt --keyless sql -q "DELETE FROM keyless WHERE c0 = 2;"
    [ $status -eq 0 ]

    run dolt --keyless sql -q "SELECT * FROM keyless ORDER BY c0;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "0,0" ]] || false
    [[ "${lines[2]}" = "1,1" ]] || false
    [[ "${lines[3]}" = "1,1" ]] || false
}

@test "update keyless" {
    run dolt --keyless sql -q "UPDATE keyless SET c0 = 9 WHERE c0 = 2;"
    [ $status -eq 0 ]

    run dolt --keyless sql -q "SELECT * FROM keyless ORDER BY c0;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "0,0" ]] || false
    [[ "${lines[2]}" = "1,1" ]] || false
    [[ "${lines[3]}" = "1,1" ]] || false
    [[ "${lines[4]}" = "9,2" ]] || false
}

# keyless tables allow duplicate rows
@test "keyless table import" {
    skip "unimplemented"
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
    run dolt sql -q "SELECT * FROM tbl;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "0,0" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "2,2" ]] || false
}

# updates are always appends
@test "keyless table update" {
    skip "unimplemented"
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

@test "keyless diff against working set" {
    skip "unimplemented"
    dolt sql -q "INSERT INTO keyless VALUES (9,9);"
    run dolt diff
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  +  | 9  | 9  |" ]] || false
}

@test "keyless merge fast-forward" {
    skip "unimplemented"
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
    skip "unimplemented"
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
    skip "unimplemented"
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
    skip "unimplemented"
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
    skip "unimplemented"
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
    skip "unimplemented"
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
    skip "unimplemented"
    make_dupe_table

    dolt branch left
    dolt checkout -b right

    dolt sql -q "DELETE FROM dupe LIMIT 2;"
    dolt commit -am "deleted two rows on right"

    dolt checkout left
    dolt sql -q "DELETE FROM dupe LIMIT 4;"
    dolt commit -am "deleted four rows on left"

    run dolt merge right
    [ $status -ne 0 ]
    [[ "$output" = "conflict" ]] || false
}

@test "keyless diff duplicate updates" {
    skip "unimplemented"
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
    skip "unimplemented"
    make_dupe_table

    dolt branch left
    dolt checkout -b right

    dolt sql -q "UPDATE dupe SET c1 = 2 LIMIT 2;"
    dolt commit -am "updated two rows on right"

    dolt checkout left
    dolt sql -q "UPDATE dupe SET c1 = 2 LIMIT 4;"
    dolt commit -am "updated four rows on left"

    run dolt merge master
    [ $status -ne 0 ]
    [[ "$output" = "conflict" ]] || false
}

@test "keyless sql diff" {
    skip "unimplemented"
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
    skip "unimplemented"
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


@test "keyless tables read in sorted order" {
    skip "unimplemented"
    run dolt sql -q "SELECT * FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "0,0" ]] || false
    [[ "$output" = "1,1" ]] || false
    [[ "$output" = "1,1" ]] || false
    [[ "$output" = "2,2" ]] || false
}

# tables are read/stored in sorted order
@test "keyless table replace" {
    skip "unimplemented"
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
    skip "unimplemented"
    dolt sql -q "UPDATE keyless SET c1 = 9 where c0 = 2;"
    run dolt diff
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  -  | 2  | 2  |" ]] || false
    [[ "$lines[@]" = "|  +  | 2  | 9  |" ]] || false
}

# in-place updates create become drop/add
@test "keyless sql diff with in-place updates (working set)" {
    skip "unimplemented"
    dolt sql -q "UPDATE keyless SET c1 = 9 where c0 = 2;"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$lines[@]" = "DELETE FROM keyless WHERE c0 = 2 AND c1 = 2 LIMIT 1" ]] || false
    [[ "$lines[@]" = "INSERT INTO keyless (c0,c1) VALUES (2,9);" ]] || false
}

# update patch always recreates identical branches
@test "keyless updates as a sql diff patch" {
    skip "unimplemented"
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
    skip "unimplemented"
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
    skip "unimplemented"
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
    skip "unimplemented"
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
    skip "unimplemented"
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
    skip "unimplemented"
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
    skip "unimplemented"
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
    skip "unimplemented"
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
    skip "unimplemented"
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
    skip "unimplemented"
    dolt sql <<SQL
DELETE FROM keyless WHERE c0 = 2;
INSERT INTO keyless VALUES (2,2)
SQL
    run dolt diff
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

@test "keyless diff delete+add on two branches" {
    skip "unimplemented"
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
    skip "unimplemented"
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
