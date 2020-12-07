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

@test "keyless tables read in insert order" {
    run dolt sql -q "SELECT * FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "0,0" ]] || false
    [[ "$lines[@]" = "2,2" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
}

# hidden key will maintain the import row order
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
    [[ "$lines[@]" = "2,2" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
}

# updates are appends to the end of the table
# import order is maintained
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
    [[ "$lines[@]" = "2,2" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "0,0" ]] || false
    [[ "$lines[@]" = "2,2" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
    [[ "$lines[@]" = "1,1" ]] || false
}

# row order is respected, sorting data creates a diff
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
    [[ "$lines[@]" = "|  -  | 2  | 2  |" ]] || false
    [[ "$lines[@]" = "|  +  | 1  | 1  |" ]] || false
    # third row creates no diff
    [[ "$lines[@]" = "|  -  | 1  | 1  |" ]] || false
    [[ "$lines[@]" = "|  +  | 2  | 2  |" ]] || false
}

# in-place updates create cell-wise diffs
@test "keyless diff with in-place updates (working set)" {
    dolt sql -q "UPDATE keyless SET c1 = 9 where c0 = 2;"
    run dolt diff
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  <  | 2  | 2  |" ]] || false
    [[ "$lines[@]" = "|  >  | 2  | 9  |" ]] || false
}

# diff -r sql can't create update statements
@test "keyless sql diff with in-place updates (working set)" {
    dolt sql -q "UPDATE keyless SET c1 = 9 where c0 = 2;"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$lines[@]" = "DELETE FROM keyless WHERE c0 = 2 AND c1 = 2 LIMIT 1" ]] || false
    [[ "$lines[@]" = "INSERT INTO keyless (c0,c1) VALUES (2,9);" ]] || false
}

# update patch deletes from middle and inserts at the end
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
    [[ "$lines[@]" = "|  -  | 2  | 9  |" ]] || false
    [[ "$lines[@]" = "|  +  | 2  | 9  |" ]] || false
}

# in-place updates create cell-wise diffs
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
    [[ "$lines[@]" = "|  <  | 7  | 27  |" ]] || false
    [[ "$lines[@]" = "|  >  | 7  | 17  |" ]] || false
    [[ "$lines[@]" = "|  <  | 8  | 28  |" ]] || false
    [[ "$lines[@]" = "|  >  | 8  | 18  |" ]] || false
    [[ "$lines[@]" = "|  <  | 9  | 29  |" ]] || false
    [[ "$lines[@]" = "|  >  | 9  | 19  |" ]] || false
}

# where in-place updates are divergent, both versions are kept
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
    [ $status -eq 0 ]
    run dolt sql -q "SELECT * FROM keyless WHERE c0 > 6;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "7,27" ]] || false
    [[ "$lines[@]" = "7,17" ]] || false
    [[ "$lines[@]" = "8,28" ]] || false
    [[ "$lines[@]" = "8,18" ]] || false
    [[ "$lines[@]" = "9,29" ]] || false
    [[ "$lines[@]" = "9,19" ]] || false
}

# hidden key creates insert order, which creates a diff
@test "keyless diff branches with reordered mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (9,9),(8,8),(7,7),;"
    dolt commit -am "inserted on other"

    run dolt diff master
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  -  | 7  | 7  |" ]] || false
    [[ "$lines[@]" = "|  +  | 9  | 9  |" ]] || false
        # row (8,8) creates no diff
    [[ "$lines[@]" = "|  -  | 9  | 9  |" ]] || false
    [[ "$lines[@]" = "|  +  | 7  | 7  |" ]] || false
}

# hidden key creates insert order, which creates a diff
@test "keyless merge branches with reordered mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (9,9),(8,8),(7,7),;"
    dolt commit -am "inserted on other"

    run dolt merge master
    [ $status -eq 0 ]
    run dolt sql -q "SELECT * FROM keyless WHERE c0 > 6;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "7,7" ]] || false
    [[ "$lines[@]" = "9,9" ]] || false
    [[ "$lines[@]" = "8,8" ]] || false
    [[ "$lines[@]" = "9,9" ]] || false
    [[ "$lines[@]" = "7,7" ]] || false
}

# hidden key is created on insert and doesn't change
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
    [[ "$lines[@]" = "|  -  | 7  | 7  |" ]] || false
    [[ "$lines[@]" = "|  +  | 7  | 7  |" ]] || false
    [[ "$lines[@]" = "|  -  | 8  | 8  |" ]] || false
    [[ "$lines[@]" = "|  +  | 8  | 8  |" ]] || false
    [[ "$lines[@]" = "|  -  | 9  | 9  |" ]] || false
    [[ "$lines[@]" = "|  +  | 9  | 9  |" ]] || false
}

# hidden key is created on insert and doesn't change
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
    [ $status -eq 0 ]
    run dolt sql -q "SELECT * FROM keyless WHERE c0 > 6;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "7,7" ]] || false
    [[ "$lines[@]" = "7,7" ]] || false
    [[ "$lines[@]" = "8,8" ]] || false
    [[ "$lines[@]" = "8,8" ]] || false
    [[ "$lines[@]" = "9,9" ]] || false
    [[ "$lines[@]" = "9,9" ]] || false
}

# extra row creates a cascading diff
@test "keyless diff branches with offset mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(7,7),(8,8),(9,9);"
    dolt commit -am "inserted on other"

    run dolt diff master
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  +  | 7  | 7  |" ]] || false
    [[ "$lines[@]" = "|  -  | 8  | 8  |" ]] || false
    [[ "$lines[@]" = "|  +  | 8  | 8  |" ]] || false
    [[ "$lines[@]" = "|  -  | 9  | 9  |" ]] || false
    [[ "$lines[@]" = "|  +  | 9  | 9  |" ]] || false
}

# extra row creates a cascading diff
@test "keyless merge branches with offset mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(7,7),(8,8),(9,9);"
    dolt commit -am "inserted on other"

    run dolt merge master
    [ $status -eq 0 ]
    run dolt sql -q "SELECT * FROM keyless WHERE c0 > 6;" -r csv
    [ $status -eq 0 ]
    [[ "$lines[@]" = "7,7" ]] || false
    [[ "$lines[@]" = "7,7" ]] || false
    [[ "$lines[@]" = "8,8" ]] || false
    [[ "$lines[@]" = "8,8" ]] || false
    [[ "$lines[@]" = "9,9" ]] || false
    [[ "$lines[@]" = "9,9" ]] || false
}

@test "keyless diff delete+add against working" {
    dolt sql <<SQL
DELETE FROM keyless WHERE c0 = 2;
INSERT INTO keyless VALUES (2,2)
SQL
    run dolt diff
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  +  | 2  | 2  |" ]] || false
    [[ "$lines[@]" = "|  -  | 2  | 2  |" ]] || false
}

# row gets deleted from the middle and added to the end
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

    run dolt merge right
    [ $status -eq 0 ]
    run dolt diff master
    [ $status -eq 0 ]
    [[ "$lines[@]" = "|  -  | 2  | 2  |" ]] || false
    [[ "$lines[@]" = "|  +  | 2  | 2  |" ]] || false
}

