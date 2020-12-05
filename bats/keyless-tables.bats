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
    [[ "$output" = "keyless" ]] || false

    run dolt sql -q "SHOW CREATE TABLE keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "CREATE TABLE \`keyless\` (" ]] || false
    [[ "$output" = "\`c1\` int," ]] || false
    [[ "$output" = "\`c2\` int" ]] || false
    [[ "$output" = ")" ]] || false

    run dolt sql -q "SELECT count(*) FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "4" ]] || false
}

@test "keyless tables maintain insert order" {
    run dolt sql -q "SELECT * FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "0,0" ]] || false
    [[ "$output" = "2,2" ]] || false
    [[ "$output" = "1,1" ]] || false
    [[ "$output" = "1,1" ]] || false
}

@test "delete from keyless" {
    run dolt sql -q "DELETE FROM keyless WHERE c1 = 2;"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "0,0" ]] || false
    [[ "$output" = "1,1" ]] || false
    [[ "$output" = "1,1" ]] || false
}

@test "update keyless" {
    run dolt sql -q "UPDATE keyless SET c0 = 9 WHERE c0 = 2;"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "0,0" ]] || false
    [[ "$output" = "9,2" ]] || false
    [[ "$output" = "1,1" ]] || false
    [[ "$output" = "1,1" ]] || false
}

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
    [[ "$output" = "4" ]] || false
    run dolt sql -q "SELECT * FROM tbl;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "0,0" ]] || false
    [[ "$output" = "2,2" ]] || false
    [[ "$output" = "1,1" ]] || false
    [[ "$output" = "1,1" ]] || false
}

@test "keyless table update" {
    cat <<CSV > data.csv
c0,c1
0,0
2,2
1,1
1,1
CSV
    # update is a pure append
    dolt table import -u keyless data.csv
    run dolt sql -q "SELECT count(*) FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "8" ]] || false
    run dolt sql -q "SELECT * FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "0,0" ]] || false
    [[ "$output" = "2,2" ]] || false
    [[ "$output" = "1,1" ]] || false
    [[ "$output" = "1,1" ]] || false
    [[ "$output" = "0,0" ]] || false
    [[ "$output" = "2,2" ]] || false
    [[ "$output" = "1,1" ]] || false
    [[ "$output" = "1,1" ]] || false
}

@test "keyless table replace" {
    cat <<CSV > data.csv
c0,c1
7,7
8,8
9,9
CSV
    dolt table import -r keyless data.csv
    run dolt sql -q "SELECT count(*) FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "3" ]] || false
    run dolt sql -q "SELECT * FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "7,7" ]] || false
    [[ "$output" = "8,8" ]] || false
    [[ "$output" = "9,9" ]] || false

}

@test "keyless diff against working set" {
    dolt sql -q "INSERT INTO keyless VALUES (9,9);"
    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" = "|  +  | 9  | 9  |" ]] || false
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
    [[ "$output" = "9,9" ]] || false
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
    [ "$output" = "" ]
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
    run dolt sql -q "SELECT * FROM keyless WHERE c0 > 6;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "7,7" ]] || false
    [[ "$output" = "8,8" ]] || false
    [[ "$output" = "9,9" ]] || false
}

@test "keyless diff with cell-wise updates (working set)" {
    dolt sql -q "UPDATE keyless SET c1 = 9 where c0 = 2;"
    run dolt diff
    [ $status -eq 0 ]
    # in-place updates create cell-wise diffs
    [[ "$output" = "|  <  | 2  | 2  |" ]] || false
    [[ "$output" = "|  >  | 2  | 9  |" ]] || false
}

@test "keyless diff with cell-wise updates (branches)" {
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
    # in-place updates create cell-wise diffs
    [[ "$output" = "|  <  | 7  | 27  |" ]] || false
    [[ "$output" = "|  >  | 7  | 17  |" ]] || false
    [[ "$output" = "|  <  | 8  | 28  |" ]] || false
    [[ "$output" = "|  >  | 8  | 18  |" ]] || false
    [[ "$output" = "|  <  | 9  | 29  |" ]] || false
    [[ "$output" = "|  >  | 9  | 19  |" ]] || false
}

@test "keyless merge with cell-wise updates (branches)" {
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
    [[ "$output" = "7,27" ]] || false
    [[ "$output" = "7,17" ]] || false
    [[ "$output" = "8,28" ]] || false
    [[ "$output" = "8,18" ]] || false
    [[ "$output" = "9,29" ]] || false
    [[ "$output" = "9,19" ]] || false
}

@test "keyless diff branches with reordered mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (9,9),(8,8),(7,7),;"
    dolt commit -am "inserted on other"

    run dolt diff master
    [ $status -eq 0 ]
    [[ "$output" = "|  -  | 7  | 7  |" ]] || false
    [[ "$output" = "|  +  | 9  | 9  |" ]] || false
        # row (8,8) creates no diff
    [[ "$output" = "|  -  | 9  | 9  |" ]] || false
    [[ "$output" = "|  +  | 7  | 7  |" ]] || false
}

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
    [[ "$output" = "7,7" ]] || false
    [[ "$output" = "9,9" ]] || false
    [[ "$output" = "8,8" ]] || false
    [[ "$output" = "9,9" ]] || false
    [[ "$output" = "7,7" ]] || false
}

@test "keyless diff branches with convergent mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql <<SQL
INSERT INTO keyless VALUES (9,9),(8,8),(7,7),;
UPDATE keyless SET (c0,c1) = (7,7) WHERE c0 = 9;
UPDATE keyless SET (c0,c1) = (9,9) WHERE c0 = 7;
SQL
    dolt commit -am "inserted on other"

    run dolt diff master
    [ $status -eq 0 ]
    [ $output = "" ]
}

@test "keyless merge branches with convergent mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql <<SQL
INSERT INTO keyless VALUES (9,9),(8,8),(7,7),;
UPDATE keyless SET (c0,c1) = (7,7) WHERE c0 = 9;
UPDATE keyless SET (c0,c1) = (9,9) WHERE c0 = 7;
SQL
    dolt commit -am "inserted on other"

    run dolt merge master
    [ $status -eq 0 ]
    run dolt sql -q "SELECT * FROM keyless WHERE c0 > 6;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "7,7" ]] || false
    [[ "$output" = "8,8" ]] || false
    [[ "$output" = "9,9" ]] || false
}

@test "keyless diff branches with offset mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(7,7),(8,8),(9,9);"
    dolt commit -am "inserted on other"

    run dolt diff master
    [ $status -eq 0 ]
    [[ "$output" = "|  +  | 7  | 7  |" ]] || false
    [[ "$output" = "|  -  | 8  | 8  |" ]] || false
    [[ "$output" = "|  +  | 8  | 8  |" ]] || false
    [[ "$output" = "|  -  | 9  | 9  |" ]] || false
    [[ "$output" = "|  +  | 9  | 9  |" ]] || false
}


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
    [[ "$output" = "7,7" ]] || false
    [[ "$output" = "7,7" ]] || false
    [[ "$output" = "8,8" ]] || false
    [[ "$output" = "8,8" ]] || false
    [[ "$output" = "9,9" ]] || false
    [[ "$output" = "9,9" ]] || false
}

@test "keyless diff delete on two branches" {
    dolt branch other

    dolt sql -q "DELETE FROM keyless WHERE c0 = 0"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql -q "DELETE FROM keyless WHERE c0 = 2"
    dolt commit -am "inserted on other"

    run dolt diff master
    [ $status -eq 0 ]
    [[ "$output" = "|  +  | 0  | 0  |" ]] || false
    [[ "$output" = "|  -  | 2  | 2  |" ]] || false
}

@test "keyless merge deletes from two branches" {
    dolt branch other

    dolt sql -q "DELETE FROM keyless WHERE c0 = 0"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql -q "DELETE FROM keyless WHERE c0 = 2"
    dolt commit -am "inserted on other"

    run dolt merge master
    [ $status -eq 0 ]
    run dolt sql -q "SELECT * FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "1,1" ]] || false
    [[ "$output" = "1,1" ]] || false
}

@test "keyless diff delete+add against working" {
    dolt sql <<SQL
DELETE FROM keyless WHERE c0 = 2;
INSERT INTO keyless VALUES (2,2)
SQL
    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" = "|  +  | 2  | 2  |" ]] || false
    [[ "$output" = "|  -  | 2  | 2  |" ]] || false
}

@test "keyless diff delete+add on two branches" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (2,2);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql <<SQL
DELETE FROM keyless WHERE c0 = 2;
INSERT INTO keyless VALUES (2,2)
SQL
    dolt commit -am "inserted on other"

    run dolt diff master
    [ $status -eq 0 ]
    [[ "$output" = "|  -  | 2  | 2  |" ]] || false
}

@test "keyless merge delete+add on two branches" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (2,2);"
    dolt commit -am "inserted on master"

    dolt checkout other
    dolt sql <<SQL
DELETE FROM keyless WHERE c0 = 2;
INSERT INTO keyless VALUES (2,2)
SQL
    dolt commit -am "inserted on other"

    run dolt merge master
    [ $status -eq 0 ]
    run dolt sql -q "SELECT * FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "$output" = "0,0" ]] || false
    [[ "$output" = "1,1" ]] || false
    [[ "$output" = "1,1" ]] || false
    [[ "$output" = "2,2" ]] || false
}