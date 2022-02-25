#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

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
    assert_feature_version
    teardown_common
}

@test "keyless: feature gate add/drop column" {
    run dolt sql -q "ALTER TABLE keyless DROP COLUMN c0;"
    [ $status -ne 0 ]
    [[ ! "$output" =~ "panic" ]] || false

    run dolt sql -q "ALTER TABLE keyless ADD COLUMN c2 int;"
    [ $status -ne 0 ]
    [[ ! "$output" =~ "panic" ]] || false
}

@test "keyless: feature indexes and foreign keys" {
    run dolt sql -q "ALTER TABLE keyless ADD INDEX (c1);"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "panic" ]] || false

    run dolt sql -q "CREATE TABLE fine (a int, b int, INDEX (b));"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "panic" ]] || false

    run dolt sql -q "CREATE TABLE worse (a int, b int, FOREIGN KEY (b) REFERENCES keyless(c1));"
    [ $status -eq 0 ]
    [[ ! "$output" =~ "panic" ]] || false
}

@test "keyless: create keyless table" {
    # created in setup()

    run dolt ls
    [ $status -eq 0 ]
    [[ "$output" =~ "keyless" ]] || false

    dolt sql -q "SHOW CREATE TABLE keyless;"
    run dolt sql -q "SHOW CREATE TABLE keyless;"
    [ $status -eq 0 ]
    [[ "$output" =~ "CREATE TABLE \`keyless\` (" ]] || false
    [[ "$output" =~ "\`c0\` int," ]] || false
    [[ "$output" =~ "\`c1\` int" ]] || false
    [[ "$output" =~ ")" ]] || false

    dolt sql -q "SELECT sum(c0),sum(c1) FROM keyless;" -r csv
    run dolt sql -q "SELECT sum(c0),sum(c1) FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" =~ "4,4" ]] || false
}

@test "keyless: delete from keyless" {
    run dolt sql -q "DELETE FROM keyless WHERE c0 = 2;"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM keyless ORDER BY c0;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "0,0" ]] || false
    [[ "${lines[2]}" = "1,1" ]] || false
    [[ "${lines[3]}" = "1,1" ]] || false
}

@test "keyless: update keyless" {
    run dolt sql -q "UPDATE keyless SET c0 = 9 WHERE c0 = 2;"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM keyless ORDER BY c0;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "0,0" ]] || false
    [[ "${lines[2]}" = "1,1" ]] || false
    [[ "${lines[3]}" = "1,1" ]] || false
    [[ "${lines[4]}" = "9,2" ]] || false
}

@test "keyless: column add/drop" {
    skip "unimplemented"
    run dolt sql <<SQL
ALTER TABLE keyless ADD COLUMN c2 int;
ALTER TABLE keyless DROP COLUMN c0;
SQL
    [ $status -eq 0 ]

    dolt sql -q "SELECT * FROM keyless ORDER BY c1;" -r csv
    run dolt sql -q "SELECT * FROM keyless ORDER BY c1;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "c1,c2" ]] || false
    [[ "${lines[1]}" = "0," ]] || false
    [[ "${lines[2]}" = "1," ]] || false
    [[ "${lines[3]}" = "1," ]] || false
    [[ "${lines[4]}" = "2," ]] || false
}

# keyless tables allow duplicate rows
@test "keyless: table import" {
    cat <<CSV > data.csv
c0,c1
0,0
2,2
1,1
1,1
,9
CSV
    dolt table import -c imported data.csv
    run dolt sql -q "SELECT count(*) FROM imported;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "5" ]] || false
    run dolt sql -q "SELECT c0,c1 FROM imported ORDER BY c1;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "0,0" ]] || false
    [[ "${lines[2]}" = "1,1" ]] || false
    [[ "${lines[3]}" = "1,1" ]] || false
    [[ "${lines[4]}" = "2,2" ]] || false
    [[ "${lines[5]}" = ",9" ]] || false

    # tests for NULL hashing in keyless tables
    dolt sql -q "UPDATE imported SET c1 = c1 + 10"
    run dolt sql -q "SELECT c0,c1 FROM imported ORDER BY c1;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "0,10" ]] || false
    [[ "${lines[2]}" = "1,11" ]] || false
    [[ "${lines[3]}" = "1,11" ]] || false
    [[ "${lines[4]}" = "2,12" ]] || false
    [[ "${lines[5]}" = ",19" ]] || false
}

# updates are always appends
@test "keyless: table update" {
    cat <<CSV > data.csv
c0,c1
0,0
2,2
1,1
1,1
CSV
    dolt table import -u keyless data.csv
    dolt sql -q "SELECT * FROM keyless ORDER BY c0;" -r csv
    run dolt sql -q "SELECT count(*) FROM keyless;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "8" ]] || false
    dolt sql -q "SELECT * FROM keyless ORDER BY c0;" -r csv
    run dolt sql -q "SELECT * FROM keyless ORDER BY c0;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "0,0" ]] || false
    [[ "${lines[2]}" = "0,0" ]] || false
    [[ "${lines[3]}" = "1,1" ]] || false
    [[ "${lines[4]}" = "1,1" ]] || false
    [[ "${lines[5]}" = "1,1" ]] || false
    [[ "${lines[6]}" = "1,1" ]] || false
    [[ "${lines[7]}" = "2,2" ]] || false
    [[ "${lines[8]}" = "2,2" ]] || false
}

@test "keyless: table export CSV" {
    dolt table export keyless
    run dolt table export keyless
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "c0,c1" ]] || false
    [[ "${lines[1]}" = "1,1" ]] || false
    [[ "${lines[2]}" = "1,1" ]] || false
    [[ "${lines[3]}" = "0,0" ]] || false
    [[ "${lines[4]}" = "2,2" ]] || false
}

@test "keyless: table export SQL" {
    dolt table export keyless export.sql
    cat export.sql
    run cat export.sql
    [[ "${lines[0]}" = "DROP TABLE IF EXISTS \`keyless\`;"   ]] || false
    [[ "${lines[1]}" = "CREATE TABLE \`keyless\` ("          ]] || false
    [[ "${lines[2]}" = "  \`c0\` int,"  ]] || false
    [[ "${lines[3]}" = "  \`c1\` int"   ]] || false
    [[ "${lines[4]}" = ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"     ]] || false
    [[ "${lines[5]}" = "INSERT INTO \`keyless\` (\`c0\`,\`c1\`) VALUES (1,1);" ]] || false
    [[ "${lines[6]}" = "INSERT INTO \`keyless\` (\`c0\`,\`c1\`) VALUES (1,1);" ]] || false
    [[ "${lines[7]}" = "INSERT INTO \`keyless\` (\`c0\`,\`c1\`) VALUES (0,0);" ]] || false
    [[ "${lines[8]}" = "INSERT INTO \`keyless\` (\`c0\`,\`c1\`) VALUES (2,2);" ]] || false

}

@test "keyless: diff against working set" {
    dolt sql <<SQL
DELETE FROM keyless WHERE c0 = 0;
INSERT INTO keyless VALUES (8,8);
UPDATE keyless SET c1 = 9 WHERE c0 = 1;
SQL
    run dolt diff
    [ $status -eq 0 ]
    # output order is random
    [[ "${lines[6]}"  =~ "|  +  | 8  | 8  |" ]] || false
    [[ "${lines[7]}"  =~ "|  -  | 1  | 1  |" ]] || false
    [[ "${lines[8]}"  =~ "|  -  | 1  | 1  |" ]] || false
    [[ "${lines[9]}"  =~ "|  +  | 1  | 9  |" ]] || false
    [[ "${lines[10]}" =~ "|  +  | 1  | 9  |" ]] || false
    [[ "${lines[11]}" =~ "|  -  | 0  | 0  |" ]] || false
}

@test "keyless: diff --summary" {
    dolt sql <<SQL
DELETE FROM keyless WHERE c0 = 0;
INSERT INTO keyless VALUES (8,8);
UPDATE keyless SET c1 = 9 WHERE c0 = 1;
SQL
    run dolt diff --summary
    [ $status -eq 0 ]
    [[ "$output" =~ "3 Rows Added" ]] || false
    [[ "$output" =~ "3 Rows Deleted" ]] || false
}

@test "keyless: dolt_diff_ table" {
    dolt sql <<SQL
DELETE FROM keyless WHERE c0 = 0;
INSERT INTO keyless VALUES (8,8);
UPDATE keyless SET c1 = 9 WHERE c0 = 1;
SQL
    run dolt sql -q "
        SELECT to_c0, to_c1, from_c0, from_c1
        FROM dolt_diff_keyless
        ORDER BY to_commit_date" -r csv
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 11 ]
    [[ "${lines[0]}"  = "to_c0,to_c1,from_c0,from_c1"  ]] || false
    [[ "${lines[1]}"  = "8,8,,"  ]] || false
    [[ "${lines[2]}"  = ",,1,1"  ]] || false
    [[ "${lines[3]}"  = ",,1,1"  ]] || false
    [[ "${lines[4]}"  = "1,9,,"  ]] || false
    [[ "${lines[5]}"  = "1,9,,"  ]] || false
    [[ "${lines[6]}"  = ",,0,0"  ]] || false
    [[ "${lines[7]}"  = "1,1,,"  ]] || false
    [[ "${lines[8]}"  = "1,1,,"  ]] || false
    [[ "${lines[9]}"  = "0,0,,"  ]] || false
    [[ "${lines[10]}" = "2,2,,"  ]] || false
}

@test "keyless: diff column add/drop" {
    skip "unimplemented"
    run dolt sql <<SQL
ALTER TABLE keyless ADD COLUMN c2 int;
ALTER TABLE keyless DROP COLUMN c0;
SQL
    [ $status -eq 0 ]

    dolt diff
    run dolt diff
    [ $status -eq 0 ]
    [[ "${lines[3]}"  =~ "CREATE TABLE keyless (" ]] || false
    [[ "${lines[4]}"  =~ "-   \`c0\` INT"         ]] || false
    [[ "${lines[5]}"  =~ "    \`c1\` INT"         ]] || false
    [[ "${lines[6]}"  =~ "+   \`c2\` INT"         ]] || false
    [[ "${lines[7]}"  =~ "     PRIMARY KEY ()"    ]] || false
    [[ "${lines[8]}"  =~ ");"                     ]] || false

    [[ "${lines[10]}" =~ "|  <  | c1 |    | c0 |" ]] || false
    [[ "${lines[11]}" =~ "|  >  | c1 | c2 |    |" ]] || false
}

@test "keyless: merge fast-forward" {
    dolt checkout -b other
    dolt sql -q "INSERT INTO keyless VALUES (9,9);"
    dolt commit -am "9,9"
    dolt checkout main
    run dolt merge other
    [ $status -eq 0 ]
    run dolt sql -q "SELECT * FROM keyless WHERE c0 > 6;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "9,9" ]] || false
}

@test "keyless: diff branches with identical mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on main"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on other"

    dolt diff main
    run dolt diff main
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

@test "keyless: merge branches with identical mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on main"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on other"

    run dolt merge main
    [ $status -eq 0 ]
    run dolt sql -q "SELECT * FROM keyless WHERE c0 > 6 ORDER BY c0;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "7,7" ]] || false
    [[ "${lines[2]}" = "8,8" ]] || false
    [[ "${lines[3]}" = "9,9" ]] || false
}

@test "keyless: diff deletes from two branches" {
    dolt branch left
    dolt checkout -b right

    dolt sql -q "DELETE FROM keyless WHERE c0 = 0;"
    dolt commit -am "deleted ones on right"

    run dolt diff main
    [ $status -eq 0 ]
    [[ "$output" =~ "|  -  | 0  | 0  |" ]] || false

    dolt checkout left
    dolt sql -q "DELETE FROM keyless WHERE c0 = 2;"
    dolt commit -am "deleted twos on left"

    run dolt diff main
    [ $status -eq 0 ]
    [[ "$output" =~ "|  -  | 2  | 2  |" ]] || false
}

@test "keyless: merge deletes from two branches" {
    dolt branch left
    dolt checkout -b right

    dolt sql -q "DELETE FROM keyless WHERE c0 = 0;"
    dolt commit -am "deleted ones on right"

    dolt checkout left
    dolt sql -q "DELETE FROM keyless WHERE c0 = 2;"
    dolt commit -am "deleted twos on left"

    run dolt merge right
    [ $status -eq 0 ]
    run dolt diff main
    [ $status -eq 0 ]
    [[ "$output" =~ "|  -  | 0  | 0  |" ]] || false
    [[ "$output" =~ "|  -  | 2  | 2  |" ]] || false
}

function make_dupe_table() {
    dolt sql <<SQL
CREATE TABLE dupe (
    c0 int,
    c1 int
);
INSERT INTO dupe (c0,c1) VALUES
    (1,1),(1,1),(1,1),(1,1),(1,1),
    (1,1),(1,1),(1,1),(1,1),(1,1);
SQL
    dolt commit -am "created table dupe"
}

@test "keyless: diff duplicate deletes" {
    make_dupe_table

    dolt branch left
    dolt checkout -b right

    dolt sql -q "DELETE FROM dupe LIMIT 2;"
    dolt commit -am "deleted two rows on right"

    dolt diff main
    run dolt diff main
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 9 ] # 2 diffs + 6 header + 1 footer
    [[ "${lines[6]}" =~ "|  -  | 1  | 1  |" ]] || false
    [[ "${lines[7]}" =~ "|  -  | 1  | 1  |" ]] || false

    dolt checkout left
    dolt sql -q "DELETE FROM dupe LIMIT 4;"
    dolt commit -am "deleted four rows on left"

    run dolt diff main
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 11 ] # 4 diffs + 6 header + 1 footer
    [[ "${lines[6]}" = "|  -  | 1  | 1  |" ]] || false
    [[ "${lines[7]}" = "|  -  | 1  | 1  |" ]] || false
    [[ "${lines[8]}" = "|  -  | 1  | 1  |" ]] || false
    [[ "${lines[9]}" = "|  -  | 1  | 1  |" ]] || false

}

@test "keyless: merge duplicate deletes" {
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
    [[ "$output" =~ "CONFLICT" ]] || false

    run dolt conflicts resolve --ours dupe
    [ $status -eq 0 ]
    dolt commit -am "resolved"
    run dolt sql -q "select sum(c0), sum(c1) from dupe" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "6,6" ]] || false
}

@test "keyless: diff duplicate updates" {
    make_dupe_table

    dolt branch left
    dolt checkout -b right

    dolt sql -q "UPDATE dupe SET c1 = 2 LIMIT 2;"
    dolt commit -am "updated two rows on right"

    run dolt diff main
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 11 ] # 4 diffs + 6 header + 1 footer

    dolt checkout left
    dolt sql -q "UPDATE dupe SET c1 = 2 LIMIT 4;"
    dolt commit -am "updated four rows on left"

    run dolt diff main
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 15 ] # 8 diffs + 6 header + 1 footer
}

@test "keyless: merge duplicate updates" {
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
    [[ "$output" =~ "CONFLICT" ]] || false

    run dolt conflicts resolve --theirs dupe
    [ $status -eq 0 ]
    dolt commit -am "resolved"
    run dolt sql -q "select sum(c0), sum(c1) from dupe" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "10,12" ]] || false
}

@test "keyless: sql diff" {
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

@test "keyless: sql diff as a patch" {
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

@test "keyless: table replace" {
    cat <<CSV > data.csv
c0,c1
0,0
2,2
1,1
1,1
CSV
    run dolt table import -r keyless data.csv
    [ $status -eq 0 ]
    dolt diff
    run dolt diff
    [ $status -eq 0 ]
    [ "$output" = "" ]

    cat <<CSV > data2.csv
c0,c1
9,9
0,0
1,1
1,1
2,2
CSV
    run dolt table import -r keyless data2.csv
    [ $status -eq 0 ]
    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "|  +  | 9  | 9  |" ]] || false
}

# in-place updates create become drop/add
@test "keyless: diff with in-place updates (working set)" {
    dolt sql -q "UPDATE keyless SET c1 = 9 where c0 = 2;"
    run dolt diff
    [ $status -eq 0 ]
    [[ "$output" =~ "|  -  | 2  | 2  |" ]] || false
    [[ "$output" =~ "|  +  | 2  | 9  |" ]] || false
}

# in-place updates create become drop/add
@test "keyless: sql diff with in-place updates (working set)" {
    skip "unimplemented"
    dolt sql -q "UPDATE keyless SET c1 = 9 where c0 = 2;"
    run dolt diff -r sql
    [ $status -eq 0 ]
    [[ "$lines[@]" = "DELETE FROM keyless WHERE c0 = 2 AND c1 = 2 LIMIT 1" ]] || false
    [[ "$lines[@]" = "INSERT INTO keyless (c0,c1) VALUES (2,9);" ]] || false
}

# update patch always recreates identical branches
@test "keyless: updates as a sql diff patch" {
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
@test "keyless: diff with in-place updates (branches)" {
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "added rows"
    dolt branch other

    dolt sql -q "UPDATE keyless SET c1 = c1+10 WHERE c0 > 6"
    dolt commit -am "updated on main"

    dolt checkout other
    dolt sql -q "UPDATE keyless SET c1 = c1+20 WHERE c0 > 6"
    dolt commit -am "updated on other"

    dolt diff main
    run dolt diff main
    [ $status -eq 0 ]
    [[ "$output" =~ "|  -  | 7  | 17 |" ]] || false
    [[ "$output" =~ "|  +  | 7  | 27 |" ]] || false
    [[ "$output" =~ "|  -  | 9  | 19 |" ]] || false
    [[ "$output" =~ "|  +  | 9  | 29 |" ]] || false
    [[ "$output" =~ "|  -  | 8  | 18 |" ]] || false
    [[ "$output" =~ "|  +  | 8  | 28 |" ]] || false
}

@test "keyless: merge with in-place updates (branches)" {
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "added rows"
    dolt branch other

    dolt sql -q "UPDATE keyless SET c1 = c1+10 WHERE c0 > 6"
    dolt commit -am "updated on main"

    dolt checkout other
    dolt sql -q "UPDATE keyless SET c1 = c1+20 WHERE c0 > 6"
    dolt commit -am "updated on other"

    run dolt merge main
    [ $status -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false

    dolt conflicts resolve --ours keyless
    run dolt conflicts resolve --ours keyless
    [ $status -eq 0 ]
    dolt commit -am "resolved"

    skip "incorrect resolve"
    # updates become delete+add
    # conflict is generated for delete
    # the corresponding add does not conflict
    # on resolve, we get both sets of adds

    run dolt sql -q "select * from keyless where c0 > 6 order by c0" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "7,17" ]] || false
    [[ "${lines[1]}" = "8,18" ]] || false
    [[ "${lines[1]}" = "9,19" ]] || false
}

@test "keyless: diff branches with reordered mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on main"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (9,9),(8,8),(7,7);"
    dolt commit -am "inserted on other"

    run dolt diff main
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

@test "keyless: merge branches with reordered mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on main"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (9,9),(8,8),(7,7);"
    dolt commit -am "inserted on other"

    run dolt merge main
    [ $status -eq 0 ]
     run dolt sql -q "SELECT count(*) FROM keyless WHERE c0 > 6;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "3" ]] || false
    run dolt sql -q "SELECT * FROM keyless WHERE c0 > 6 ORDER BY c0;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "7,7" ]] || false
    [[ "${lines[2]}" = "8,8" ]] || false
    [[ "${lines[3]}" = "9,9" ]] || false
}

@test "keyless: diff branches with convergent mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on main"

    dolt checkout other
    dolt sql <<SQL
INSERT INTO keyless VALUES (9,19),(8,8),(7,17);
UPDATE keyless SET c0 = 7, c1 = 7 WHERE c1 = 19;
UPDATE keyless SET c0 = 9, c1 = 9 WHERE c1 = 17;
SQL
    dolt commit -am "inserted on other"

    dolt diff main
    run dolt diff main
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

@test "keyless: merge branches with convergent mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on main"

    dolt checkout other
    dolt sql <<SQL
INSERT INTO keyless VALUES (9,19),(8,8),(7,17);
UPDATE keyless SET c0 = 7, c1 = 7 WHERE c1 = 19;
UPDATE keyless SET c0 = 9, c1 = 9 WHERE c1 = 17;
SQL
    dolt commit -am "inserted on other"

    run dolt merge main
    [ $status -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false

    run dolt conflicts resolve --theirs keyless
    [ $status -eq 0 ]
    dolt commit -am "resolved"
    run dolt sql -q "select * from keyless where c0 > 6 order by c0" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "7,7" ]] || false
    [[ "${lines[2]}" = "8,8" ]] || false
    [[ "${lines[3]}" = "9,9" ]] || false
}

@test "keyless: diff branches with offset mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on main"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(7,7),(8,8),(9,9);"
    dolt commit -am "inserted on other"

    run dolt diff main
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 8 ] # 1 diffs + 6 header + 1 footer
    [[ "${lines[6]}" =~ "|  +  | 7  | 7  |" ]] || false
}

@test "keyless: merge branches with offset mutation history" {
    dolt branch other

    dolt sql -q "INSERT INTO keyless VALUES (7,7),(8,8),(9,9);"
    dolt commit -am "inserted on main"

    dolt checkout other
    dolt sql -q "INSERT INTO keyless VALUES (7,7),(7,7),(8,8),(9,9);"
    dolt commit -am "inserted on other"

    run dolt merge main
    [ $status -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false

    run dolt conflicts resolve --ours keyless
    [ $status -eq 0 ]
    dolt commit -am "resolved"
    run dolt sql -q "select * from keyless where c0 > 6 order by c0" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "7,7" ]] || false
    [[ "${lines[2]}" = "7,7" ]] || false
    [[ "${lines[3]}" = "8,8" ]] || false
    [[ "${lines[4]}" = "9,9" ]] || false
}

@test "keyless: diff delete+add against working" {
    dolt sql <<SQL
DELETE FROM keyless WHERE c0 = 2;
INSERT INTO keyless VALUES (2,2)
SQL
    run dolt diff
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

@test "keyless: diff delete+add on two branches" {
    dolt branch left
    dolt checkout -b right

    dolt sql -q "DELETE FROM keyless WHERE c0 = 2;"
    dolt commit -am "deleted ones on right"

    run dolt diff main
    [ $status -eq 0 ]
    [[ "${lines[6]}" = "|  -  | 2  | 2  |" ]] || false

    dolt checkout left
    dolt sql -q "INSERT INTO keyless VALUES (2,2);"
    dolt commit -am "deleted twos on left"

    run dolt diff main
    [ $status -eq 0 ]
    [[ "${lines[6]}" = "|  +  | 2  | 2  |" ]] || false
}

@test "keyless: merge delete+add on two branches" {
    dolt branch left
    dolt checkout -b right

    dolt sql -q "DELETE FROM keyless WHERE c0 = 2;"
    dolt commit -am "deleted twos on right"

    dolt checkout left
    dolt sql -q "INSERT INTO keyless VALUES (2,2);"
    dolt commit -am "inserted twos on left"

    run dolt merge right
    [ $status -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false

    run dolt conflicts resolve --theirs keyless
    [ $status -eq 0 ]
    dolt commit -am "resolved"
    run dolt sql -q "select * from keyless order by c0" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "0,0" ]] || false
    [[ "${lines[2]}" = "1,1" ]] || false
    [[ "${lines[3]}" = "1,1" ]] || false
    [ "${#lines[@]}" -eq 4 ]
}

@test "keyless: create secondary index" {
    dolt sql -q "create index idx on keyless (c1)"

    run dolt sql -q "show index from keyless" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "Table,Non_unique,Key_name,Seq_in_index,Column_name,Collation,Cardinality,Sub_part,Packed,Null,Index_type,Comment,Index_comment,Visible,Expression" ]] || false
    [[ "${lines[1]}" = "keyless,1,idx,1,c1,,0,,,YES,BTREE,\"\",\"\",YES," ]] || false

    run dolt sql -q "select * from keyless where c1 > 0 order by c0" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "c0,c1" ]] || false
    [[ "${lines[1]}" = "1,1" ]] || false
    [[ "${lines[2]}" = "1,1" ]] || false
    [[ "${lines[3]}" = "2,2" ]] || false
    [ "${#lines[@]}" -eq 4 ]

    run dolt sql -q "select c0 from keyless where c1 = 1" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "c0" ]] || false
    [[ "${lines[1]}" = "1" ]] || false
    [[ "${lines[2]}" = "1" ]] || false
    [ "${#lines[@]}" -eq 3 ]
}

@test "keyless: secondary index insert" {
    dolt sql -q "create index idx on keyless (c1)"

    dolt sql -q "insert into keyless values (3,3), (4,4)"

    run dolt sql -q "select c0 from keyless where c1 = 4" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "c0" ]] || false
    [[ "${lines[1]}" = "4" ]] || false
    [ "${#lines[@]}" -eq 2 ]
}

@test "keyless: secondary index duplicate insert" {
    dolt sql -q "create index idx on keyless (c1)"

    dolt sql -q "insert into keyless values (3,3), (4,4), (4,4)"

    run dolt sql -q "select c0 from keyless where c1 = 4" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "c0" ]] || false
    [[ "${lines[1]}" = "4" ]] || false
    [[ "${lines[2]}" = "4" ]] || false
    [ "${#lines[@]}" -eq 3 ]
}

@test "keyless: secondary index update" {
    dolt sql -q "create index idx on keyless (c1)"

    dolt sql -q "update keyless set c0 = c0 + 1"

    run dolt sql -q "select * from keyless order by c0" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "1,0" ]] || false
    [[ "${lines[2]}" = "2,1" ]] || false
    [[ "${lines[3]}" = "2,1" ]] || false
    [[ "${lines[4]}" = "3,2" ]] || false
    [ "${#lines[@]}" -eq 5 ]

    run dolt sql -q "select c0 from keyless where c1 = 1" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "c0" ]] || false
    [[ "${lines[1]}" = "2" ]] || false
    [[ "${lines[1]}" = "2" ]] || false
    [ "${#lines[@]}" -eq 3 ]
}

@test "keyless: secondary index delete single" {
    dolt sql -q "create index idx on keyless (c1)"

    dolt sql -q "insert into keyless values (3,3), (4,4)"
    dolt sql -q "delete from keyless where c0 = 4"

    run dolt sql -q "select c0 from keyless where c1 > 2" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "c0" ]] || false
    [[ "${lines[1]}" = "3" ]] || false
    [ "${#lines[@]}" -eq 2 ]

    run dolt sql -q "select c0 from keyless where c0 > 2" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "c0" ]] || false
    [[ "${lines[1]}" = "3" ]] || false
    [ "${#lines[@]}" -eq 2 ]
}

@test "keyless: secondary index delete duplicate" {
    dolt sql -q "create index idx on keyless (c1)"

    dolt sql -q "insert into keyless values (3,3), (4,4), (4,4)"
    dolt sql -q "delete from keyless where c0 = 4"

    run dolt sql -q "select c0 from keyless where c1 > 2" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "c0" ]] || false
    [[ "${lines[1]}" = "3" ]] || false
    [ "${#lines[@]}" -eq 2 ]

    run dolt sql -q "select c0 from keyless where c0 > 2" -r csv
    [ $status -eq 0 ]
    [[ "${lines[0]}" = "c0" ]] || false
    [[ "${lines[1]}" = "3" ]] || false
    [ "${#lines[@]}" -eq 2 ]
}
