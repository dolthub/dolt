#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "merge non-existant branch errors" {
    run dolt merge batmans-parents
    [ $status -eq 1 ]
    [[ "$output" =~ "unknown branch" ]] || false
    [[ ! "$output" =~ "panic" ]] || false
}

@test "two branches modify different cell different row. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b change-cell-one
    dolt sql -q "replace into test values (0, 11, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "changed pk=0 c1 to 11"

    dolt checkout master
    dolt checkout -b change-cell-two
    dolt sql -q "replace into test values (1, 1, 1, 1, 1, 11)"
    dolt add test
    dolt commit -m "changed pk=1 c5 to 11"

    run dolt merge change-cell-one
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ "$output" =~ "1 rows modified" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
    run dolt status
    [[ "$output" =~ "All conflicts fixed" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
}

@test "two branches modify different cell same row. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b change-cell-one
    dolt sql -q "replace into test values (0, 11, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "changed pk=0 c1 to 11"

    dolt checkout master
    dolt checkout -b change-cell-two
    dolt sql -q "replace into test values (0, 0, 0, 0, 0, 11)"
    dolt add test
    dolt commit -m "changed pk=0 c5 to 11"

    run dolt merge change-cell-one
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ "$output" =~ "1 rows modified" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
    run dolt status
    [[ "$output" =~ "All conflicts fixed" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
}

@test "two branches modify same cell. merge. conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b change-cell-one
    dolt sql -q "replace into test values (0, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "changed pk=0 all cells to 1"

    dolt checkout master
    dolt checkout -b change-cell-two
    dolt sql -q "replace into test values (0, 11, 11, 11, 11, 11)"
    dolt add test
    dolt commit -m "changed pk=0 all cells to 11"

    run dolt merge change-cell-one
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
    run dolt status
    [[ "$output" =~ "You have unmerged tables." ]] || false
    [[ "$output" =~ "Unmerged paths:" ]] || false
}

@test "two branches add a different row. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b add-row-one
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "added pk=0 row"

    dolt checkout master
    dolt checkout -b add-row-two
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "added pk=1 row"

    run dolt merge add-row-one
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ "$output" =~ "1 rows added" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches add same row. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b add-row-one
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "added pk=0 row"

    dolt checkout master
    dolt checkout -b add-row-two
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "added pk=0 row"

    run dolt merge add-row-one
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "one branch add table, other modifies table. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b add-table-one
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "added row"

    dolt checkout master
    dolt checkout -b add-table-two
    dolt sql <<SQL
CREATE TABLE test2 (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test2
    dolt commit -m "added new table test2"

    run dolt merge add-table-one
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    skip "should have a merge summary section that says 1 table changed"
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches add same column. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b add-column-one
    dolt sql -q "alter table test add c0 bigint"
    dolt add test
    dolt commit -m "added column c0"

    dolt checkout master
    dolt checkout -b add-column-two
    dolt sql -q "alter table test add c0 bigint"
    dolt add test
    dolt commit -m "added same column c0"

    run dolt merge add-column-one
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches add different column. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b add-column-one
    dolt sql -q "alter table test add c0 bigint"
    dolt add test
    dolt commit -m "added column c0"

    dolt checkout master
    dolt checkout -b add-column-two
    dolt sql -q "alter table test add c6 bigint"
    dolt add test
    dolt commit -m "added column c6"

    run dolt merge add-column-one
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches add same column, different types. merge. conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b add-column-one
    dolt sql -q "alter table test add c0 longtext"
    dolt add test
    dolt commit -m "added column c0 as string"

    dolt checkout master
    dolt checkout -b add-column-two
    dolt sql -q "alter table test add c0 bigint"
    dolt add test
    dolt commit -m "added column c0 as int"

    run dolt merge add-column-one
    [ $status -eq 0 ]
    skip "This created two c0 columns with different types and tag numbers. Bug I think."
    [[ "$output" =~ "CONFLICT" ]] || false
}

@test "two branches delete same column. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b delete-column-one
    dolt sql -q "alter table test drop column c5"
    dolt add test
    dolt commit -m "deleted c5 column"

    dolt checkout master
    dolt checkout -b delete-column-two
    dolt sql -q "alter table test drop column c5"
    dolt add test
    dolt commit -m "deleted c5 again"

    run dolt merge delete-column-one
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches delete different column. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b delete-column-one
    dolt sql -q "alter table test drop column c5"
    dolt add test
    dolt commit -m "deleted column c5"

    dolt checkout master
    dolt checkout -b delete-column-two
    dolt sql -q "alter table test drop column c4"
    dolt add test
    dolt commit -m "deleted column c4"

    run dolt merge delete-column-one
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false

    run dolt schema show
    [[ "${lines[0]}" =~ "test @ working" ]] || false
    [[ "${lines[1]}" =~ "CREATE TABLE \`test\` (" ]] || false
    [[ "${lines[2]}" =~ "\`pk\` BIGINT NOT NULL COMMENT 'tag:0'," ]] || false
    [[ "${lines[3]}" =~ "\`c1\` BIGINT COMMENT 'tag:1'," ]] || false
    [[ "${lines[4]}" =~ "\`c2\` BIGINT COMMENT 'tag:2'," ]] || false
    [[ "${lines[5]}" =~ "\`c3\` BIGINT COMMENT 'tag:3'," ]] || false
    [[ "${lines[6]}" =~ "PRIMARY KEY (\`pk\`)" ]] || false
    [[ "${lines[7]}" =~ ");" ]] || false
}

@test "two branches rename same column to same name. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b rename-column-one
    dolt sql -q "alter table test rename column c5 to c0"
    dolt add test
    dolt commit -m "renamed c5 to c0"

    dolt checkout master
    dolt checkout -b rename-column-two
    dolt sql -q "alter table test rename column c5 to c0"
    dolt add test
    dolt commit -m "renamed c5 to c0 again"

    run dolt merge rename-column-one
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches rename same column to different name. merge. conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    skip "This currently is a failed merge. I think it should be a conflict that you can resolve by modifying the schema. Basically choose a column name for the tag. The data is the same."
    dolt add test
    dolt commit -m "table created"
    dolt branch rename-column
    dolt sql -q "alter table test rename column c5 to c0"
    dolt add test
    dolt commit -m "renamed c5 to c0"
    dolt checkout rename-column
    dolt sql -q "alter table test rename column c5 to c6"
    dolt add test
    dolt commit -m "renamed c5 to c6"
    dolt checkout master
    run dolt merge rename-column
    [ $status -eq 1 ]
    [[ "$output" =~ "Bad merge" ]] || false
    [ $status -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
}

@test "two branches rename different column to same name. merge. conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    skip "Same as test above. This case needs some thought. My initial instinct was that this generates a tag conflict. Pick one tag and then you have a data conflict because the schemas are the same on both branches."
    dolt add test
    dolt commit -m "table created"
    dolt branch rename-column
    dolt sql -q "alter table test rename column c5 to c0"
    dolt add test
    dolt commit -m "renamed c5 to c0"
    dolt checkout rename-column
    dolt sql -q "alter table test rename column c4 to c0"
    dolt add test
    dolt commit -m "renamed c5 to c6"
    dolt checkout master
    run dolt merge rename-column
    [ $status -eq 1 ]
    [[ "$output" =~ "Bad merge" ]] || false
    [ $status -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
}

# Altering types and properties of the schema are not really supported by the
# command line. Have to upload schema files for these next few tests.
@test "two branches change type of same column to same type. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b change-types-one
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT UNSIGNED COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "changed c1 to type uint"

    dolt checkout master
    dolt checkout -b change-types-two
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT UNSIGNED COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "changed c1 to type uint again"

    run dolt merge change-types-one
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches change type of same column to different type. merge. conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b change-types-one
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT UNSIGNED COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "changed c1 to type uint"

    dolt checkout master
    dolt checkout -b change-types-two
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 DOUBLE COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    skip "I think changing a type to two different types should throw a conflict"
    dolt add test
    dolt commit -m "changed c1 to type float"

    run dolt merge change-types-one
    [ $status -eq 1 ]
    [[ "$output" =~ "Bad merge" ]] || false
    [ $status -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
}

@test "two branches make same column primary key. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b add-pk-one
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk,c1)
);
SQL
    dolt add test
    dolt commit -m "made c1 a pk"

    dolt checkout master
    dolt checkout -b add-pk-two
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk,c1)
);
SQL
    dolt add test
    dolt commit -m "made c1 a pk again"

    run dolt merge add-pk-one
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches add same primary key column. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b add-pk-one
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  pk1 BIGINT NOT NULL COMMENT 'tag:6',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk,pk1)
);
SQL
    dolt add test
    dolt commit -m "added pk pk1"

    dolt checkout master
    dolt checkout -b add-pk-two
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  pk1 BIGINT NOT NULL COMMENT 'tag:6',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk,pk1)
);
SQL
    dolt add test
    dolt commit -m "added pk pk1 again"

    run dolt merge add-pk-one
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "two branches make different columns primary key. merge. conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b add-pk-one
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  pk1 BIGINT NOT NULL COMMENT 'tag:6',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk,pk1)
);
SQL
    dolt add test
    dolt commit -m "added pk pk1"

    dolt checkout master
    dolt checkout -b add-pk-two
    dolt table rm test
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  pk2 BIGINT NOT NULL COMMENT 'tag:7',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk,pk2)
);
SQL
    dolt add test
    dolt commit -m "added pk pk2"

    dolt merge add-pk-one
    run dolt merge add-pk-one
    [ $status -eq 0 ]
   skip "This merges fine right now. Should throw conflict."
    [[ "$output" =~ "CONFLICT" ]] || false
}

@test "two branches both create different tables. merge. no conflict" {
    dolt branch table1
    dolt branch table2
    dolt checkout table1
    dolt sql <<SQL
CREATE TABLE table1 (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
    dolt add table1
    dolt commit -m "first table"
    dolt checkout table2
    dolt sql <<SQL
CREATE TABLE table2 (
  pk1 BIGINT NOT NULL COMMENT 'tag:0',
  pk2 BIGINT NOT NULL COMMENT 'tag:1',
  c1 BIGINT COMMENT 'tag:2',
  c2 BIGINT COMMENT 'tag:3',
  c3 BIGINT COMMENT 'tag:4',
  c4 BIGINT COMMENT 'tag:5',
  c5 BIGINT COMMENT 'tag:6',
  PRIMARY KEY (pk1,pk2)
);
SQL
    dolt add table2
    dolt commit -m "second table"
    dolt checkout master
    run dolt merge table1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]] || false
    run dolt merge table2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}