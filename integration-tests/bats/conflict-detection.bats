#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "conflict-detection: merge non-existant branch errors" {
    run dolt merge batmans-parents
    [ $status -eq 1 ]
    [[ "$output" =~ "branch not found" ]] || false
    [[ ! "$output" =~ "panic" ]] || false
}

@test "conflict-detection: cannot merge into dirty working table" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "table created"

    dolt checkout -b other
    dolt sql -q "replace into test values (1, 1, 1, 1, 1, 11)"
    dolt add test
    dolt commit -m "changed pk=1 c5 to 11"

    dolt checkout main
    dolt sql -q "replace into test values (0, 11, 0, 0, 0, 0)"

    run dolt merge other
    [ "$status" -ne 0 ]
    [[ "$output" =~ "error: Your local changes to the following tables would be overwritten by merge:" ]] || false
    [[ "$output" =~ "test" ]] || false
    [[ "$output" =~ "Please commit your changes before you merge." ]] || false

    dolt add test
    dolt commit -m "changes pk=0 c1 t0 11"
    run dolt merge other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ "$output" =~ "1 rows modified" ]] || false

}

@test "conflict-detection: two branches modify different cell different row. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "table created"
    dolt branch change-cell
    dolt sql -q "replace into test values (0, 11, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "changed pk=0 c1 to 11"
    dolt checkout change-cell
    dolt sql -q "replace into test values (1, 1, 1, 1, 1, 11)"
    dolt add test
    dolt commit -m "changed pk=1 c5 to 11"
    dolt checkout main
    run dolt merge change-cell
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ "$output" =~ "1 rows modified" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
    run dolt status
    [[ "$output" =~ "All conflicts and constraint violations fixed" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
}

@test "conflict-detection: two branches modify different cell same row. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "table created"
    dolt branch change-cell
    dolt sql -q "replace into test values (0, 11, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "changed pk=0 c1 to 11"
    dolt checkout change-cell
    dolt sql -q "replace into test values (0, 0, 0, 0, 0, 11)"
    dolt add test
    dolt commit -m "changed pk=0 c5 to 11"
    dolt checkout main
    run dolt merge change-cell
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ "$output" =~ "1 rows modified" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
    run dolt status
    [[ "$output" =~ "All conflicts and constraint violations fixed" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
}

@test "conflict-detection: two branches modify same cell. merge. conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "table created"
    dolt branch change-cell
    dolt sql -q "replace into test values (0, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "changed pk=0 all cells to 1"
    dolt checkout change-cell
    dolt sql -q "replace into test values (0, 11, 11, 11, 11, 11)"
    dolt add test
    dolt commit -m "changed pk=0 all cells to 11"
    dolt checkout main
    run dolt merge change-cell
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
    run dolt status
    [[ "$output" =~ "You have unmerged tables." ]] || false
    [[ "$output" =~ "Unmerged paths:" ]] || false
}

@test "conflict-detection: two branches add a different row. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"
    dolt branch add-row
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "added pk=0 row"
    dolt checkout add-row
    dolt sql -q "insert into test values (1, 1, 1, 1, 1, 1)"
    dolt add test
    dolt commit -m "added pk=1 row"
    dolt checkout main
    run dolt merge add-row
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ "$output" =~ "1 rows added" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection: two branches add same row. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"
    dolt branch add-row
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "added pk=0 row"
    dolt checkout add-row
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "added pk=0 row"
    dolt checkout main
    run dolt merge add-row
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection: one branch add table, other modifies table. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"
    dolt branch add-table
    dolt sql -q "insert into test values (0, 0, 0, 0, 0, 0)"
    dolt add test
    dolt commit -m "added row"
    dolt checkout add-table
    dolt sql <<SQL
CREATE TABLE test2 (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt add test2
    dolt commit -m "added new table test2"
    dolt checkout main
    run dolt merge add-table
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    skip "should have a merge summary section that says 1 table changed"
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection: two branches add same column. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"
    dolt branch add-column
    dolt sql -q "alter table test add c0 bigint"
    dolt add test
    dolt commit -m "added column c0"
    dolt checkout add-column
    dolt sql -q "alter table test add c0 bigint"
    dolt add test
    dolt commit -m "added same column c0"
    dolt checkout main
    run dolt merge add-column
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection: two branches add different column. merge. no conflict" {
    skip https://github.com/dolthub/dolt/issues/773
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt sql -q 'insert into test (pk, c1, c2, c3, c4, c5) values (0,1,2,3,4,5);'
    dolt add test
    dolt commit -m "table created"
    dolt branch add-column
    dolt sql -q "alter table test add c0 bigint"
    dolt add test
    dolt commit -m "added column c0"
    dolt checkout add-column
    dolt sql -q "alter table test add c6 bigint"
    dolt add test
    dolt commit -m "added column c6"
    dolt checkout main
    run dolt merge add-column
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ "$output" =~ "1 tables changed" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false

    run dolt sql -q 'select * from test;'
    [ $status -eq 0 ]
    [[ "${lines[3]}" =~ "| 0  | 1  | 2  | 3  | 4  | 5  |  NULL  |  NULL  |" ]] || false
}

@test "conflict-detection: two branches add same column, different types. merge. conflict" {
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
    dolt branch add-column
    dolt sql -q "alter table test add c0 longtext"
    dolt add test
    dolt commit -m "added column c0 as string"
    dolt checkout add-column
    dolt sql -q "alter table test add c0 bigint"
    dolt add test
    dolt commit -m "added column c0 as int"
    dolt checkout main
    skip "This created two c0 columns with different types and tag numbers. Bug I think."
    run dolt merge add-column
    [ $status -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection: two branches delete same column. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"
    dolt branch delete-column
    dolt sql -q "alter table test drop column c5"
    dolt add test
    dolt commit -m "deleted c5 column"
    dolt checkout delete-column
    dolt sql -q "alter table test drop column c5"
    dolt add test
    dolt commit -m "deleted c5 again"
    dolt checkout main
    run dolt merge delete-column
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection: two branches delete different column. merge. no conflict" {
    skip https://github.com/dolthub/dolt/issues/773
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

    dolt checkout -b one
    dolt sql -q "alter table test drop column c5"
    dolt add test
    dolt commit -m "deleted column c5"

    dolt checkout main
    dolt checkout -b two
    dolt sql -q "alter table test drop column c4"
    dolt add test
    dolt commit -m "deleted column c4"

    run dolt merge one
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

@test "conflict-detection: two branches rename same column to same name. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"
    dolt branch rename-column
    dolt sql -q "alter table test rename column c5 to c0"
    dolt add test
    dolt commit -m "renamed c5 to c0"
    dolt checkout rename-column
    dolt sql -q "alter table test rename column c5 to c0"
    dolt add test
    dolt commit -m "renamed c5 to c0 again"
    dolt checkout main
    run dolt merge rename-column
    [ $status -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection: two branches rename same column to different name. merge. conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
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
    dolt checkout main
    run dolt merge rename-column
    [ $status -eq 1 ]
    [[ "$output" =~ "Bad merge" ]] || false
    [ $status -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection: two branches rename different column to same name. merge. conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL,
  c1 BIGINT,
  c2 BIGINT,
  c3 BIGINT,
  c4 BIGINT,
  c5 BIGINT,
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
    dolt checkout main
    run dolt merge rename-column
    [ $status -eq 1 ]
    [[ "$output" =~ "Bad merge" ]] || false
    [ $status -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
}

@test "conflict-detection: adding primary key also adds not null constraint. merge. no conflict" {
    dolt sql <<SQL
CREATE TABLE test (
  pk INT,
  c1 INT,
  PRIMARY KEY (pk)
);
SQL
    dolt add test
    dolt commit -m "table created"
    dolt branch other
    dolt checkout other
    dolt sql -q "alter table test modify pk int not null"
    run dolt status
    [ $status -eq 0 ]
    [[ "$output" =~ "nothing to commit" ]] || false
    dolt checkout main
    run dolt merge other
    [ $status -eq 0 ]
    [[ ! "$output" =~ "CONFLICT" ]] || false
}
