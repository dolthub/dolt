#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
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
}

teardown() {
    teardown_common
}

# Create a single primary key table and do stuff
@test "create a table with a schema file and examine repo" {
    # Remove the docs, because they will show up in the diff below and break the lines[x] assertions.
    rm LICENSE.md
    rm README.md
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "test" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ pk[[:space:]]+\|[[:space:]]+c1[[:space:]]+\|[[:space:]]+c2[[:space:]]+\|[[:space:]]+c3[[:space:]]+\|[[:space:]]+c4[[:space:]]+\|[[:space:]]+c5 ]] || false
    run dolt diff
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "diff --dolt a/test b/test" ]
    [ "${lines[1]}" = "added table" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]]
    [[ "$output" =~ new[[:space:]]table:[[:space:]]+test ]] || false
}

@test "create a table, dolt add, dolt reset, and dolt commit" {
    run dolt add test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed" ]]
    [[ "$output" =~ "new table:" ]] || false
    run dolt reset test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]]
    [[ "$output" =~ "new table:" ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed" ]]
    [[ "$output" =~ "new table:" ]] || false
    run dolt commit -m "test commit"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]] || false

}

@test "dolt log with -n specified" {
    dolt add test
    dolt commit -m "first commit"
    run dolt log
    [ "$status" -eq "0" ]
    [[ "$output" =~ "first commit" ]] || false
    [[ "$output" =~ "Initialize data repository" ]] || false
    run dolt log -n 1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "first commit" ]] || false
    [[ ! "$output" =~ "Initialize data repository" ]] || false
}

@test "add a row to a created table using dolt table put-row" {
    dolt add test
    dolt commit -m "create table"
    run dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    [ "$status" -eq 0 ]
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+0[[:space:]]+\|[[:space:]]+1 ]] || false
}

@test "dolt table import from stdin export to stdout" {
    skiponwindows "Need to install python before this test will work."
    echo 'pk,c1,c2,c3,c4,c5
0,1,2,3,4,5
9,8,7,6,5,4
'|dolt table import -u test
    dolt table export --file-type=csv test|python -c '
import sys
rows = []
for line in sys.stdin:
    line = line.strip()

    if line != "":
        rows.append(line.strip().split(","))

if len(rows) != 3:
    sys.exit(1)

if rows[0] != "pk,c1,c2,c3,c4,c5".split(","):
    sys.exit(1)

if rows[1] != "0,1,2,3,4,5".split(","):
    sys.exit(1)

if rows[2] != "9,8,7,6,5,4".split(","):
    sys.exit(1)
'
}

@test "dolt sql all manner of inserts" {
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 1       |" ]] || false
    run dolt sql -q "select * from test"
    [[ "$output" =~ "6" ]] || false
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (1,7,7,7,7,7),(2,8,8,8,8,8)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 2       |" ]] || false
    run dolt sql -q "select * from test"
    [[ "$output" =~ "7" ]] || false
    [[ "$output" =~ "8" ]] || false
    run dolt sql -q "insert into test (pk,c1,c3,c5) values (3,9,9,9)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 1       |" ]] || false
    run dolt sql -q "select * from test"
    [[ "$output" =~ "9" ]] || false
    run dolt sql -q "insert into test (c1,c3,c5) values (50,55,60)"
    [ "$status" -eq 1 ]
    [ "$output" = "column name 'pk' is non-nullable but attempted to set default value of null" ]
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5,c6) values (10,1,1,1,1,1,1)"
    [ "$status" -eq 1 ]
    [ "$output" = "invalid column name c6" ]
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "duplicate primary key" ]] || false
}

@test "dolt sql insert same column twice" {
    run dolt sql -q "insert into test (pk,c1,c1) values (3,1,2)"
    [ "$status" -eq 1 ]
    [ "$output" = "duplicate column name c1" ]
}

@test "dolt sql insert no columns specified" {
    run dolt sql -q "insert into test values (0,0,0,0,0,0)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 1       |" ]] || false
    run dolt sql -q "select * from test"
    [[ "$output" =~ "0" ]] || false
    run dolt sql -q "insert into test values (4,1,2)"
    [ "$status" -eq 1 ]
    [ "$output" = "number of values does not match number of columns provided" ]
}

@test "dolt sql with insert ignore" {
    skip "New engine does not support insert ignore"
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6)"
    run dolt sql -q "insert ignore into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6),(11,111,111,111,111,111)"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "Rows inserted: 1" ]
    [ "${lines[1]}" = "Errors ignored: 1" ]
    run dolt sql -q "select * from test"
    [[ "$output" =~ "111" ]] || false
}

@test "dolt sql replace into" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6)"
    run dolt sql -q "replace into test (pk,c1,c2,c3,c4,c5) values (0,7,7,7,7,7),(1,8,8,8,8,8)"
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "updated" ]] || false
    ## No skip, but this should report 3 but is reporting 4 [[ "${lines[3]}" =~ "3" ]] || false
    run dolt sql -q "select * from test"
    [[ "$output" =~ "7" ]] || false
    [[ "$output" =~ "8" ]] || false
    [[ ! "$output" =~ "6" ]] || false
}

@test "dolt sql insert and dolt sql select" {
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 1       |" ]] || false
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (101,102,103,104,105,106),(1,6,7,8,9,10)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 2       |" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ \|[[:space:]]+c5 ]] || false
    [[ "$output" =~ \|[[:space:]]+5 ]] || false
    [[ "$output" =~ \|[[:space:]]+10 ]] || false
    [[ "$output" =~ \|[[:space:]]+106 ]] || false
    run dolt sql -q "select * from test where pk=1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ \|[[:space:]]+c5 ]] || false
    [[ "$output" =~ \|[[:space:]]+10 ]] || false
    run dolt sql -q "select c5 from test where pk=1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ \|[[:space:]]+c5 ]] || false
    [[ "$output" =~ \|[[:space:]]+10 ]] || false
    run dolt sql -q "select * from test limit 1"
    [ "$status" -eq 0 ]
    # All line number assertions are offset by 3 to allow for table separator lines
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select * from test where c2 > 7"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select * from test where c2 >= 7"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    run dolt sql -q "select * from test where c2 <> 7"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    run dolt sql -q "select * from test where c2 > 3 and c1 < 10"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    run dolt sql -q "select c10 from test where pk=1"
    [ "$status" -eq 1 ]
    [ "$output" = "column \"c10\" could not be found in any table in scope" ]
    run dolt sql -q "select * from test where c2=147"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
}

@test "dolt sql select as" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "select c1 as column1, c2 as column2 from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "column1" ]] || false
    [[ "$output" =~ "column2" ]] || false
    [[ ! "$output" =~ "c1" ]] || false
    [[ ! "$output" =~ "c2" ]] || false
    run dolt sql -q "select c1 as column1 from test where c1=1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ column1 ]] || false
    [ "${#lines[@]}" -eq 5 ]
}

@test "dolt sql select csv output" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "select c1 as column1, c2 as column2 from test" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "column1,column2" ]] || false
    [[ "$output" =~ "1,2" ]] || false
    [[ "$output" =~ "11,12" ]] || false

    run dolt sql -q "select c1 as column1 from test where c1=1" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'column1' ]] || false
    [ "${#lines[@]}" -eq 2 ]
}


@test "dolt sql select with inverted where clause" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "select * from test where 5 > c1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
}

@test "dolt sql update queries" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "update test set c1=6,c2=7,c3=8,c4=9,c5=10 where pk=0"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 1       | 1       |" ]] || false
    run dolt sql -q "select * from test where pk=0"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "10" ]] || false
    [[ ! "$output" =~ "|5" ]] || false
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (4,11,12,13,14,15)"
    run dolt sql -q "update test set c2=11,c3=11,c4=11,c5=11 where c1=11"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 2       | 2       |" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "11" ]] || false
    [[ ! "$output" =~ "12" ]] || false
    run dolt sql -q "update test set c2=50,c3=50,c4=50,c5=50 where c1=50"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 0       | 0       |" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "50" ]] || false
    run dolt sql -q "update test set c12=11 where pk=0"
    [ "$status" -eq 1 ]
    [ "$output" = "column \"c12\" could not be found in any table in scope" ]
    run dolt sql -q "update test set c1='foo' where pk=0"
    [ "$status" -eq 1 ]
    [ "$output" = "unable to cast \"foo\" of type string to int64" ]
    run dolt sql -q "update test set c1=100,c2=100,c3=100,c4=100,c5=100 where pk>0"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 3       | 3       |" ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "100" ]] || false
    [[ "$output" =~ "10" ]] || false
    [[ ! "$output" =~ "11" ]] || false
}

@test "dolt sql delete queries" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "delete from test where pk=2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 1       |" ]] || false
    run dolt sql -q "delete from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 2       |" ]] || false
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "delete from test where pk>0"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 2       |" ]] || false
    run dolt sql -q "delete from test where c1=1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 1       |" ]] || false
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "delete from test where c10=1"
    [ "$status" -eq 1 ]
    [ "$output" = "column \"c10\" could not be found in any table in scope" ]
    run dolt sql -q "delete from test where c1='foo'"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "| 0       |" ]] || false
}

@test "dolt checkout to put a table back to its checked in state" {
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "Added table and test row"
    dolt sql -q "replace into test values (0, 1, 2, 3, 4, 10)"
    run dolt checkout test
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "5" ]] || false
    [[ ! "$output" =~ "10" ]] || false
}

@test "dolt checkout branch and table name collision" {
    dolt branch test
    run dolt checkout test
    [ "$status" -eq 0 ]
    skip "behavior ambiguous right now. should reset test table and switch to branch per git"
}

@test "make a change on a different branch, commit, and merge to master" {
    dolt branch test-branch
    dolt checkout test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "added test row"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]] || false
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added test row" ]] || false
}

@test "create a branch off an older commit than HEAD" {
    dolt add test
    dolt commit -m "first commit"
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "added test row"
    run dolt checkout -b older-branch HEAD^
    [ "$status" -eq 0 ]
    [ "$output" = "Switched to branch 'older-branch'" ]
    run dolt log
    [[ ! "$output" =~ "added test row" ]] || false
    [[ "$output" =~ "first commit" ]] || false
}

@test "delete an unmerged branch" {
    dolt checkout -b test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "added test row"
    run dolt branch -d test-branch
    [ "$status" -ne 0 ]
    [ "$output" = "error: Cannot delete checked out branch 'test-branch'" ]
    dolt checkout master
    run dolt branch -d test-branch
    [ "$status" -ne 0 ]
    run dolt branch -d -f test-branch
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "generate a merge conflict and resolve with ours" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 6)"
    dolt add test
    dolt commit -m "added conflicting test row"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT (content)" ]]
    run dolt conflicts cat test
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+ours[[:space:]] ]] || false
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+theirs[[:space:]] ]] || false
    run dolt conflicts resolve --ours test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ \|[[:space:]]+5 ]] || false
    [[ ! "$output" =~ \|[[:space:]]+6 ]] || false
    run dolt conflicts cat test
    [[ ! "$output" =~ "ours" ]] || false
    [[ ! "$output" =~ "theirs" ]] || false
    dolt add test
    dolt commit -m "merged and resolved conflict"
    run dolt log
    [[ "$output" =~ "added test row" ]] || false
    [[ "$output" =~ "added conflicting test row" ]] || false
    [[ "$output" =~ "merged and resolved conflict" ]] || false
    [[ "$output" =~ "Merge:" ]] || false
}

@test "generate a merge conflict and try to roll back using dolt merge --abort" {
    # L&R must be removed (or added and committed) in order to test merge
    rm "LICENSE.md"
    rm "README.md"
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 6)"
    dolt add test
    dolt commit -m "added conflicting test row"
    dolt checkout master
    dolt merge test-branch
    run dolt checkout test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt sql -q "select * from test"
    [[ "$output" =~ \|[[:space:]]+5 ]] || false
    run dolt conflicts cat test
    [[ ! "$output" =~ "ours" ]] || false
    [[ ! "$output" =~ "theirs" ]] || false
    run dolt status
    [[ "$output" =~ "All conflicts fixed but you are still merging." ]] || false
    run dolt merge --abort
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "generate a merge conflict and resolve with theirs" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 6)"
    dolt add test
    dolt commit -m "added conflicting test row"
    dolt checkout master
    dolt merge test-branch
    run dolt conflicts resolve --theirs test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt sql -q "select * from test"
    [[ "$output" =~ \|[[:space:]]+6 ]] || false
    [[ ! "$output" =~ "|5" ]] || false
}

@test "put a row that violates the schema" {
    run dolt sql -q "insert into test values (0, 1, 2, 3, 4, 'foo')"
    [ "$status" -ne 0 ]
}

@test "put a row that has a column not in the schema" {
    run dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5, 10)"
    [ "$status" -ne 0 ]
}

@test "import data from a csv file after table created" {
    run dolt table import test -u `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    # Number of lines offset by 3 for table printing style
    [ "${#lines[@]}" -eq 6 ]
}

@test "import data from a csv file with a bad line" {
    run dolt table import test -u `batshelper 1pk5col-ints-badline.csv`
    [ "$status" -eq 1 ]
    [[ "${lines[0]}" =~ "Additions" ]] || false
    [[ "${lines[1]}" =~ "A bad row was encountered" ]] || false
    [[ "${lines[2]}" =~ "expects 6 fields" ]] || false
    [[ "${lines[2]}" =~ "line only has 1 value" ]] || false
}

@test "import data from a csv file with a bad header" {
cat <<DELIM > bad.csv
,c1,c2,c3,c4,c5
0,1,2,3,4,5
DELIM
    run dolt table import test -u bad.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "bad header line: column cannot be NULL or empty string" ]] || false
    [[ ! "$output" =~ "panic" ]] || false

cat <<DELIM > bad.csv
pk,c1, ,c3,c4,c5
0,1,2,3,4,5
DELIM
        run dolt table import test -u bad.csv
        [ "$status" -eq 1 ]
        [[ "$output" =~ "bad header line: column cannot be NULL or empty string" ]] || false
        [[ ! "$output" =~ "panic" ]] || false

cat <<DELIM > bad.csv
pk,c1,"",c3,c4,c5
0,1,2,3,4,5
DELIM
        run dolt table import test -u bad.csv
        [ "$status" -eq 1 ]
        [[ "$output" =~ "bad header line: column cannot be NULL or empty string" ]] || false
        [[ ! "$output" =~ "panic" ]] || false

cat <<DELIM > bad.csv
pk,c1," ",c3,c4,c5
0,1,2,3,4,5
DELIM
        run dolt table import test -u bad.csv
        [ "$status" -eq 0 ]
}

@test "import data from a psv file after table created" {
    run dolt table import test -u  `batshelper 1pk5col-ints.psv`
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully." ]] || false
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    # Number of lines offset by 3 for table printing style
    [ "${#lines[@]}" -eq 6 ]
}

@test "overwrite a row. make sure it updates not inserts" {
    dolt table import test -u `batshelper 1pk5col-ints.csv`
    run dolt sql -q "replace into test values (1, 2, 4, 6, 8, 10)"
    [ "$status" -eq 0 ]
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    # Number of lines offset by 3 for table printing style
    [ "${#lines[@]}" -eq 6 ]
}

@test "dolt table export" {
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    run dolt table export test export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f export.csv ]
    run grep 5 export.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    run dolt table export test export.csv
    [ "$status" -ne 0 ]
    [[ "$output" =~ "export.csv already exists" ]] || false
    run dolt table export -f test export.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f export.csv ]
}

@test "dolt table SQL export" {
    dolt sql -q "insert into test values (0, 1, 2, 3, 4, 5)"
    run dolt table export test export.sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f export.sql ]
    diff --strip-trailing-cr $BATS_TEST_DIRNAME/helper/1pk5col-ints.sql export.sql
}

@test "dolt schema show" {
    run dolt schema show
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]] || false
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL COMMENT 'tag:0'" ]] || false
    [[ "$output" =~ "\`c1\` BIGINT COMMENT 'tag:1'" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT COMMENT 'tag:2'" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT COMMENT 'tag:3'" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT COMMENT 'tag:4'" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT COMMENT 'tag:5'" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
    run dolt schema show test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]] || false
    [[ "$output" =~ "CREATE TABLE \`test\`" ]] || false
    [[ "$output" =~ "\`pk\` BIGINT NOT NULL COMMENT 'tag:0'" ]] || false
    [[ "$output" =~ "\`c1\` BIGINT COMMENT 'tag:1'" ]] || false
    [[ "$output" =~ "\`c2\` BIGINT COMMENT 'tag:2'" ]] || false
    [[ "$output" =~ "\`c3\` BIGINT COMMENT 'tag:3'" ]] || false
    [[ "$output" =~ "\`c4\` BIGINT COMMENT 'tag:4'" ]] || false
    [[ "$output" =~ "\`c5\` BIGINT COMMENT 'tag:5'" ]] || false
    [[ "$output" =~ "PRIMARY KEY (\`pk\`)" ]] || false
}

@test "dolt schema show on non existant table" {
    run dolt schema show foo
    [ "$status" -eq 0 ]
    [ "$output" = "foo not found" ]
}

@test "dolt schema export" {
    run dolt schema export test export.schema
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    [ -f export.schema ]
    run diff --strip-trailing-cr $BATS_TEST_DIRNAME/helper/1pk5col-ints-schema.json export.schema
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "rm a staged but uncommitted table" {
    # L&R must be removed (or added and committed) for `nothing to commit` message
    rm "LICENSE.md"
    rm "README.md"
    run dolt add test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table rm test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt add test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "${lines[0]}" = "On branch master" ]
    [ "${lines[1]}" = "nothing to commit, working tree clean" ]
}

@test "create and view a table with NULL values" {
    dolt sql -q "insert into test (pk) values (0)"
    dolt sql -q "insert into test (pk) values (1)"
    dolt sql -q "insert into test (pk) values (2)"
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "<NULL>" ]] || false
    doltsqloutput=$output
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "<NULL>" ]] || false
    [ "$output" = "$doltsqloutput" ]
    # Make sure we don't get a table with no spaces because that bug was
    # generated when making changes to NULL printing
    [[ ! "$output" =~ "|||||" ]] || false
}

@test "using dolt sql to select rows with NULL values" {
    dolt sql -q "insert into test (pk) values (0)"
    dolt sql -q "insert into test (pk) values (1)"
    dolt sql -q "insert into test values (2, 0, 0, 0, 0, 0)"
    run dolt sql -q "select * from test where c1 is null"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "<NULL>" ]] || false
}
