#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    # Append the directory name with the pid of the calling process so
    # multiple tests can be run in parallel on the same machine
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col-ints.schema test
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

# Create a single primary key table and do stuff
@test "create a table with a schema file and examine repo" {
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "test" ]] || false
    run dolt table select test
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
    [ "$output" = "" ]
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
    [[ "$output" =~ "Data repository created." ]] || false
    run dolt log -n 1
    [ "$status" -eq "0" ]
    [[ "$output" =~ "first commit" ]] || false
    [[ ! "$output" =~ "Data repository created." ]] || false
}

@test "add a row to a created table using dolt table put-row" {
    dolt add test
    dolt commit -m "create table"
    run dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+0[[:space:]]+\|[[:space:]]+1 ]] || false
}

@test "dolt sql all manner of inserts" {
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6)"
    [ "$status" -eq 0 ]
    [ "$output" = "Rows inserted: 1" ]
    run dolt table select test
    [[ "$output" =~ "6" ]] || false
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (1,7,7,7,7,7),(2,8,8,8,8,8)"
    [ "$status" -eq 0 ]
    [ "$output" = "Rows inserted: 2" ]
    run dolt table select test
    [[ "$output" =~ "7" ]] || false
    [[ "$output" =~ "8" ]] || false
    run dolt sql -q "insert into test (pk,c1,c3,c5) values (3,9,9,9)"
    [ "$status" -eq 0 ]
    [ "$output" = "Rows inserted: 1" ]
    run dolt table select test
    [[ "$output" =~ "9" ]] || false
    run dolt sql -q "insert into test (c1,c3,c5) values (50,55,60)"
    [ "$status" -eq 1 ]
    [ "$output" = "Error inserting rows: [one or more primary key columns missing from insert statement]" ]
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5,c6) values (10,1,1,1,1,1,1)"
    [ "$status" -eq 1 ]
    [ "$output" = "Error inserting rows: [Unknown column c6]" ]
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot insert existing row" ]] || false
}

@test "dolt sql insert same column twice" {
    run dolt sql -q "insert into test (pk,c1,c1) values (3,1,2)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Repeated column" ]] || false
}

@test "dolt sql insert no columns specified" {
    run dolt sql -q "insert into test values (0,0,0,0,0,0)"
    [ "$status" -eq 0 ]
    [ "$output" = "Rows inserted: 1" ]
    run dolt table select test
    [[ "$output" =~ "0" ]] || false
    run dolt sql -q "insert into test values (4,1,2)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Wrong number of values" ]] || false
}

@test "dolt sql with insert ignore" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6)"
    run dolt sql -q "insert ignore into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6),(11,111,111,111,111,111)"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "Rows inserted: 1" ]
    [ "${lines[1]}" = "Errors ignored: 1" ]
    run dolt table select test
    [[ "$output" =~ "111" ]] || false
}

@test "dolt sql replace into" {
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,6,6,6,6,6)"
    run dolt sql -q "replace into test (pk,c1,c2,c3,c4,c5) values (0,7,7,7,7,7),(1,8,8,8,8,8)"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "Rows inserted: 1" ]
    [ "${lines[1]}" = "Rows updated: 1" ]
    run dolt table select test
    [[ "$output" =~ "7" ]] || false
    [[ "$output" =~ "8" ]] || false
    [[ ! "$output" =~ "6" ]] || false
}

@test "dolt sql insert and dolt sql select" {
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5)"
    [ "$status" -eq 0 ]
    [ "$output" = "Rows inserted: 1" ]
    run dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (101,102,103,104,105,106),(1,6,7,8,9,10)"
    [ "$status" -eq 0 ]
    [ "$output" = "Rows inserted: 2" ]
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
    [ "$output" = "Unknown column: 'c10'" ]
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
    run dolt sql -q "select c1 as column1 from test where column1=1"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
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
    [ "$output" = "Rows updated: 1" ]
    run dolt sql -q "select * from test where pk=0"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "10" ]] || false
    [[ ! "$output" =~ "|5" ]] || false
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (4,11,12,13,14,15)"
    run dolt sql -q "update test set c2=11,c3=11,c4=11,c5=11 where c1=11"
    [ "$status" -eq 0 ]
    [ "$output" = "Rows updated: 2" ]
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "11" ]] || false
    [[ ! "$output" =~ "12" ]] || false
    run dolt sql -q "update test set c2=50,c3=50,c4=50,c5=50 where c1=50"

[ "$status" -eq 0 ]
    [ "$output" = "Rows updated: 0" ]
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "50" ]] || false
    run dolt sql -q "update test set c12=11 where pk=0"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Unknown column 'c12'" ]] || false
    run dolt sql -q "update test set c1='foo' where pk=0"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Type mismatch" ]] || false
    run dolt sql -q "update test set c1=100,c2=100,c3=100,c4=100,c5=100 where pk>0"
    [ "$status" -eq 0 ]
    [ "$output" = "Rows updated: 3" ]
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
    [ "$output" = "Rows deleted: 1" ]
    run dolt sql -q "delete from test"
    [ "$status" -eq 0 ]
    [ "$output" = "Rows deleted: 2" ]
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "delete from test where pk>0"
    [ "$status" -eq 0 ]
    [ "$output" = "Rows deleted: 2" ]
    run dolt sql -q "delete from test where c1=1"
    [ "$status" -eq 0 ]
    [ "$output" = "Rows deleted: 1" ]
    dolt sql -q "insert into test (pk,c1,c2,c3,c4,c5) values (0,1,2,3,4,5),(1,11,12,13,14,15),(2,21,22,23,24,25)"
    run dolt sql -q "delete from test where c10=1"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Unknown column: 'c10'" ]] || false
    run dolt sql -q "delete from test where c1='foo'"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error during update" ]] || false
}

@test "delete a row with dolt table rm-row" {
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    run dolt table rm-row test 0
    [ "$status" -eq 0 ]
    [ "$output" = "Removed 1 rows" ]
}

@test "delete multiple rows with dolt table rm-row" {
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt table put-row test pk:1 c1:1 c2:2 c3:3 c4:4 c5:5
    run dolt table rm-row test 0 1
    [ "$status" -eq 0 ]
    [ "$output" = "Removed 2 rows" ]
}

@test "dolt checkout to put a table back to its checked in state" {
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "Added table and test row"
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:10
    run dolt checkout test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
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
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
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
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
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
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
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
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:6
    dolt add test
    dolt commit -m "added conflicting test row"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT (content)" ]]
    run dolt table select test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Cnf" ]]
    [[ "$output" =~ "!" ]]
    run dolt conflicts cat test
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+ours[[:space:]] ]] || false
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+theirs[[:space:]] ]] || false
    run dolt conflicts resolve --ours test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Cnf" ]] || false
    [[ ! "$output" =~ "!" ]] || false
    [[ "$output" =~ \|[[:space:]]+5 ]] || false
    [[ ! "$output" =~ \|[[:space:]]+6 ]] || false
    dolt add test
    dolt commit -m "merged and resolved conflict"
    run dolt log
    [[ "$output" =~ "added test row" ]] || false
    [[ "$output" =~ "added conflicting test row" ]] || false
    [[ "$output" =~ "merged and resolved conflict" ]] || false
    [[ "$output" =~ "Merge:" ]] || false
}

@test "dolt table select --hide-conflicts" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:6
    dolt add test
    dolt commit -m "added conflicting test row"
    dolt checkout master
    dolt merge test-branch
    run dolt table select --hide-conflicts test
    [ "$status" -eq 0 ]
    skip "--hide-conflicts does not work"
    [[ ! "$output" =~ "Cnf" ]] || false
    [[ ! "$output" =~ "!" ]] || false
}

@test "generate a merge conflict and try to roll back using dolt merge --abort" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:6
    dolt add test
    dolt commit -m "added conflicting test row"
    dolt checkout master
    dolt merge test-branch
    run dolt checkout test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [[ "$output" =~ \|[[:space:]]+5 ]] || false
    [[ ! "$output" =~ "Cnf" ]] || false
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
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:6
    dolt add test
    dolt commit -m "added conflicting test row"
    dolt checkout master
    dolt merge test-branch
    run dolt conflicts resolve --theirs test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [[ "$output" =~ \|[[:space:]]+6 ]] || false
    [[ ! "$output" =~ "|5" ]] || false
}

@test "put a row that violates the schema" {
    run dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:foo
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "inserted row does not match schema" ]
}

@test "put a row that has a column not in the schema" {
    run dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5 c6:10
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "Not all supplied keys are known in this table's schema." ]
}

@test "import data from a csv file after table created" {
    run dolt table import test -u $BATS_TEST_DIRNAME/helper/1pk5col-ints.csv
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    # Number of lines offset by 3 for table printing style
    [ "${#lines[@]}" -eq 6 ]
}

@test "import data from a psv file after table created" {
    run dolt table import test -u  $BATS_TEST_DIRNAME/helper/1pk5col-ints.psv
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    # Number of lines offset by 3 for table printing style
    [ "${#lines[@]}" -eq 6 ]
}

@test "overwrite a row. make sure it updates not inserts" {
    dolt table import test -u $BATS_TEST_DIRNAME/helper/1pk5col-ints.csv
    run dolt table put-row test pk:1 c1:2 c2:4 c3:6 c4:8 c5:10
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt table select test
    [ "$status" -eq 0 ]
    # Number of lines offset by 3 for table printing style
    [ "${#lines[@]}" -eq 6 ]
}

@test "add row on two different branches. no merge conflict" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt table put-row test pk:1 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row with one more column"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
    run dolt table select test
    [ "$status" -eq 0 ]
    # Number of lines offset by 3 for table printing style
    [ "${#lines[@]}" -eq 6 ]
}

@test "add column, no merge conflict" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt schema --add-column test c6 int 
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5 c6:6
    dolt add test
    dolt commit -m "added test row with one more column"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
    run dolt diff 
    [ "$status" -eq 0 ]
    [[ "$output" =~ " 6" ]] || false
}

@test "add different columns on two branches, no merge conflict" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt schema --add-column test c6 int
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5 c6:6
    dolt add test
    dolt commit -m "added new column and test row"
    dolt checkout test-branch
    dolt schema --add-column test c7 int
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5 c7:7
    dolt add test
    dolt commit -m "added different column and test row"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ " 6" ]] || false
    [[ "$output" =~ " 7" ]] || false
}

@test "add a column and row on one branch a row on the other, no merge conflict" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added a test row"
    dolt checkout test-branch
    dolt schema --add-column test c6 int
    dolt table put-row test pk:1 c1:2 c2:4 c3:6 c4:8 c5:10 c6:12
    dolt add test
    dolt commit -m "added a column and new test row"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ " 12" ]] || false
}

@test "modify different fields, same row, no merge conflict" {
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt branch test-branch
    dolt table put-row test pk:0 c1:2 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "modified c1 of test row"
    dolt checkout test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:10
    dolt add test
    dolt commit -m "modified c5 of test row"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "10" ]] || false
}

@test "remove different row, no merge conflict" {
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt table put-row test pk:1 c1:6 c2:7 c3:8 c4:9 c5:10
    dolt table put-row test pk:2 c1:11 c2:12 c3:13 c4:14 c5:15
    dolt add test
    dolt commit -m "added test rows"
    dolt branch test-branch
    dolt table rm-row test 1
    dolt add test
    dolt commit -m "removed pk=1 row"
    dolt checkout test-branch
    dolt table rm-row test 2
    dolt add test
    dolt commit -m "removed pk=2 row"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
}

@test "remove same row, no merge conflict" {
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt table put-row test pk:1 c1:6 c2:7 c3:8 c4:9 c5:10
    dolt table put-row test pk:2 c1:11 c2:12 c3:13 c4:14 c5:15
    dolt add test
    dolt commit -m "added test rows"
    dolt branch test-branch
    dolt table rm-row test 1
    dolt add test
    dolt commit -m "removed pk=1 row"
    dolt checkout test-branch
    dolt table rm-row test 1
    dolt add test
    dolt commit -m "removed pk=1 row"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]] || false
    [[ ! "$output" =~ "CONFLICT" ]] || false
} 

@test "dolt table select with options" {
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt table put-row test pk:1 c1:6 c2:7 c3:8 c4:9 c5:10
    dolt table put-row test pk:2 c1:1 c2:2 c3:3 c4:4 c5:5
    run dolt table select --where pk=1 test
    [ "$status" -eq 0 ]
    [[ "$output" =~ \|[[:space:]]+10 ]] || false
    [[ ! "$output" =~ \|[[:space:]]+5 ]] || false
    [ "${#lines[@]}" -eq 5 ]
    run dolt table select --where c1=1 test
    [ "$status" -eq 0 ]
    [[ "$output" =~ \|[[:space:]]+5 ]] || false
    [[ ! "$output" =~ "|10" ]] || false
    [ "${#lines[@]}" -eq 6 ]
    run dolt table select test pk c1 c2 c3
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "c4" ]] || false
    run dolt table select --where c1=1 --limit=1 test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
}

@test "dolt table export" {
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    run dolt table export test export.csv
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully exported data." ]
    [ -f export.csv ]
    run grep 5 export.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    run dolt table export test export.csv
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Data already exists in" ]] || false
    run dolt table export -f test export.csv
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully exported data." ]
    [ -f export.csv ]
}

@test "dolt schema" {
    run dolt schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]] || false
    [[ "$output" =~ "CREATE TABLE test" ]] || false
    [[ "$output" =~ "pk int not null comment 'tag:0'" ]] || false
    [[ "$output" =~ "c1 int comment 'tag:1'" ]] || false
    [[ "$output" =~ "c2 int comment 'tag:2'" ]] || false
    [[ "$output" =~ "c3 int comment 'tag:3'" ]] || false
    [[ "$output" =~ "c4 int comment 'tag:4'" ]] || false
    [[ "$output" =~ "c5 int comment 'tag:5'" ]] || false
    [[ "$output" =~ "primary key (pk)" ]] || false
    run dolt schema test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]] || false
    [[ "$output" =~ "CREATE TABLE test" ]] || false
    [[ "$output" =~ "pk int not null comment 'tag:0'" ]] || false
    [[ "$output" =~ "c1 int comment 'tag:1'" ]] || false
    [[ "$output" =~ "c2 int comment 'tag:2'" ]] || false
    [[ "$output" =~ "c3 int comment 'tag:3'" ]] || false
    [[ "$output" =~ "c4 int comment 'tag:4'" ]] || false
    [[ "$output" =~ "c5 int comment 'tag:5'" ]] || false
    [[ "$output" =~ "primary key (pk)" ]] || false
}

@test "dolt schema on non existant table" {
    run dolt schema foo
    [ "$status" -eq 0 ]
    [ "$output" = "foo not found" ]
}

@test "dolt schema --export" {
    run dolt schema --export test export.schema
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    [ -f export.schema ]
    run diff $BATS_TEST_DIRNAME/helper/1pk5col-ints.schema export.schema
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "rm a staged but uncommitted table" {
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
    dolt table put-row test pk:0
    dolt table put-row test pk:1
    dolt table put-row test pk:2
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "<NULL>" ]] || false
    doltsqloutput=$output
    run dolt table select test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "<NULL>" ]] || false
    [ "$output" = "$doltsqloutput" ]
    # Make sure we don't get a table with no spaces because that bug was 
    # generated when making changes to NULL printing
    [[ ! "$output" =~ "|||||" ]] || false
}

@test "using dolt sql to select rows with NULL values" {
    dolt table put-row test pk:0
    dolt table put-row test pk:1
    dolt table put-row test pk:2 c1:0 c2:0 c3:0 c4:0 c5:0
    run dolt sql -q "select * from test where c1 is null"
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]
    [[ "$output" =~ "<NULL>" ]] || false
}