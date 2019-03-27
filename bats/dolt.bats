#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir dolt-repo
    cd dolt-repo
}

teardown() {
    rm -rf $BATS_TMPDIR/dolt-repo
}

@test "checking we have a dolt executable available" {
    command -v dolt
}

@test "invoking dolt with no arguments" {
    run dolt
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "Valid commands for dolt are" ]
    # Check help output for supported commands
    [[ "$output" =~ "init -" ]]
    [[ "$output" =~ "status -" ]]
    [[ "$output" =~ "add -" ]]
    [[ "$output" =~ "reset -" ]]
    [[ "$output" =~ "commit -" ]]
    [[ "$output" =~ "sql -" ]]
    [[ "$output" =~ "log -" ]]
    [[ "$output" =~ "diff -" ]]
    [[ "$output" =~ "merge -" ]]
    [[ "$output" =~ "branch -" ]]
    [[ "$output" =~ "checkout -" ]]
    [[ "$output" =~ "remote -" ]]
    [[ "$output" =~ "push -" ]]
    [[ "$output" =~ "pull -" ]]
    [[ "$output" =~ "fetch -" ]]
    [[ "$output" =~ "clone -" ]]
    [[ "$output" =~ "creds -" ]]
    [[ "$output" =~ "login -" ]]
    [[ "$output" =~ "version -" ]]
    [[ "$output" =~ "config -" ]]
    [[ "$output" =~ "ls -" ]]
    [[ "$output" =~ "table -" ]]
    [[ "$output" =~ "conflicts -" ]]
}

@test "testing dolt version output" {
    run dolt version
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt version " ]] 
}


# Tests for dolt commands outside of a dolt repository
NOT_VALID_REPO_ERROR="The current directory is not a valid dolt repository."
@test "dolt status outside of a dolt repository" {
    run dolt status
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt add outside of a dolt repository" {
    run dolt add
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt reset outside of a dolt repository" {
    run dolt reset
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt commit outside of a dolt repository" {
    run dolt commit
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt log outside of a dolt repository" {
    run dolt log
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt diff outside of a dolt repository" {
    run dolt diff
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt merge outside of a dolt repository" {
    run dolt merge
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt branch outside of a dolt repository" {
    run dolt branch
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt checkout outside of a dolt repository" {
    run dolt checkout
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt remote outside of a dolt repository" {
    run dolt remote
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt push outside of a dolt repository" {
    run dolt push
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt pull outside of a dolt repository" {
    run dolt pull
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt fetch outside of a dolt repository" {
    run dolt fetch
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt ls outside of a dolt repository" {
    run dolt ls
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "$NOT_VALID_REPO_ERROR" ]
}

@test "dolt table outside of a dolt repository" {
    run dolt table
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "Valid commands for dolt table are" ]
    # Check help output for supported commands
    [[ "$output" =~ "import -" ]]
    [[ "$output" =~ "export -" ]]
    [[ "$output" =~ "create -" ]]
    [[ "$output" =~ "rm -" ]]
    [[ "$output" =~ "mv -" ]]
    [[ "$output" =~ "cp -" ]]
    [[ "$output" =~ "select -" ]]
    [[ "$output" =~ "schema -" ]]
    [[ "$output" =~ "put-row -" ]]
    [[ "$output" =~ "rm-row -" ]]
}

@test "dolt conflicts outside of a dolt repository" {
    run dolt conflicts
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "Valid commands for dolt conflicts are" ]
    # Check help output for supported commands
    [[ "$output" =~ "cat -" ]]
    [[ "$output" =~ "resolve -" ]]
}

# Tests on an empty dolt repository
@test "initializing a dolt repository" {
    run dolt init
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully initialized dolt data repository." ]
    [ -d .dolt ]
    [ -d .dolt/noms ]
    [ -f .dolt/config.json ] 
    [ -f .dolt/repo_state.json ]
}

@test "dolt init on an already initialized repository" {
    dolt init
    run dolt init
    [ "$status" -ne 0 ]
    [ "$output" = "This directory has already been initialized." ]
}

@test "dolt status on a new repository" {
    dolt init
    run dolt status
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "On branch master" ]
    [ "${lines[1]}" = "nothing to commit, working tree clean" ]
}

@test "dolt ls in a new repository" {
    dolt init
    run dolt ls
    [ "$status" -eq 0 ]
    [ "$output" = "Tables in working set:" ]
}

@test "dolt branch in a new repository" {
    dolt init 
    run dolt branch
    [ "$status" -eq 0 ]
    # I can't seem to get this to match "* master" so I made a regex instead
    # [ "$output" = "* master" ] 
    [[ "$output" =~ "* master" ]]
}

@test "dolt log in a new repository" {
    dolt init
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "commit " ]]
    [[ "$output" =~ "Data repository created." ]]
}

@test "dolt add . in new repository" {
    dolt init
    run dolt add .
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "dolt reset in new repository" {
    dolt init
    run dolt reset
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "dolt diff in new repository" {
    dolt init
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "dolt commit with nothing added" {
    dolt init 
    skip "This should fail. Currently succeeds and adds to the log."
    run dolt commit -m "commit"
    [ "$status" -eq 1 ]
    [ "$output" = "" ]
}

@test "dolt checkout master on master" {
    dolt init 
    run dolt checkout master 
    [ "$status" -eq 0 ]
    skip "Should say Already on branch 'master'. Says Switched to branch 'master'"
    [ "$output" = "Already on branch 'master'" ]
}

@test "dolt checkout non-existant branch" {
    dolt init
    run dolt checkout foo
    [ "$status" -ne 0 ]
    [ "$output" = "error: could not find foo" ]
}

@test "create and checkout a branch" {
    dolt init
    run dolt branch test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt checkout test
    [ "$status" -eq 0 ]
    [ "$output" = "Switched to branch 'test'" ]
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* test" ]]
}

@test "delete a branch" {
    dolt init
    dolt branch test
    run dolt branch -d test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch
    [[ ! "$output" =~ "test" ]]
}

@test "move a branch" {
    dolt init
    dolt branch test
    run dolt branch -m test test2
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch
    [[ ! "$output" =~ "test" ]]
    [[ "$output" =~ "test2" ]]
}

@test "copy a branch" {
    dolt init
    dolt branch test
    run dolt branch -c test test2
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch
    [[ "$output" =~ "test" ]]
    [[ "$output" =~ "test2" ]]
}

# Create a single primary key table and do stuff
@test "create a table with a schema file and examine repo" {
    dolt init
    run dolt table create -s=$BATS_TEST_DIRNAME/1pk5col.schema test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "test" ]]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "$output" = "pk|c1|c2|c3|c4|c5" ]
    run dolt diff
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "diff --dolt a/test b/test" ]
    [ "${lines[1]}" = "added table" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]]
    [[ "$output" =~ new[[:space:]]table:[[:space:]]+test ]]
}

@test "create a table, dolt add, dolt reset, and dolt commit" {
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/1pk5col.schema test
    run dolt add test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed" ]]
    [[ "$output" =~ new[[:space:]]table:[[:space:]]+test ]]
    run dolt reset test 
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]]
    [[ "$output" =~ new[[:space:]]table:[[:space:]]+test ]]
    run dolt add .
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed" ]]
    [[ "$output" =~ new[[:space:]]table:[[:space:]]+test ]]
    run dolt commit -m "test commit"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]]
}

@test "add a row to a created table using dolt table put-row" {
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/1pk5col.schema test
    dolt add test
    dolt commit -m "create table"
    run dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+0[[:space:]]+\|[[:space:]]+1 ]]
}

@test "dolt checkout branch and table name collision" {
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/1pk5col.schema test
    dolt branch test
    run dolt checkout test
    [ "$status" -eq 0 ]
    skip "behavior ambiguous right now. should reset test table and switch to branch per git"
}

@test "make a change on a different branch, commit, and merge to master" {
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/1pk5col.schema test
    dolt branch test-branch
    dolt checkout test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]]
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added test row" ]]
}

@test "generate a merge conflict and resolve" {
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/1pk5col.schema test
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
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+ours[[:space:]]+\| ]]
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+theirs[[:space:]]+\| ]]
    run dolt conflicts resolve --ours test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Cnf" ]]
    [[ ! "$output" =~ "!" ]]
    dolt add test
    dolt commit -m "merged and resolved conflict"
    run dolt log
    [[ "$output" =~ "added test row" ]]
    [[ "$output" =~ "added conflicting row" ]]
    [[ "$output" =~ "merged and resolved conflict" ]]
    [[ "$output" =~ "Merge:" ]]
}

@test "put a row that violates the schema" {
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/1pk5col.schema test
    run dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:foo
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "inserted row does not match schema" ]
}

@test "import data from a csv file after table created" {
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/1pk5col.schema test
    run dolt table import test $BATS_TEST_DIRNAME/1pk5col.csv
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "import data from csv and create the table" {
    dolt init 
    run dolt table import -c --pk=pk test $BATS_TEST_DIRNAME/1pk5col.csv
        [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "import data from a psv file after table created" {
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/1pk5col.schema test
    run dolt table import test $BATS_TEST_DIRNAME/1pk5col.psv
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "import data from psv and create the table" {
    dolt init
    run dolt table import -c --pk=pk test $BATS_TEST_DIRNAME/1pk5col.psv
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "overwrite a row. make sure it actually updates not inserts" {
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/1pk5col.schema test
    dolt table import test $BATS_TEST_DIRNAME/1pk5col.csv
    run dolt table put-row test pk:1 c1:2 c2:4 c3:6 c4:8 c5:10
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "add row on two different branches. no merge conflict" {
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/1pk5col.schema test
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
    [[ "$output" =~ "Updating" ]]
    [[ ! "$output" =~ "CONFLICT" ]]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "add column, no merge conflict" {
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/1pk5col.schema test
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt table schema --add-field test c6 int 
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5 c6:6
    dolt add test
    dolt commit -m "added test row with one more column"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]]
    [[ ! "$output" =~ "CONFLICT" ]]
    run dolt diff 
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6" ]]
}

@test "modify different fields, same row, no merge conflict" {
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/1pk5col.schema test
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
    [[ "$output" =~ "Updating" ]]
    [[ ! "$output" =~ "CONFLICT" ]]
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "10" ]]
}