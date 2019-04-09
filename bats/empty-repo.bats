setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

# Tests on an empty dolt repository
@test "dolt init on an already initialized repository" {
    run dolt init
    [ "$status" -ne 0 ]
    [ "$output" = "This directory has already been initialized." ]
}

@test "dolt status on a new repository" {
    run dolt status
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "On branch master" ]
    [ "${lines[1]}" = "nothing to commit, working tree clean" ]
}

@test "dolt ls in a new repository" {
    run dolt ls
    [ "$status" -eq 0 ]
    [ "$output" = "No tables in working set" ]
}

@test "dolt branch in a new repository" {
    run dolt branch
    [ "$status" -eq 0 ]
    # I can't seem to get this to match "* master" so I made a regex instead
    # [ "$output" = "* master" ]
    [[ "$output" =~ "* master" ]] || false
}

@test "dolt log in a new repository" {
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "commit " ]] || false
    [[ "$output" =~ "Data repository created." ]] || false
}

@test "dolt add . in new repository" {
    run dolt add .
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "dolt reset in new repository" {
    run dolt reset
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "dolt diff in new repository" {
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "dolt commit with nothing added" {
    run dolt commit -m "commit"
    [ "$status" -eq 1 ]
    [ "$output" = 'no changes added to commit (use "dolt add")' ]
}

@test "dolt commit --allow-empty with nothing added" {
    run dolt commit -m "distinctively-named commit" --allow-empty
    [ "$status" -eq 0 ]
    run dolt log
    [[ "$output" =~ "distinctively-named commit" ]] || false
}

@test "dolt sql in a new repository" {
   run dolt sql
   [ "$status" -eq 1 ]
   [ "${lines[0]}" = "usage: dolt sql [options] -q query_string" ]
   run dolt sql -q "select * from test"
   [ "$status" -eq 1 ]
   [ "$output" = "error: unknown table 'test'" ]
}

@test "invalid sql in a new repository" {
    run dolt sql -q "foo bar"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error parsing SQL" ]] || false
}

@test "dolt table schema in new repository" {
    run dolt table schema
    [ "$status" -eq 1 ]
    [[ "$output" =~ "usage" ]] || false
}

@test "dolt table select in new repository" {
    run dolt table select test
    [ "$status" -ne 0 ]
    [ "$output" = "error: unknown table 'test'" ]
}

@test "dolt table import in a new repository" {
    run dolt table import
    [ "$status" -ne 0 ]
    [[ "${lines[0]}" =~ "usage" ]] || false
}

@test "dolt table export in a new repository" {
    run dolt table export
    [ "$status" -ne 0 ]
    [[ "${lines[0]}" =~ "usage" ]] || false
}

@test "dolt table rm in a new repository" {
    run dolt table rm 
    [ "$status" -ne 0 ]
    [[ "${lines[0]}" =~ "usage" ]] || false
}

@test "dolt table cp in a new repository" {
    run dolt table cp
    [ "$status" -ne 0 ]
    [[ "${lines[0]}" =~ "usage" ]] || false
}

@test "dolt table put-row in a new repository" {
    run dolt table put-row
    [ "$status" -ne 0 ]
    [[ "${lines[0]}" =~ "usage" ]] || false
}

@test "dolt table rm-row in a new repository" {
    run dolt table rm-row
    [ "$status" -ne 0 ]
    [[ "${lines[0]}" =~ "usage" ]] || false
}

@test "dolt checkout master on master" {
    run dolt checkout master
    [ "$status" -eq 1 ]
    [ "$output" = "Already on branch 'master'" ]
}

@test "dolt checkout non-existant branch" {
    run dolt checkout foo
    [ "$status" -ne 0 ]
    [ "$output" = "error: could not find foo" ]
}

@test "create and checkout a branch" {
    run dolt branch test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt checkout test
    [ "$status" -eq 0 ]
    [ "$output" = "Switched to branch 'test'" ]
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* test" ]] || false
}

@test "create and checkout a branch with dolt checkout -b" {
    run dolt checkout -b test
    [ "$status" -eq 0 ]
    [ "$output" = "Switched to branch 'test'" ]
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* test" ]] || false
}

@test "delete a branch" {
    dolt branch test
    run dolt branch -d test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch
    [[ ! "$output" =~ "test" ]] || false
}

@test "move a branch" {
    dolt branch foo
    run dolt branch -m foo bar
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch
    [[ ! "$output" =~ "foo" ]] || false
    [[ "$output" =~ "bar" ]] || false
}

@test "copy a branch" {
    dolt branch foo
    run dolt branch -c foo bar
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt branch
    [[ "$output" =~ "foo" ]] || false
    [[ "$output" =~ "bar" ]] || false
}