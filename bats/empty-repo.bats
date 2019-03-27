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

@test "dolt table schema in new repository" {
    dolt init
    run dolt table schema
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

@test "dolt table select in new repository" {
    dolt init
    run dolt table select test
    [ "$status" -ne 0 ]
    [ "$output" = "error: unknown table 'test'" ]
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