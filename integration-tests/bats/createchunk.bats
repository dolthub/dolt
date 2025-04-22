#! /usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    # get the current root value
    dolt branch initial
    initialCommitHash=$(dolt sql -q "select dolt_hashof('initial')" -r csv | tail -n 1)
    rootValueHash=$(dolt sql -q "SELECT dolt_hashof_db();" -r csv | tail -n 1)
    # create a new commit with some changes
    dolt sql -q "CREATE TABLE test_table(pk INT PRIMARY KEY);"
    dolt commit -Am "create table"

    dolt sql -q "INSERT INTO test_table VALUES (1);"
    dolt commit -Am "insert into table"

    # get the updated root value
    newCommitHash=$(dolt sql -q "select dolt_hashof('HEAD')" -r csv | tail -n 1)
    newRootValueHash=$(dolt sql -q "SELECT dolt_hashof_db();" -r csv | tail -n 1)
}

teardown() {
    teardown_common
}

@test "createchunk: create commit in CLI on new branch" {
    # create a new branch that flattens the commit history
    flattenedCommitHash=$(dolt admin createchunk commit --root "$newRootValueHash" \
      --author "a <b@c.com>" --desc "flattened history" --parents "refs/internal/create" --branch newBranch)

    run dolt show newBranch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Author: a <b@c.com>" ]] || false
    [[ "$output" =~ "flattened history" ]] || false
    # Check that this commit contains both the table creation and insert
    [[ "$output" =~ "added table" ]] || false
    [[ "$output" =~ "| + | 1  |" ]] || false

    # check that there are only two commits in the history
    run dolt log newBranch
    [ "$status" -eq 0 ]
    [ "$(echo "$output" | grep -c commit)" -eq 2 ]
    [[ "$output" =~ "Initialize data repository" ]] || false
    [[ "$output" =~ "flattened history" ]] || false
}

@test "createchunk: force is a no-op when used on a new branch" {
    # create a new branch that flattens the commit history
    flattenedCommitHash=$(dolt admin createchunk commit --root "$newRootValueHash" \
      --author "a <b@c.com>" --desc "flattened history" --parents "refs/internal/create" --branch newBranch --force)

    run dolt show newBranch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Author: a <b@c.com>" ]] || false
    [[ "$output" =~ "flattened history" ]] || false
    [[ "$output" =~ "added table" ]] || false
    [[ "$output" =~ "| + | 1  |" ]] || false

    # check that there are only two commits in the history
    run dolt log newBranch
    [ "$status" -eq 0 ]
    [ "$(echo "$output" | grep -c commit)" -eq 2 ]
    [[ "$output" =~ "Initialize data repository" ]] || false
    [[ "$output" =~ "flattened history" ]] || false

}

@test "createchunk: use default author when none is specified" {
    flattenedCommitHash=$(dolt admin createchunk commit --root "$newRootValueHash" --desc "flattened history" \
      --parents "refs/internal/create" --branch newBranch)
    run dolt show newBranch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Author: Bats Tests <bats@email.fake>" ]] || false
    [[ "$output" =~ "$flattenedCommitHash" ]] || false
}

@test "createchunk: commit with multiple parents" {
    echo "$initialCommitHash,$newCommitHash"
    flattenedCommitHash=$(dolt admin createchunk commit --root "$newRootValueHash" --desc "flattened history" \
      --parents "$initialCommitHash,$newCommitHash" --branch mergeBranch)
    run dolt show mergeBranch
    echo "$output"
    [[ "$output" =~ "Merge: $initialCommitHash $newCommitHash" ]] || false
    [[ "$output" =~ "$flattenedCommitHash" ]] || false
}

@test "createchunk: create commit in CLI on existing branch" {
    dolt branch existingBranch "$initialCommitHash"
    # overwriting an existing branch is allowed if the current commit is a parent of the new onw
    flattenedCommitHash=$(dolt admin createchunk commit --root "$newRootValueHash" --desc "flattened history" \
      --parents "$initialCommitHash" --branch existingBranch)

    run dolt log existingBranch
    [ "$status" -eq 0 ]
    [ "$(echo "$output" | grep -c commit)" -eq 2 ]
    [[ "$output" =~ "Initialize data repository" ]] || false
    [[ "$output" =~ "flattened history" ]] || false

    # but overwriting an existing branch with a different history is an error
    run dolt admin createchunk commit --root "$newRootValueHash" --desc "flattened history" --parents "$newCommitHash" --branch existingBranch
    [ "$status" -eq 1 ]
    [[ "$output" =~ "branch existingBranch already exists. If you wish to overwrite it, add the --force flag" ]] || false

    # but we can make it succeed with --force, overwriting the branch
    overwrittenCommitHash=$(dolt admin createchunk commit --root "$newRootValueHash" --desc "overwritten desc" \
      --parents "$newCommitHash" --branch existingBranch --force)

    run dolt log existingBranch
    [ "$status" -eq 0 ]
    [ "$(echo "$output" | grep -c commit)" -eq 4 ]
    [[ "$output" =~ "Initialize data repository" ]] || false
    [[ "$output" =~ "create table" ]] || false
    [[ "$output" =~ "insert into table" ]] || false
    [[ "$output" =~ "overwritten desc" ]] || false
    [[ ! "$output" =~ "flattened history" ]] || false
}

@test "createchunk: attempt to create commit in CLI with no provided branch" {
    run dolt admin createchunk commit --root "$newRootValueHash" --desc "flattened history" --parents "$initialCommitHash,$newCommitHash"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "the --branch flag is required when creating a chunk using the CLI" ]] || false
}

@test "createchunk: create commit in SQL on existing branch" {
    dolt branch existingBranch
    run dolt sql -q "CALL DOLT_ADMIN_CREATECHUNK_COMMIT('--root', '$newRootValueHash', '--author', 'a <b@c.com>', \
      '--desc', 'flattened history', '--parents', 'refs/internal/create', '--branch', 'existingBranch', '--force');" -r csv
    echo "$output"
    [ "$status" -eq 0 ]
    flattenedCommitHash=$(echo "$output" | tail -n 1)
    run dolt show existingBranch
    echo "$output"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Author: a <b@c.com>" ]] || false
    [[ "$output" =~ "flattened history" ]] || false
    [[ "$output" =~ "added table" ]] || false
    [[ "$output" =~ "| + | 1  |" ]] || false

    # check that there are only two commits in the history
    run dolt log existingBranch
    [ "$status" -eq 0 ]
    [ "$(echo "$output" | grep -c commit)" -eq 2 ]
    [[ "$output" =~ "Initialize data repository" ]] || false
    [[ "$output" =~ "flattened history" ]] || false
}

@test "createchunk: create commit in SQL on new branch" {
    flattenedCommitHash=$(dolt sql -q "CALL DOLT_ADMIN_CREATECHUNK_COMMIT('--root', '$newRootValueHash', '--author', 'a <b@c.com>', '--desc',\
     'flattened history', '--parents', 'refs/internal/create', '--branch', 'newBranch');" -r csv | tail -n 1)

    run dolt show newBranch
    echo "$output"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Author: a <b@c.com>" ]] || false
    [[ "$output" =~ "flattened history" ]] || false
    [[ "$output" =~ "added table" ]] || false
    [[ "$output" =~ "| + | 1  |" ]] || false

    # check that there are only two commits in the history
    run dolt log newBranch
    [ "$status" -eq 0 ]
    [ "$(echo "$output" | grep -c commit)" -eq 2 ]
    [[ "$output" =~ "Initialize data repository" ]] || false
    [[ "$output" =~ "flattened history" ]] || false
}

@test "createchunk: create commit in SQL with no provided branch" {
    run dolt sql -r csv <<SQL
    CALL DOLT_ADMIN_CREATECHUNK_COMMIT('--root', '$newRootValueHash', '--author', 'a <b@c.com>', '--desc',
     'flattened history', '--parents', '$initialCommitHash', '--branch', 'newBranch');
    SELECT * from dolt_log;
SQL
    [ "$status" -eq 0 ]
    # Just capture the last four lines (the select)
    run echo "$(echo "$output" | tail -n 4)"
    echo "$output"
    [[ "${lines[0]}" =~ "commit_hash,committer,email,date,message" ]] || false
    [[ "${lines[1]}" =~ "insert into table" ]] || false
    [[ "${lines[2]}" =~ "create table" ]] || false
    [[ "${lines[3]}" =~ "Initialize data repository" ]] || false
}
