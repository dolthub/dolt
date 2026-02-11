#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    skiponwindows "tests are flaky on Windows"
    skip_if_remote
    setup_common
    if ! command -v git >/dev/null 2>&1; then
        skip "git not installed"
    fi
    cd $BATS_TMPDIR
    cd dolt-repo-$$
}

teardown() {
    assert_feature_version
    teardown_common
}

seed_git_remote_branch() {
    # Create an initial branch on an otherwise-empty bare git remote.
    # Dolt git remotes require at least one git branch to exist on the remote.
    local remote_git_dir="$1"
    local branch="${2:-main}"

    mkdir seed-repo
    cd seed-repo
    git init >/dev/null
    git config user.email "bats@email.fake"
    git config user.name "Bats Tests"
    echo "seed" > README
    git add README
    git commit -m "seed" >/dev/null
    git branch -M "$branch"
    git remote add origin "../$remote_git_dir"
    git push origin "$branch" >/dev/null
    cd ..
    rm -rf seed-repo
}

@test "sql-remotes-git: dolt_remote add supports --ref for git remotes" {
    mkdir remote.git
    git init --bare remote.git
    seed_git_remote_branch remote.git main

    mkdir repo1
    cd repo1
    dolt init
    dolt sql -q "create table test(pk int primary key, v int);"
    dolt sql -q "insert into test values (1, 111);"
    dolt add .
    dolt commit -m "seed"

    run dolt sql <<SQL
CALL dolt_remote('add', '--ref', 'refs/dolt/custom', 'origin', '../remote.git');
CALL dolt_push('origin', 'main');
SQL
    [ "$status" -eq 0 ]

    run git --git-dir ../remote.git show-ref refs/dolt/custom
    [ "$status" -eq 0 ]
    run git --git-dir ../remote.git show-ref refs/dolt/data
    [ "$status" -ne 0 ]
}

@test "sql-remotes-git: dolt_clone supports --ref for git remotes" {
    mkdir remote.git
    git init --bare remote.git
    seed_git_remote_branch remote.git main

    mkdir repo1
    cd repo1
    dolt init
    dolt sql -q "create table test(pk int primary key, v int);"
    dolt sql -q "insert into test values (1, 111);"
    dolt add .
    dolt commit -m "seed"

    dolt remote add --ref refs/dolt/custom origin ../remote.git
    dolt push --set-upstream origin main

    cd ..
    mkdir host
    cd host
    dolt init

    run dolt sql -q "call dolt_clone('--ref', 'refs/dolt/custom', '../remote.git', 'repo2');"
    [ "$status" -eq 0 ]

    cd repo2
    run dolt sql -q "select v from test where pk = 1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "111" ]] || false

    run git --git-dir ../../remote.git show-ref refs/dolt/custom
    [ "$status" -eq 0 ]
    run git --git-dir ../../remote.git show-ref refs/dolt/data
    [ "$status" -ne 0 ]
}

@test "sql-remotes-git: dolt_backup sync-url supports --ref for git remotes" {
    mkdir remote.git
    git init --bare remote.git
    seed_git_remote_branch remote.git main

    mkdir repo1
    cd repo1
    dolt init
    dolt sql -q "create table test(pk int primary key, v int);"
    dolt sql -q "insert into test values (1, 111);"
    dolt add .
    dolt commit -m "seed"

    run dolt sql -q "call dolt_backup('sync-url', '--ref', 'refs/dolt/custom', '../remote.git');"
    [ "$status" -eq 0 ]

    run git --git-dir ../remote.git show-ref refs/dolt/custom
    [ "$status" -eq 0 ]
    run git --git-dir ../remote.git show-ref refs/dolt/data
    [ "$status" -ne 0 ]
}

