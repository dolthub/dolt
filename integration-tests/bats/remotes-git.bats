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
    mkdir "dolt-repo-clones"
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

    local remote_abs
    remote_abs="$(cd "$remote_git_dir" && pwd)"

    local seed_dir
    seed_dir="$(mktemp -d "${BATS_TMPDIR:-/tmp}/seed-repo.XXXXXX")"

    (
        set -euo pipefail
        trap 'rm -rf "$seed_dir"' EXIT
        cd "$seed_dir"

        git init >/dev/null
        git config user.email "bats@email.fake"
        git config user.name "Bats Tests"
        echo "seed" > README
        git add README
        git commit -m "seed" >/dev/null
        git branch -M "$branch"
        git remote add origin "$remote_abs"
        git push origin "$branch" >/dev/null
    )
}

@test "remotes-git: smoke push/clone/push-back/pull" {
    mkdir remote.git
    git init --bare remote.git
    seed_git_remote_branch remote.git main

    mkdir repo1
    cd repo1
    dolt init
    dolt sql -q "create table test(pk int primary key, v int);"
    dolt add .
    dolt commit -m "create table"

    dolt remote add origin ../remote.git
    run dolt push --set-upstream origin main
    [ "$status" -eq 0 ]

    cd ..
    cd dolt-repo-clones
    run dolt clone ../remote.git repo2
    [ "$status" -eq 0 ]

    cd repo2
    dolt sql -q "insert into test values (1, 10);"
    dolt add .
    dolt commit -m "add row"
    run dolt push origin main
    [ "$status" -eq 0 ]

    cd ../../repo1
    run dolt pull
    [ "$status" -eq 0 ]

    run dolt sql -q "select v from test where pk = 1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "10" ]] || false

}

@test "remotes-git: empty remote bootstrap creates refs/dolt/data" {
    mkdir remote.git
    git init --bare remote.git
    seed_git_remote_branch remote.git main

    # Assert the dolt data ref doesn't exist yet.
    run git --git-dir remote.git show-ref refs/dolt/data
    [ "$status" -eq 1 ]

    mkdir repo1
    cd repo1
    dolt init
    dolt sql -q "create table test(pk int primary key);"
    dolt add .
    dolt commit -m "create table"

    dolt remote add origin ../remote.git
    run dolt push --set-upstream origin main
    [ "$status" -eq 0 ]

    run git --git-dir ../remote.git show-ref refs/dolt/data
    [ "$status" -eq 0 ]

}

@test "remotes-git: pull also fetches branches from git remote" {
    mkdir remote.git
    git init --bare remote.git
    seed_git_remote_branch remote.git main

    mkdir repo1
    cd repo1
    dolt init
    dolt remote add origin ../remote.git
    dolt push origin main

    cd ..
    cd dolt-repo-clones
    run dolt clone ../remote.git repo2
    [ "$status" -eq 0 ]

    cd repo2
    run dolt branch -va
    [[ "$output" =~ "main" ]] || false
    [[ ! "$output" =~ "other" ]] || false

    cd ../../repo1
    dolt checkout -b other
    dolt commit --allow-empty -m "first commit on other"
    dolt push origin other

    cd ../dolt-repo-clones/repo2
    dolt pull
    run dolt branch -va
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "other" ]] || false

}

@test "remotes-git: pull fetches but does not merge other branches" {
    mkdir remote.git
    git init --bare remote.git
    seed_git_remote_branch remote.git main

    mkdir repo1
    cd repo1
    dolt init
    dolt remote add origin ../remote.git
    dolt push --set-upstream origin main
    dolt checkout -b other
    dolt commit --allow-empty -m "first commit on other"
    dolt push --set-upstream origin other

    cd ..
    cd dolt-repo-clones
    run dolt clone ../remote.git repo2
    [ "$status" -eq 0 ]

    cd repo2
    main_state1=$(get_head_commit)

    run dolt pull
    [ "$status" -eq 0 ]

    main_state2=$(get_head_commit)
    [[ "$main_state1" = "$main_state2" ]] || false

    run dolt branch -va
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "other" ]] || false

    run dolt checkout other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "branch 'other' set up to track 'origin/other'." ]] || false

    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "first commit on other" ]] || false

}

@test "remotes-git: custom --ref writes to configured dolt data ref" {
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
    run dolt push --set-upstream origin main
    [ "$status" -eq 0 ]

    run git --git-dir ../remote.git show-ref refs/dolt/custom
    [ "$status" -eq 0 ]
    run git --git-dir ../remote.git show-ref refs/dolt/data
    [ "$status" -ne 0 ]

    cd ..
    cd dolt-repo-clones
    run dolt clone --ref refs/dolt/custom ../remote.git repo2
    [ "$status" -eq 0 ]

    cd repo2
    run dolt sql -q "select v from test where pk = 1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "111" ]] || false

    run git --git-dir ../../remote.git show-ref refs/dolt/data
    [ "$status" -ne 0 ]

}

@test "remotes-git: push works with per-repo git cache under .dolt/" {
    mkdir remote.git
    git init --bare remote.git
    seed_git_remote_branch remote.git main

    mkdir repo1
    cd repo1
    dolt init
    dolt commit --allow-empty -m "init"

    dolt remote add origin ../remote.git
    run dolt push --set-upstream origin main
    [ "$status" -eq 0 ]
}
