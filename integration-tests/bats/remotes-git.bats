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

@test "remotes-git: uninitialized bare git remote (no branches) errors clearly" {
    mkdir remote.git
    git init --bare remote.git

    mkdir repo1
    cd repo1
    dolt init
    dolt commit --allow-empty -m "init"

    dolt remote add origin ../remote.git
    run dolt push --set-upstream origin main
    [ "$status" -ne 0 ]
    [[ "$output" =~ "initialize the repository with an initial branch/commit first" ]] || false
}

@test "remotes-git: remote add --ref cannot be empty" {
    mkdir remote.git
    git init --bare remote.git
    seed_git_remote_branch remote.git main

    mkdir repo1
    cd repo1
    dolt init

    run dolt remote add --ref "" origin ../remote.git
    [ "$status" -ne 0 ]
    [[ "$output" =~ "error: --ref cannot be empty" ]] || false
}

@test "remotes-git: remote add --ref is rejected for non-git remotes" {
    mkdir non-git-remote
    mkdir remote.git
    git init --bare remote.git
    seed_git_remote_branch remote.git main

    mkdir repo1
    cd repo1
    dolt init

    run dolt remote add --ref refs/dolt/custom origin file://../non-git-remote
    [ "$status" -ne 0 ]
    [[ "$output" =~ "--ref is only supported for git remotes" ]] || false
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

@test "remotes-git: fetch --prune removes deleted branch from git remote" {
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
    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ "$output" =~ "remotes/origin/other" ]] || false

    # Delete the remote branch using dolt semantics (empty source ref), then prune it locally.
    cd ../../repo1
    run dolt push origin :other
    [ "$status" -eq 0 ]

    cd ../dolt-repo-clones/repo2
    run dolt fetch -p
    [ "$status" -eq 0 ]

    run dolt branch -a
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "remotes/origin/other" ]] || false
    [[ "$output" =~ "remotes/origin/main" ]] || false
}

@test "remotes-git: non-fast-forward push rejected, then force push succeeds" {
    mkdir remote.git
    git init --bare remote.git
    seed_git_remote_branch remote.git main

    mkdir repo1
    cd repo1
    dolt init
    dolt sql -q "create table t(pk int primary key, v int);"
    dolt sql -q "insert into t values (1, 1);"
    dolt add .
    dolt commit -m "seed t"
    dolt remote add origin ../remote.git
    run dolt push --set-upstream origin main
    [ "$status" -eq 0 ]

    cd ..
    cd dolt-repo-clones
    run dolt clone ../remote.git repo2
    [ "$status" -eq 0 ]

    cd repo2
    dolt sql -q "insert into t values (2, 2);"
    dolt add .
    dolt commit -m "repo2 advances main"
    run dolt push origin main
    [ "$status" -eq 0 ]

    cd ../../repo1
    dolt sql -q "insert into t values (3, 3);"
    dolt add .
    dolt commit -m "repo1 diverges"

    run dolt push origin main
    [ "$status" -ne 0 ]
    [[ "$output" =~ "non-fast-forward" ]] || false

    run dolt push -f origin main
    [ "$status" -eq 0 ]

    cd ../dolt-repo-clones
    run dolt clone ../remote.git repo3
    [ "$status" -eq 0 ]

    cd repo3
    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "repo1 diverges" ]] || false
}

@test "remotes-git: pull from git remote produces data conflict; resolve and complete merge" {
    mkdir remote.git
    git init --bare remote.git
    seed_git_remote_branch remote.git main

    mkdir repo1
    cd repo1
    dolt init
    dolt sql -q "create table t(pk int primary key, v int);"
    dolt sql -q "insert into t values (1, 0);"
    dolt add .
    dolt commit -m "base"
    dolt remote add origin ../remote.git
    dolt push --set-upstream origin main

    cd ..
    cd dolt-repo-clones
    run dolt clone ../remote.git repo2
    [ "$status" -eq 0 ]

    cd repo2
    dolt sql -q "update t set v = 200 where pk = 1;"
    dolt add .
    dolt commit -m "repo2 local edit"

    cd ../../repo1
    dolt sql -q "update t set v = 100 where pk = 1;"
    dolt add .
    dolt commit -m "repo1 remote edit"
    dolt push origin main

    cd ../dolt-repo-clones/repo2
    run dolt pull
    [ "$status" -ne 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "unmerged" ]] || false

    run dolt conflicts cat t
    [ "$status" -eq 0 ]

    run dolt conflicts resolve --theirs t
    [ "$status" -eq 0 ]

    dolt add t
    dolt commit -m "resolve conflict"

    run dolt sql -q "select v from t where pk = 1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "100" ]] || false

    # Push the resolution back to the remote, then ensure another clone can pull it.
    run dolt push origin main
    [ "$status" -eq 0 ]

    cd ../../repo1
    run dolt pull
    [ "$status" -eq 0 ]
    run dolt sql -q "select v from t where pk = 1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "100" ]] || false
}

@test "remotes-git: pull from git remote produces schema conflict; resolve via abort+align+rerepull" {
    mkdir remote.git
    git init --bare remote.git
    seed_git_remote_branch remote.git main

    mkdir repo1
    cd repo1
    dolt init
    dolt sql -q "create table t(pk int primary key, c0 int);"
    dolt add .
    dolt commit -m "base"
    dolt remote add origin ../remote.git
    dolt push --set-upstream origin main

    cd ..
    cd dolt-repo-clones
    run dolt clone ../remote.git repo2
    [ "$status" -eq 0 ]

    cd repo2
    dolt sql -q "alter table t modify c0 datetime(6);"
    dolt add .
    dolt commit -m "repo2 schema change"

    cd ../../repo1
    dolt sql -q "alter table t modify c0 varchar(20);"
    dolt add .
    dolt commit -m "repo1 schema change"
    dolt push origin main

    cd ../dolt-repo-clones/repo2
    run dolt pull
    [ "$status" -ne 0 ]
    [[ "$output" =~ "CONFLICT (schema)" ]] || false

    run dolt conflicts cat .
    [ "$status" -eq 0 ]
    [[ "$output" =~ "varchar(20)" ]] || false
    [[ "$output" =~ "datetime(6)" ]] || false

    # Work around current schema conflict resolution limitations:
    # abort merge, align schemas, then pull again.
    run dolt merge --abort
    [ "$status" -eq 0 ]

    dolt sql -q "alter table t modify c0 varchar(20);"
    dolt add .
    dolt commit -m "align schema with remote"

    run dolt pull
    [ "$status" -eq 0 ]

    run dolt schema show t
    [ "$status" -eq 0 ]
    [[ "$output" =~ "varchar(20)" ]] || false

    # Push the resolved history and ensure the other clone can pull it.
    run dolt push origin main
    [ "$status" -eq 0 ]

    cd ../../repo1
    run dolt pull
    [ "$status" -eq 0 ]
    run dolt schema show t
    [ "$status" -eq 0 ]
    [[ "$output" =~ "varchar(20)" ]] || false
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

@test "remotes-git: non-interactive git auth fails fast with normalized error" {
    if ! command -v timeout >/dev/null 2>&1; then
        skip "timeout not installed"
    fi

    install_fake_git_auth_failure

    old_path="$PATH"
    export PATH="$BATS_TMPDIR/fakebin:$PATH"
    export DOLT_IGNORE_LOCK_FILE=1

    mkdir repo1
    cd repo1
    dolt init
    dolt commit --allow-empty -m "init"
    dolt remote add origin https://example.com/org/repo.git

    # Ensure the URL was normalized into git-remote-backed form.
    run dolt remote -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ origin[[:blank:]]git[+]https://example.com/org/repo[.]git ]] || false

    # The command must fail quickly (no hang) and present a normalized auth error.
    run timeout 5s dolt push origin main
    export PATH="$old_path"

    [ "$status" -ne 0 ]
    [ "$status" -ne 124 ]
    [[ "$output" =~ "git authentication required but interactive prompting is disabled" ]] || false
    [[ "$output" =~ "terminal prompts disabled" ]] || false
}

install_fake_git_auth_failure() {
    mkdir -p "$BATS_TMPDIR/fakebin"
    cat > "$BATS_TMPDIR/fakebin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

# This fake git binary exists to simulate an auth-required remote where interactive
# prompting is disabled. It implements only the small subset of commands Dolt's
# git-remote-backed plumbing uses for setup, and then fails auth on remote access.

git_dir=""
if [[ "${1:-}" == "--git-dir" ]]; then
  git_dir="${2:-}"
  shift 2
fi

cmd="${1:-}"
shift || true

case "$cmd" in
  init)
    # git init --bare <dir>
    if [[ "${1:-}" == "--bare" ]]; then
      mkdir -p "${2:-}"
      exit 0
    fi
    ;;
  remote)
    sub="${1:-}"; shift || true
    case "$sub" in
      get-url)
        # git remote get-url -- <name>
        shift || true # consume --
        name="${1:-}"
        f="${git_dir}/remote_${name}_url"
        if [[ -f "$f" ]]; then
          cat "$f"
          exit 0
        fi
        echo "fatal: No such remote '$name'" >&2
        exit 2
        ;;
      add|set-url)
        # git remote add/set-url -- <name> <url>
        shift || true # consume --
        name="${1:-}"; url="${2:-}"
        mkdir -p "$git_dir"
        printf "%s" "$url" > "${git_dir}/remote_${name}_url"
        exit 0
        ;;
    esac
    ;;
  ls-remote)
    # Simulate an auth-required scenario where prompting is disabled.
    echo "fatal: could not read Username for 'https://example.com/org/repo.git': terminal prompts disabled" >&2
    exit 128
    ;;
esac

echo "fatal: could not read Username for 'https://example.com/org/repo.git': terminal prompts disabled" >&2
exit 128
EOF
    chmod +x "$BATS_TMPDIR/fakebin/git"
}
