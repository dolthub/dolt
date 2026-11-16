#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/git-ssh-common.bash

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
    teardown_git_wrapper
    teardown_git_ssh
    teardown_git_repo
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
    [[ "$output" =~ "hint: dolt does not support interactive credential prompts" ]] || false
    [[ "$output" =~ "terminal prompts disabled" ]] || false
}

@test "remotes-git: scp-style relative path is not converted to absolute (#10564)" {
    # Regression test for https://github.com/dolthub/dolt/issues/10564
    # When a user adds a remote as `git@host:relative/repo.git` (SCP-style,
    # relative path), the path must NOT become absolute when passed to git.
    # Bug: dolt was converting this to `ssh://git@host/relative/repo.git`
    # which git interprets as absolute path `/relative/repo.git`.

    install_fake_git_url_recorder

    old_path="$PATH"
    export PATH="$BATS_TMPDIR/fakebin:$PATH"
    export DOLT_IGNORE_LOCK_FILE=1

    mkdir repo1
    cd repo1
    dolt init
    dolt commit --allow-empty -m "init"
    dolt remote add origin git@myhost:relative/repo.git

    # Trigger CreateDB → ensureGitRemoteURL → git remote add (in internal cache repo).
    # The push will ultimately fail (fake host), but we only need the remote-add to fire.
    run dolt push origin main

    export PATH="$old_path"

    # The fake git recorded the URL it received for `git remote add`.
    [ -f "$BATS_TMPDIR/recorded_remote_urls" ]
    recorded_url=$(tail -1 "$BATS_TMPDIR/recorded_remote_urls")

    # The URL passed to git must NOT convert the relative SCP path into an
    # absolute SSH path.  `ssh://git@myhost/relative/repo.git` is wrong because
    # the leading `/` makes git send `git-upload-pack '/relative/repo.git'` to
    # the server — an absolute filesystem path.
    #
    # Acceptable forms:
    #   git@myhost:relative/repo.git          (SCP-style, preserves relative)
    #   ssh://git@myhost/./relative/repo.git  (explicit relative marker)
    if [[ "$recorded_url" != "git@myhost:relative/repo.git" && \
          "$recorded_url" != "ssh://git@myhost/./relative/repo.git" ]]; then
        echo "BUG: Unexpected URL passed to git remote add: $recorded_url"
        echo "Expected one of:"
        echo "  git@myhost:relative/repo.git"
        echo "  ssh://git@myhost/./relative/repo.git"
        echo "Got: $recorded_url"
        false
    fi
}

_init_repo_with_remote() {
    mkdir repo1
    cd repo1
    dolt init
    dolt commit --allow-empty -m "init"
    dolt remote add origin "$1"
}

@test "remotes-git: GIT_SSH_COMMAND set by user is not clobbered" {
    # See https://github.com/dolthub/dolt/issues/10811.
    setup_git_repo
    setup_git_ssh_wrapper
    hook_git_ssh_record_env "GIT_SSH_COMMAND"
    _init_repo_with_remote "git@localhost:${GIT_REMOTE_DIR}"

    run dolt push origin main
    [ "$status" -eq 0 ]
    [ -f "$BATS_TEST_TMPDIR/git_env_GIT_SSH_COMMAND" ]
}

# bats test_tags=no_lambda
@test "remotes-git: ssh passphrase prompt is blocked and returns normalized error" {
    # See https://github.com/dolthub/dolt/issues/10811.
    setup_git_repo
    setup_git_sshd
    gen_ssh_key "$BATS_TMPDIR/ssh_key_locked" "test_passphrase"
    export GIT_SSH_COMMAND="ssh -i $BATS_TMPDIR/ssh_key_locked -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    unset SSH_AUTH_SOCK
    _init_repo_with_remote "git+ssh://$(whoami)@127.0.0.1:${SSHD_PORT}${GIT_REMOTE_DIR}"

    # expect allocates a real PTY for the dolt process so the test can verify
    # that git subprocesses cannot reach it to prompt for a passphrase.
    run expect "$BATS_TEST_DIRNAME/remotes-git-ssh-prompt.expect" "Enter passphrase"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "hint: dolt does not support interactive credential prompts" ]] || false
}

# bats test_tags=no_lambda
@test "remotes-git: host key prompt is blocked and returns error" {
    # See https://github.com/dolthub/dolt/issues/10811.
    setup_git_repo
    setup_git_sshd
    gen_ssh_key "$BATS_TMPDIR/ssh_key_unlocked" ""
    # An empty known_hosts file causes SSH to default to StrictHostKeyChecking=ask
    # and prompt for host key confirmation. expect verifies the prompt cannot reach
    # the controlling terminal.
    touch "$BATS_TMPDIR/ssh_known_hosts_empty"
    export GIT_SSH_COMMAND="ssh -i $BATS_TMPDIR/ssh_key_unlocked -o IdentitiesOnly=yes -o UserKnownHostsFile=$BATS_TMPDIR/ssh_known_hosts_empty"
    _init_repo_with_remote "git+ssh://$(whoami)@127.0.0.1:${SSHD_PORT}${GIT_REMOTE_DIR}"

    run expect "$BATS_TEST_DIRNAME/remotes-git-ssh-prompt.expect" "The authenticity of host"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Host key verification failed" ]] || false

    export GIT_SSH_COMMAND="ssh -i $BATS_TMPDIR/ssh_key_unlocked -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    run dolt push origin main
    [ "$status" -eq 0 ]
}

# bats test_tags=no_lambda
@test "remotes-git: ssh push without passphrase prompt succeeds" {
    setup_git_repo
    setup_git_sshd
    gen_ssh_key "$BATS_TMPDIR/ssh_key_unlocked" ""
    export GIT_SSH_COMMAND="ssh -i $BATS_TMPDIR/ssh_key_unlocked -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    _init_repo_with_remote "git+ssh://$(whoami)@127.0.0.1:${SSHD_PORT}${GIT_REMOTE_DIR}"

    run dolt push origin main
    [ "$status" -eq 0 ]
}


install_fake_git_url_recorder() {
    # Fake git that records the URL passed to `git remote add` so tests can
    # inspect whether SCP-style relative paths survive normalization intact.
    # Behaves like install_fake_git_auth_failure but also writes the remote-add
    # URL to a well-known file for later assertion.
    mkdir -p "$BATS_TMPDIR/fakebin"
    cat > "$BATS_TMPDIR/fakebin/git" <<'FAKEGIT'
#!/usr/bin/env bash
set -euo pipefail

git_dir=""
if [[ "${1:-}" == "--git-dir" ]]; then
  git_dir="${2:-}"
  shift 2
fi

cmd="${1:-}"
shift || true

case "$cmd" in
  init)
    if [[ "${1:-}" == "--bare" ]]; then
      mkdir -p "$git_dir"
      exit 0
    fi
    ;;
  remote)
    sub="${1:-}"; shift || true
    case "$sub" in
      get-url)
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
        shift || true # consume --
        name="${1:-}"; url="${2:-}"
        mkdir -p "$git_dir"
        printf "%s" "$url" > "${git_dir}/remote_${name}_url"
        # Record URL for test assertions.
        printf "%s\n" "$url" >> "${BATS_TMPDIR}/recorded_remote_urls"
        exit 0
        ;;
    esac
    ;;
  ls-remote)
    # Return a dummy branch so ensureRemoteHasBranches succeeds, but the
    # subsequent fetch will fail.  That's fine — we only need the remote-add
    # URL to be recorded.
    echo "0000000000000000000000000000000000000000	refs/heads/main"
    exit 0
    ;;
  fetch)
    echo "fatal: could not connect to 'myhost'" >&2
    exit 128
    ;;
esac

echo "fatal: unknown command" >&2
exit 128
FAKEGIT
    chmod +x "$BATS_TMPDIR/fakebin/git"
    rm -f "$BATS_TMPDIR/recorded_remote_urls"
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
    # git --git-dir <dir> init --bare
    if [[ "${1:-}" == "--bare" ]]; then
      mkdir -p "$git_dir"
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

# seed_table_with_many_chunks fills a Dolt table with enough distinct rows to
# produce a table file of hundreds of storage chunks. Runs in the current
# repository.
# Arguments:
#   $1  table name, defaults to t
#   $2  value added to every primary key, so separate calls produce distinct
#       chunks rather than identical ones that the store would dedupe
seed_table_with_many_chunks() {
    local table="${1:-t}"
    local pk_offset="${2:-0}"
    local csv_file
    csv_file="$(mktemp "${BATS_TEST_TMPDIR:-$BATS_TMPDIR}/dolt-many-chunks-XXXXXX.csv")"
    printf 'pk,a,b,c\n' > "$csv_file"
    # awk represents numbers as doubles, so a hex conversion of a value above
    # 2^32 saturates and the columns stop varying. Taking the product mod 2^32
    # keeps each column distinct per row so the rows do not dedupe into a few
    # chunks.
    awk -v offset="$pk_offset" 'BEGIN { modulus = 4294967296; for (i = 1; i <= 30000; i++) printf "%d,%064x,%064x,%064x\n", i + offset, (i*2654435761)%modulus, (i*2246822519)%modulus, (i*3266489917)%modulus }' >> "$csv_file"
    dolt table import -c -pk pk "$table" "$csv_file"
    rm -f "$csv_file"
    dolt add .
    dolt commit -m "import many rows into $table"
}

# assert_catfile_calls_under runs the given dolt command under a git wrapper that
# counts cat-file calls and asserts it makes fewer than the given maximum. It
# first checks that the remote holds a large table, since the count proves
# nothing otherwise. Run it from the directory where the command should execute.
# Arguments:
#   $1  maximum number of git cat-file calls allowed
#   $@  (rest) the dolt command to run, for example dolt clone <url> <dir>
assert_catfile_calls_under() {
    local max_calls="$1"
    shift
    local data_blob_size
    data_blob_size="$(largest_data_blob_size "$GIT_REMOTE_DIR")"
    [ -n "$data_blob_size" ] || {
        echo "No data blob found in the remote. Check that the push wrote a table file."
        false
    }
    [ "$data_blob_size" -gt 100000 ] || {
        echo "Largest data blob is only $data_blob_size bytes, too few chunks to test."
        echo "Raise the row count in seed_table_with_many_chunks."
        false
    }

    setup_git_wrapper
    hook_git_count_subcommand cat-file
    run "$@"
    log_status_eq 0

    local catfile_calls
    catfile_calls="$(git_subcommand_count cat-file)"
    [ "$catfile_calls" -lt "$max_calls" ] || {
        echo "git cat-file ran ${catfile_calls} times (limit ${max_calls})."
        echo "Reading each chunk must not start its own git process."
        false
    }
}

@test "remotes-git: clone from git remote uses O(1) git cat-file calls" {
    # See https://github.com/dolthub/dolt/issues/11236
    setup_git_repo
    _init_repo_with_remote "git+file://$GIT_REMOTE_DIR"
    seed_table_with_many_chunks
    run dolt push --set-upstream origin main
    [ "$status" -eq 0 ]
    src_root=$(dolt sql -q "select dolt_hashof_db()" -r csv | tail -n 1)
    cd ..

    assert_catfile_calls_under 50 \
        dolt clone --depth 1 "git+file://$GIT_REMOTE_DIR" dolt-repo-clones/repo2

    cd dolt-repo-clones/repo2
    run dolt sql -q "select dolt_hashof_db()" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$src_root" ]] || false
    run dolt sql -q "select count(*) from t" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "30000" ]] || false
}

@test "remotes-git: fetch from git remote uses O(1) git cat-file calls" {
    # See https://github.com/dolthub/dolt/issues/11236
    setup_git_repo
    _init_repo_with_remote "git+file://$GIT_REMOTE_DIR"
    seed_table_with_many_chunks
    run dolt push --set-upstream origin main
    [ "$status" -eq 0 ]
    cd ..

    cd dolt-repo-clones
    run dolt clone "git+file://$GIT_REMOTE_DIR" repo2
    [ "$status" -eq 0 ]
    cd ..

    # Push a second large table so the fetch has new chunks to read.
    cd repo1
    seed_table_with_many_chunks t2 1000000
    run dolt push origin main
    [ "$status" -eq 0 ]
    src_root=$(dolt sql -q "select dolt_hashof_db()" -r csv | tail -n 1)
    cd ..

    cd dolt-repo-clones/repo2
    assert_catfile_calls_under 50 dolt fetch origin

    run dolt merge origin/main
    [ "$status" -eq 0 ]
    run dolt sql -q "select dolt_hashof_db()" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$src_root" ]] || false
    run dolt sql -q "select count(*) from t2" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "30000" ]] || false
}
