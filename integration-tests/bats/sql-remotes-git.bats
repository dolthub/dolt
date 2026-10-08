#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

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
    stop_sql_server 1
    assert_feature_version
    teardown_common
}

@test "sql-remotes-git: sql-server isolates databases sharing a URL with different refs" {
    # Regression for https://github.com/dolthub/dolt/issues/12011.
    git init --bare remote.git
    seed_git_remote_branch remote.git main
    local remote_dir="$PWD/remote.git"
    local remote_url="git+file://$remote_dir"

    mkdir srv client
    for db in db1 db2; do
        mkdir "srv/$db"
        (
            cd "srv/$db"
            dolt init
            dolt sql -q "create table t_$db(id int primary key);
                insert into t_$db values (1);
                call dolt_commit('-Am', '$db initial');"
        )
    done

    cd srv
    start_sql_server db1 server.log
    # Run clients outside the server's data directory to avoid database locks.
    cd ../client
    local client_args=(--host 127.0.0.1 --port "$PORT" --user root --password "" --no-tls)

    dolt "${client_args[@]}" --use-db db1 sql -q "
        call dolt_remote('add', '--ref', 'refs/dolt/db1', 'origin', '$remote_url');
        call dolt_push('origin', 'main');"
    local db1_head
    db1_head=$(git --git-dir "$remote_dir" rev-parse refs/dolt/db1)

    dolt "${client_args[@]}" --use-db db2 sql -q "
        call dolt_remote('add', '--ref', 'refs/dolt/db2', 'origin', '$remote_url');"
    # Fetching an absent ref succeeds without fetching any branches from db1.
    run dolt "${client_args[@]}" --use-db db2 sql -r csv -q "call dolt_fetch('origin');"
    [ "$status" -eq 0 ]
    [ "$output" = $'status\n0' ]
    run dolt "${client_args[@]}" --use-db db2 sql -r csv -q "select count(*) from dolt_remote_branches;"
    [ "$status" -eq 0 ]
    local fetched_branch_count="${lines[1]}"

    run dolt "${client_args[@]}" --use-db db2 sql -q "call dolt_push('--force', 'origin', 'main');"
    [ "$status" -eq 0 ]

    # Check the remote itself, not the server's potentially shared cached handle.
    run git --git-dir "$remote_dir" rev-parse refs/dolt/db1
    [ "$status" -eq 0 ]
    [ "$output" = "$db1_head" ]
    run git --git-dir "$remote_dir" show-ref --verify refs/dolt/db2
    [ "$status" -eq 0 ]
    [ "$fetched_branch_count" = "0" ]

    for db in db1 db2; do
        dolt clone --ref "refs/dolt/$db" "$remote_url" "$db"
        run dolt --data-dir "$db" sql -r csv -q "show tables;"
        [ "$status" -eq 0 ]
        [ "${#lines[@]}" -eq 2 ]
        [ "${lines[1]}" = "t_$db" ]
        run dolt --data-dir "$db" sql -r csv -q "select id from t_$db;"
        [ "$status" -eq 0 ]
        [ "$output" = $'id\n1' ]
    done

    # Independent clients advance each ref, then the long-lived server must
    # fetch the right update through its already-cached remote handles.
    for db in db2 db1; do
        dolt --data-dir "$db" sql -q "
            insert into t_$db values (2);
            call dolt_commit('-Am', 'external update');
            call dolt_push('origin', 'main');"
        pull_until_rows "$db" "t_$db" $'id\n1\n2'
    done
}

@test "sql-remotes-git: one database isolates remotes sharing a URL with different refs" {
    git init --bare remote.git
    seed_git_remote_branch remote.git main
    local remote_url="git+file://$PWD/remote.git"

    # A single SQL process keeps the provider cache alive across both pushes.
    dolt sql -q "
        create table t1(id int primary key);
        insert into t1 values (1);
        call dolt_commit('-Am', 'first table');
        call dolt_remote('add', '--ref', 'refs/dolt/first', 'first', '$remote_url');
        call dolt_push('first', 'main');
        create table t2(id int primary key);
        insert into t1 values (2);
        call dolt_commit('-Am', 'second table');
        call dolt_remote('add', '--ref', 'refs/dolt/second', 'second', '$remote_url');
        call dolt_push('second', 'main');
        call dolt_fetch('second');
        call dolt_fetch('first');"

    run dolt sql -r csv -q "select count(*) from t1 as of hashof('first/main');"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "1" ]
    run dolt sql -r csv -q "select count(*) from t1 as of hashof('second/main');"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = "2" ]

    dolt clone --ref refs/dolt/first "$remote_url" first
    dolt clone --ref refs/dolt/second "$remote_url" second
    run dolt --data-dir first sql -r csv -q 'show tables;'
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    [ "${lines[1]}" = "t1" ]
    run dolt --data-dir second sql -r csv -q 'show tables;'
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [ "${lines[1]}" = "t1" ]
    [ "${lines[2]}" = "t2" ]
}

@test "sql-remotes-git: sql-server uses default refs for databases with separate repositories" {
    # Related cache-isolation coverage for https://github.com/dolthub/dolt/issues/12011.
    local base="$PWD"
    mkdir srv client
    for db in db1 db2; do
        git init --bare "$db.git"
        seed_git_remote_branch "$db.git" main
        mkdir "srv/$db"
        (
            cd "srv/$db"
            dolt init
            dolt sql -q "create table t_$db(id int primary key);
                insert into t_$db values (1);
                call dolt_commit('-Am', 'initial');"
        )
    done

    cd srv
    start_sql_server db1 server.log
    cd ../client
    local client_args=(--host 127.0.0.1 --port "$PORT" --user root --password "" --no-tls)
    for db in db1 db2; do
        dolt "${client_args[@]}" --use-db "$db" sql -q "
            call dolt_remote('add', 'origin', 'git+file://$base/$db.git');
            call dolt_push('origin', 'main');"
        run git --git-dir "$base/$db.git" show-ref --verify refs/dolt/data
        [ "$status" -eq 0 ]
        dolt clone "git+file://$base/$db.git" "$db"
        run dolt --data-dir "$db" sql -r csv -q 'show tables;'
        [ "$status" -eq 0 ]
        [ "${#lines[@]}" -eq 2 ]
        [ "${lines[1]}" = "t_$db" ]
    done

    for db in db2 db1; do
        dolt --data-dir "$db" sql -q "
            insert into t_$db values (2);
            call dolt_commit('-Am', 'external update');
            call dolt_push('origin', 'main');"
        pull_until_rows "$db" "t_$db" $'id\n1\n2'
    done
}

@test "sql-remotes-git: sql-server databases sharing the default ref exchange updates" {
    git init --bare remote.git
    seed_git_remote_branch remote.git main
    local remote_url="git+file://$PWD/remote.git"
    dolt sql -q "create table t(id int primary key);
        insert into t values (1);
        call dolt_commit('-Am', 'initial');
        call dolt_remote('add', 'origin', '$remote_url');
        call dolt_push('origin', 'main');"

    mkdir srv client
    dolt clone "$remote_url" srv/db1
    dolt clone "$remote_url" srv/db2
    cd srv
    start_sql_server db1 server.log
    cd ../client
    local client_args=(--host 127.0.0.1 --port "$PORT" --user root --password "" --no-tls)
    # Populate both caches before either database changes the shared destination.
    for db in db1 db2; do
        dolt "${client_args[@]}" --use-db "$db" sql -q "call dolt_fetch('origin');"
    done
    dolt "${client_args[@]}" --use-db db1 sql -q "
        insert into t values (2);
        call dolt_commit('-Am', 'db1 update');
        call dolt_push('origin', 'main');"
    pull_until_rows db2 t $'id\n1\n2'
    dolt "${client_args[@]}" --use-db db2 sql -q "
        insert into t values (3);
        call dolt_commit('-Am', 'db2 update');
        call dolt_push('origin', 'main');"
    pull_until_rows db1 t $'id\n1\n2\n3'
    for db in db1 db2; do
        run dolt "${client_args[@]}" --use-db "$db" sql -r csv -q 'select id from t order by id;'
        [ "$status" -eq 0 ]
        [ "$output" = $'id\n1\n2\n3' ]
    done
    dolt clone "$remote_url" check
    run dolt --data-dir check sql -r csv -q 'select id from t order by id;'
    [ "$status" -eq 0 ]
    [ "$output" = $'id\n1\n2\n3' ]
}

@test "sql-remotes-git: replacing a remote ref does not reuse the old destination" {
    git init --bare remote.git
    seed_git_remote_branch remote.git main
    local remote_url="git+file://$PWD/remote.git"
    # Reuse both the remote name and URL within one provider lifetime. Also
    # exercise implicit and explicit spellings of the default ref.
    dolt sql -q "
        create table t(id int primary key);
        insert into t values (1);
        call dolt_commit('-Am', 'default ref');
        call dolt_remote('add', 'origin', '$remote_url');
        call dolt_push('origin', 'main');
        call dolt_remote('remove', 'origin');
        call dolt_remote('add', '--ref', 'refs/dolt/custom', 'origin', '$remote_url');
        insert into t values (2);
        call dolt_commit('-Am', 'custom ref');
        call dolt_push('origin', 'main');
        call dolt_remote('remove', 'origin');
        call dolt_remote('add', '--ref', 'refs/dolt/data', 'origin', '$remote_url');
        call dolt_fetch('origin');"
    run dolt sql -r csv -q "select id from t as of hashof('origin/main') order by id;"
    [ "$status" -eq 0 ]
    [ "$output" = $'id\n1' ]
    dolt clone "$remote_url" default_clone
    dolt clone --ref refs/dolt/custom "$remote_url" custom_clone
    run dolt --data-dir default_clone sql -r csv -q 'select id from t order by id;'
    [ "$status" -eq 0 ]
    [ "$output" = $'id\n1' ]
    run dolt --data-dir custom_clone sql -r csv -q 'select id from t order by id;'
    [ "$status" -eq 0 ]
    [ "$output" = $'id\n1\n2' ]
}

pull_until_rows() {
    local db="$1" table="$2" expected="$3"
    local attempt
    # GitBlobstore deduplicates reads for one second. Allow that window to
    # expire after a different client pushes, without an unconditional sleep.
    for attempt in {1..50}; do
        run dolt "${client_args[@]}" --use-db "$db" sql -q "call dolt_pull('origin', 'main');"
        [ "$status" -eq 0 ] || return 1
        run dolt "${client_args[@]}" --use-db "$db" sql -r csv -q "select id from $table order by id;"
        [ "$status" -eq 0 ] || return 1
        if [ "$output" = "$expected" ]; then
            return 0
        fi
        sleep 0.1
    done
    echo "Expected rows in $db.$table: $expected; got: $output"
    return 1
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
