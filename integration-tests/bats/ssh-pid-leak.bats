#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash
load $BATS_TEST_DIRNAME/helper/git-ssh-common.bash

# Regression tests for https://github.com/dolthub/dolt/issues/10897
# Verifies that `dolt sql-server` does not leak ssh child processes
# when CALL dolt_fetch / dolt_pull / dolt_push runs against an ssh://
# remote. Each remote db must be closed at the end of the procedure,
# tearing down the spawned ssh subprocess.

setup() {
    skiponwindows "ssh transport tests are not supported on Windows"
    if ! command -v sshd >/dev/null 2>&1 && [[ ! -x /usr/sbin/sshd ]]; then
        skip "sshd not available on this host"
    fi

    cd "$BATS_TEST_TMPDIR"

    setup_git_sshd
    gen_ssh_key "$BATS_TEST_TMPDIR/ssh_key" ""

    export DOLT_SSH_COMMAND="ssh -i $BATS_TEST_TMPDIR/ssh_key -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    export DOLT_SSH_EXEC_PATH="$(command -v dolt)"

    mkdir "$BATS_TEST_TMPDIR/remote"
    (
        cd "$BATS_TEST_TMPDIR/remote"
        dolt init
    )

    REMOTE_URL="ssh://$(whoami)@127.0.0.1:${SSHD_PORT}${BATS_TEST_TMPDIR}/remote"
    export REMOTE_URL
}

teardown() {
    stop_sql_server
    teardown_git
    unset DOLT_SSH_COMMAND DOLT_SSH_EXEC_PATH REMOTE_URL
}

count_ssh_kids() {
    local parent="$1"
    ps -eo pid,ppid,comm --no-headers 2>/dev/null \
        | awk -v p="$parent" '$2==p && $3=="ssh"' \
        | wc -l
}

# wait_for_no_ssh_kids polls count_ssh_kids until it reports 0 or a short
# deadline elapses, returning the final count. Close() is synchronous from
# the SQL caller's perspective but the kernel may take a brief moment to
# fully reap the exited child.
wait_for_no_ssh_kids() {
    local parent="$1"
    local i
    for (( i = 0; i < 50; i++ )); do
        local n
        n="$(count_ssh_kids "$parent")"
        if [ "$n" -eq 0 ]; then
            echo 0
            return
        fi
        sleep 0.1
    done
    count_ssh_kids "$parent"
}

@test "ssh-pid-leak: single fetch leaves no orphan ssh process" {
    dolt clone "$REMOTE_URL" repo_local
    cd repo_local
    start_sql_server
    server_pid="$SERVER_PID"

    dolt --host=127.0.0.1 --port=$PORT --no-tls sql -q "use repo_local; call dolt_fetch('origin')"

    remaining="$(wait_for_no_ssh_kids "$server_pid")"
    [ "$remaining" -eq 0 ] || {
        echo "expected 0 ssh subprocesses after fetch, got $remaining" >&2
        ps -eo pid,ppid,stat,comm --no-headers | awk -v p="$server_pid" '$2==p' >&2
        false
    }
}

@test "ssh-pid-leak: 10 sequential fetches do not accumulate" {
    dolt clone "$REMOTE_URL" repo_local
    cd repo_local
    start_sql_server
    server_pid="$SERVER_PID"

    for i in 1 2 3 4 5 6 7 8 9 10; do
        dolt --host=127.0.0.1 --port=$PORT --no-tls sql -q "use repo_local; call dolt_fetch('origin')"
    done

    remaining="$(wait_for_no_ssh_kids "$server_pid")"
    [ "$remaining" -eq 0 ] || {
        echo "expected 0 ssh subprocesses after 10 fetches, got $remaining" >&2
        false
    }
}

@test "ssh-pid-leak: push leaves no orphan ssh process" {
    dolt clone "$REMOTE_URL" repo_local
    cd repo_local
    dolt sql -q "create table t (id int primary key)"
    dolt sql -q "call dolt_commit('-Am', 'add table')"
    start_sql_server
    server_pid="$SERVER_PID"

    dolt --host=127.0.0.1 --port=$PORT --no-tls sql -q "use repo_local; call dolt_push('origin', 'main')"

    remaining="$(wait_for_no_ssh_kids "$server_pid")"
    [ "$remaining" -eq 0 ]
}

@test "ssh-pid-leak: pull leaves no orphan ssh process" {
    dolt clone "$REMOTE_URL" repo_local
    cd repo_local
    start_sql_server
    server_pid="$SERVER_PID"

    dolt --host=127.0.0.1 --port=$PORT --no-tls sql -q "use repo_local; call dolt_pull('origin', 'main')"

    remaining="$(wait_for_no_ssh_kids "$server_pid")"
    [ "$remaining" -eq 0 ]
}

@test "ssh-pid-leak: mixed fetch+pull+push do not accumulate" {
    dolt clone "$REMOTE_URL" repo_local
    cd repo_local
    start_sql_server
    server_pid="$SERVER_PID"

    dolt --host=127.0.0.1 --port=$PORT --no-tls sql -q "use repo_local; call dolt_fetch('origin')"
    dolt --host=127.0.0.1 --port=$PORT --no-tls sql -q "use repo_local; call dolt_pull('origin', 'main')"
    dolt --host=127.0.0.1 --port=$PORT --no-tls sql -q "use repo_local; create table t (id int primary key); call dolt_commit('-Am', 'add table');"
    dolt --host=127.0.0.1 --port=$PORT --no-tls sql -q "use repo_local; call dolt_push('origin', 'main')"
    dolt --host=127.0.0.1 --port=$PORT --no-tls sql -q "use repo_local; call dolt_fetch('origin')"

    remaining="$(wait_for_no_ssh_kids "$server_pid")"
    [ "$remaining" -eq 0 ]
}

@test "ssh-pid-leak: concurrent fetches all clean up" {
    dolt clone "$REMOTE_URL" repo_local
    cd repo_local
    start_sql_server
    server_pid="$SERVER_PID"

    pids=()
    for i in 1 2 3 4 5; do
        dolt --host=127.0.0.1 --port=$PORT --no-tls sql -q "use repo_local; call dolt_fetch('origin')" &
        pids+=($!)
    done
    for p in "${pids[@]}"; do wait $p; done

    remaining="$(wait_for_no_ssh_kids "$server_pid")"
    [ "$remaining" -eq 0 ]
}
