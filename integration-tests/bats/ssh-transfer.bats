#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

# Tests for SSH transfer operations (clone, pull, push via SSH URLs)
# Uses mock SSH to avoid requiring an SSH server

setup() {
    cd "$BATS_TEST_TMPDIR"
    
    # Create mock SSH script that executes the remote command locally.
    # Real SSH runs its last argument as a command on the remote host.
    # buildTransferCommand always passes the dolt transfer invocation as
    # the final argument, so we just exec that.  All args are logged so
    # tests can verify user@host, -p port, etc.
    cat > "$BATS_TEST_TMPDIR/mock_ssh" <<'EOF'
#!/bin/bash
echo "$@" >> "$BATS_TEST_TMPDIR/mock_ssh.log"
COMMAND="${@: -1}"
exec $COMMAND 2> >(tee -a "$BATS_TEST_TMPDIR/transfer_stderr.log" >&2)
EOF
    chmod +x "$BATS_TEST_TMPDIR/mock_ssh"

    export DOLT_SSH_COMMAND="$BATS_TEST_TMPDIR/mock_ssh"
}

teardown() {
  unset DOLT_SSH_COMMAND
  stop_sql_server
}

@test "ssh-transfer: transfer command works with --data-dir" {
    mkdir "repo1"
    cd "repo1"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY, val TEXT);"
    dolt sql -q "INSERT INTO test VALUES (1, 'hello');"
    dolt add .
    dolt commit -m "initial commit"
    
    cd ..
    run dolt --data-dir="repo1" transfer </dev/null
    # transfer command is kind of strange in that it always exits with status 0. It will even show an odd error
    # here. That's all fine. We don't expect people to run this directly. We'll look at messages more closely from
    # dolt clone, dolt push, dolt pull.
    [ "$status" -eq 0 ]
    [[ "$output" =~ "server error: EOF" ]] || false
}

@test "ssh-transfer: clone via SSH URL" {
    mkdir "repo_source"
    cd "repo_source"
    dolt init
    dolt sql -q "CREATE TABLE products (id VARCHAR(36) DEFAULT (UUID()) PRIMARY KEY, name TEXT, price DECIMAL(10,2));"
    dolt sql -q "INSERT INTO products (name, price) VALUES ('Widget', 19.99), ('Gadget', 29.99);"
    dolt add .
    dolt commit -m "initial data"
    
    cd ..
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_source" repo_clone
    [ "$status" -eq 0 ]
    [ -d repo_clone ]
    
    cd repo_clone
    run dolt sql -q "SELECT COUNT(*) FROM products;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false
}

@test "ssh-transfer: clone with .dolt suffix in URL path" {
    mkdir "repo_dotsuffix"
    cd "repo_dotsuffix"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1), (2), (3);"
    dolt add .
    dolt commit -m "test data"

    cd ..
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_dotsuffix/.dolt" repo_dotsuffix_clone
    [ "$status" -eq 0 ]
    [ -d repo_dotsuffix_clone ]

    cd repo_dotsuffix_clone
    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false
    cd "$BATS_TEST_TMPDIR"
    rm -rf repo_dotsuffix_clone

    # repeat with /.dolt/ (trailing slash).
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_dotsuffix/.dolt/" repo_dotsuffix_clone
    [ "$status" -eq 0 ]
    [ -d repo_dotsuffix_clone ]

    cd repo_dotsuffix_clone
    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false
}

@test "ssh-transfer: clone with user@host format" {
    mkdir "repo_usertest"
    cd "repo_usertest"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1), (2), (3);"
    dolt add .
    dolt commit -m "test data"
    
    cd ..
    run dolt clone "ssh://testuser@localhost$BATS_TEST_TMPDIR/repo_usertest" repo_clone_user
    [ "$status" -eq 0 ]
    [ -d repo_clone_user ]

    # Verify mock SSH received testuser@localhost
    grep -q "testuser@localhost" "$BATS_TEST_TMPDIR/mock_ssh.log"

    cd repo_clone_user
    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false
}

@test "ssh-transfer: clone with custom port in URL" {
    mkdir "repo_porttest"
    cd "repo_porttest"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1), (2), (3);"
    dolt add .
    dolt commit -m "test data"

    cd ..
    run dolt clone "ssh://localhost:9999$BATS_TEST_TMPDIR/repo_porttest" repo_port_clone
    [ "$status" -eq 0 ]
    [ -d repo_port_clone ]

    # Verify -p 9999 was passed to mock SSH
    grep -q "\-p 9999" "$BATS_TEST_TMPDIR/mock_ssh.log"

    cd repo_port_clone
    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false
}

@test "ssh-transfer: clone of GCed repo" {
    mkdir "repo_gc"
    cd "repo_gc"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1), (2), (3);"
    dolt add .
    dolt commit -m "test data"
    dolt gc

    cd ..
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_gc" repo_gc_clone
    [ "$status" -eq 0 ]
    [ -d repo_gc_clone ]

    cd repo_gc_clone
    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false
}

@test "ssh-transfer: pull changes from remote" {
    mkdir "repo_pull_source"
    cd "repo_pull_source"
    dolt init
    dolt sql -q "CREATE TABLE items (id INT PRIMARY KEY, name TEXT);"
    dolt sql -q "INSERT INTO items VALUES (1, 'item1');"
    dolt add .
    dolt commit -m "initial"
    
    cd ..
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_pull_source" repo_pull_clone
    [ "$status" -eq 0 ]
    
    cd "repo_pull_source"
    dolt sql -q "INSERT INTO items VALUES (2, 'item2');"
    dolt add .
    dolt commit -m "add item2"
    
    cd "../repo_pull_clone"
    run dolt pull origin
    [ "$status" -eq 0 ]
    
    run dolt sql -q "SELECT COUNT(*) FROM items;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false
}

@test "ssh-transfer: push changes to remote" {
    mkdir "repo_push_source"
    cd "repo_push_source"
    dolt init
    dolt sql -q "CREATE TABLE data (id INT PRIMARY KEY, val TEXT);"
    dolt sql -q "INSERT INTO data VALUES (1, 'original');"
    dolt add .
    dolt commit -m "initial"
    
    cd ..
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_push_source" repo_push_clone
    [ "$status" -eq 0 ]
    
    cd "repo_push_clone"
    dolt sql -q "INSERT INTO data VALUES (2, 'from_clone');"
    dolt add .
    dolt commit -m "add from clone"
    
    # Record the commit we expect to push
    PUSH_COMMIT=$(dolt log --oneline -n 1 | awk '{print $1}')

    run dolt push origin main
    [ "$status" -eq 0 ]

    # After a successful push, the local tracking ref origin/main should
    # match the commit we just pushed.
    run dolt log --oneline -n 1 origin/main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$PUSH_COMMIT" ]] || false
}

@test "ssh-transfer: handle branch operations" {
    mkdir "repo_branch_source"
    cd "repo_branch_source"
    dolt init
    dolt sql -q "CREATE TABLE main_table (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO main_table VALUES (1);"
    dolt add .
    dolt commit -m "main commit"
    
    dolt checkout -b feature
    dolt sql -q "CREATE TABLE feature_table (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO feature_table VALUES (1);"
    dolt add .
    dolt commit -m "feature commit"
    dolt checkout main
    
    cd ..
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_branch_source" repo_branch_clone
    [ "$status" -eq 0 ]
    
    cd "repo_branch_clone"
    run dolt fetch origin feature
    [ "$status" -eq 0 ]
    
    run dolt checkout feature
    [ "$status" -eq 0 ]
    
    run dolt sql -q "SHOW TABLES;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "feature_table" ]] || false
}

@test "ssh-transfer: concurrent clone operations" {
    mkdir "repo_concurrent_source"
    cd "repo_concurrent_source"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1), (2), (3), (4), (5);"
    dolt add .
    dolt commit -m "test data"
    
    # Start multiple clones concurrently
    cd ..
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_concurrent_source" repo_concurrent_1 &
    PID1=$!
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_concurrent_source" repo_concurrent_2 &
    PID2=$!
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_concurrent_source" repo_concurrent_3 &
    PID3=$!
    
    # Wait for all to complete
    wait $PID1
    STATUS1=$?
    wait $PID2
    STATUS2=$?
    wait $PID3
    STATUS3=$?
    
    [ "$STATUS1" -eq 0 ]
    [ "$STATUS2" -eq 0 ]
    [ "$STATUS3" -eq 0 ]
    
    # Verify all have correct data
    for dir in repo_concurrent_1 repo_concurrent_2 repo_concurrent_3; do
        cd "$dir"
        run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
        [ "$status" -eq 0 ]
        [[ "$output" =~ "5" ]] || false
        cd ..
    done
}

@test "ssh-transfer: error handling for non-existent repository" {
    run dolt clone "ssh://localhost/nonexistent/repo" should_fail
    [ "$status" -ne 0 ]
    [[ "$output" =~ "repository not found" ]] || false
}

@test "ssh-transfer: verify DOLT_SSH_COMMAND environment variable works" {
    # All tests use DOLT_SSH_COMMAND, so this is kind of a sanity check to ensure that your can change it.
    cat > "$BATS_TEST_TMPDIR/custom_ssh" <<'EOF'
#!/bin/bash
echo "CUSTOM_SSH $@" >> "$BATS_TEST_TMPDIR/custom_ssh.log"
COMMAND="${@: -1}"
exec $COMMAND 2>/dev/null
EOF
    chmod +x "$BATS_TEST_TMPDIR/custom_ssh"
    export DOLT_SSH_COMMAND="$BATS_TEST_TMPDIR/custom_ssh"

    mkdir "repo_env_test"
    cd "repo_env_test"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt add .
    dolt commit -m "test"

    cd ..
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_env_test" repo_env_clone
    [ "$status" -eq 0 ]

    # Verify the custom SSH was used
    [ -f "$BATS_TEST_TMPDIR/custom_ssh.log" ]
    grep -q "CUSTOM_SSH.*localhost" "$BATS_TEST_TMPDIR/custom_ssh.log"
}

@test "ssh-transfer: DOLT_SSH_EXEC_PATH overrides remote dolt path" {
    mkdir "repo_exec_path"
    cd "repo_exec_path"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1), (2);"
    dolt add .
    dolt commit -m "test"

    # Create a mock SSH that logs the remote command and rewrites the
    # custom dolt path back to the real binary so it can actually execute.
    export REAL_DOLT_BIN="$(command -v dolt)"
    [ -n "$REAL_DOLT_BIN" ]

    cat > "$BATS_TEST_TMPDIR/mock_ssh_exec" <<'MOCK'
#!/bin/bash
COMMAND="${@: -1}"
echo "$COMMAND" >> "$BATS_TEST_TMPDIR/exec_path.log"
COMMAND=$(echo "$COMMAND" | sed "s|/nonsense/path/to/dolt|$REAL_DOLT_BIN|")
exec $COMMAND
MOCK
    chmod +x "$BATS_TEST_TMPDIR/mock_ssh_exec"

    export DOLT_SSH_COMMAND="$BATS_TEST_TMPDIR/mock_ssh_exec"
    export DOLT_SSH_EXEC_PATH="/nonsense/path/to/dolt"

    cd ..
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_exec_path" repo_exec_clone
    [ "$status" -eq 0 ]

    # Verify the custom exec path was used in the remote command
    [ -f "$BATS_TEST_TMPDIR/exec_path.log" ]
    grep -q "/nonsense/path/to/dolt --data-dir" "$BATS_TEST_TMPDIR/exec_path.log"
}

@test "ssh-transfer: clone succeeds while sql-server is running" {
    mkdir "repo_locked_read"
    cd "repo_locked_read"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY, val TEXT);"
    dolt sql -q "INSERT INTO test VALUES (1, 'one'), (2, 'two'), (3, 'three');"
    dolt add .
    dolt commit -m "initial data"

    start_sql_server "repo_locked_read"

    cd "$BATS_TEST_TMPDIR"
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_locked_read" repo_locked_read_clone
    [ "$status" -eq 0 ]

    cd "repo_locked_read_clone"
    run dolt sql -r csv  -q "SELECT * FROM test;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,one" ]] || false
    [[ "$output" =~ "2,two" ]] || false
    [[ "$output" =~ "3,three" ]] || false
}

@test "ssh-transfer: push fails while sql-server is running" {
    mkdir "repo_locked_push"
    cd "repo_locked_push"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY, val TEXT);"
    dolt sql -q "INSERT INTO test VALUES (1, 'original');"
    dolt add .
    dolt commit -m "initial"

    cd "$BATS_TEST_TMPDIR"
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_locked_push" repo_locked_push_clone
    [ "$status" -eq 0 ]

    # Start sql-server on the source repo to hold the manifest lock
    cd "repo_locked_push"
    start_sql_server "repo_locked_push"

    # Make a change in the clone and try to push
    cd "$BATS_TEST_TMPDIR/repo_locked_push_clone"
    dolt sql -q "INSERT INTO test VALUES (2, 'from_clone');"
    dolt add .
    dolt commit -m "add from clone"

    run dolt push origin main
    # Push must fail when sql-server holds the lock.
    [ "$status" -ne 0 ]
    # The transfer command logs the real error to stderr; verify it.
    grep -q "database is read only" "$BATS_TEST_TMPDIR/transfer_stderr.log"
}

@test "ssh-transfer: sql dolt_clone() via SSH URL" {
    mkdir "repo_sql_clone_src"
    cd "repo_sql_clone_src"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY, val TEXT);"
    dolt sql -q "INSERT INTO test VALUES (1, 'a'), (2, 'b'), (3, 'c');"
    dolt add .
    dolt commit -m "initial"

    # Start sql-server in a fresh directory (no pre-existing databases)
    cd "$BATS_TEST_TMPDIR"
    mkdir "sql_clone_server"
    cd "sql_clone_server"
    start_sql_server

    dolt sql -q "CALL dolt_clone('ssh://localhost$BATS_TEST_TMPDIR/repo_sql_clone_src', 'sql_cloned');"

    # Verify cloned data
    run dolt --use-db sql_cloned sql -r csv -q "SELECT COUNT(*) FROM test;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false
}

@test "ssh-transfer: sql dolt_push() to SSH remote" {
    mkdir "repo_sql_push_src"
    cd "repo_sql_push_src"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1);"
    dolt add .
    dolt commit -m "initial"

    cd "$BATS_TEST_TMPDIR"
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_sql_push_src" repo_sql_push_clone

    cd "repo_sql_push_clone"
    start_sql_server repo_sql_push_clone

    dolt sql -q "INSERT INTO test VALUES (10);"
    dolt sql -q "CALL dolt_commit('-a','-m', 'sql push 123');"
    dolt sql -q "CALL dolt_push('origin', 'main');"
    stop_sql_server 1

    cd "$BATS_TEST_TMPDIR/repo_sql_push_src"
    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "sql push 123" ]] || false
}

@test "ssh-transfer: sql dolt_pull() from SSH remote" {
    mkdir "repo_sql_pull_src"
    cd "repo_sql_pull_src"
    dolt init
    dolt sql -q "CREATE TABLE items (id INT PRIMARY KEY, name TEXT);"
    dolt sql -q "INSERT INTO items VALUES (1, 'original');"
    dolt add .
    dolt commit -m "initial"

    cd "$BATS_TEST_TMPDIR"
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_sql_pull_src" repo_sql_pull_clone

    # Add new data to source
    cd "repo_sql_pull_src"
    dolt sql -q "INSERT INTO items VALUES (99, 'new_item');"
    dolt add .
    dolt commit -m "add new item"

    cd "$BATS_TEST_TMPDIR/repo_sql_pull_clone"
    start_sql_server repo_sql_pull_clone
    dolt sql -q "CALL dolt_pull('origin');"

    run dolt sql -r csv -q "SELECT * FROM items WHERE id=99;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new_item" ]] || false
}

@test "ssh-transfer: sql dolt_remote(add) with SSH URL then push" {
    mkdir "repo_sql_remote_src"
    cd "repo_sql_remote_src"
    dolt init
    dolt sql -q "CREATE TABLE data (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO data VALUES (1), (2), (3);"
    dolt add .
    dolt commit -m "initial data"

    cd "$BATS_TEST_TMPDIR"
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_sql_remote_src" repo_sql_remote_target

    # Create a second clone as the local working copy
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_sql_remote_src" repo_sql_remote_local
    cd "repo_sql_remote_local"

    start_sql_server repo_sql_remote_local
    dolt sql -q "CALL dolt_remote('add', 'target', 'ssh://localhost$BATS_TEST_TMPDIR/repo_sql_remote_target');"
    dolt sql -q "INSERT INTO data VALUES (10);"
    dolt sql -q "CALL dolt_commit('-a', '-m', 'push to target');"
    dolt sql -q "CALL dolt_push('target', 'main');"

    cd "$BATS_TEST_TMPDIR/repo_sql_remote_target"
    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "push to target" ]] || false
}

@test "ssh-transfer: sql dolt_clone() fails for non-existent SSH repo" {
    mkdir "sql_err_server"
    cd "sql_err_server"
    start_sql_server

    # Attempt clone of non-existent path
    run dolt sql -q "CALL dolt_clone('ssh://localhost/nonexistent/repo_does_not_exist');"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "repository not found" ]] || false
}

@test "ssh-transfer: sql dolt_push() fails while sql-server locks target" {
    mkdir "repo_sql_lockpush_src"
    cd "repo_sql_lockpush_src"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1);"
    dolt add .
    dolt commit -m "initial"

    cd "$BATS_TEST_TMPDIR"
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_sql_lockpush_src" repo_sql_lockpush_clone

    # Lock source with sql-server
    cd "repo_sql_lockpush_src"
    start_sql_server repo_sql_lockpush_src

    cd "$BATS_TEST_TMPDIR/repo_sql_lockpush_clone"
    dolt sql -q "INSERT INTO test VALUES (99);"
    dolt sql -q "CALL dolt_commit('-a', '-m', 'should fail');"
    run dolt sql -q "CALL dolt_push('origin', 'main');"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "database is read only" ]] || false
}
@test "ssh-transfer: clone from bare backup repository" {
    mkdir "repo_bare_src"
    cd "repo_bare_src"
    dolt init
    dolt sql -q "CREATE TABLE items (id INT PRIMARY KEY, name TEXT);"
    dolt sql -q "INSERT INTO items VALUES (1, 'alpha'), (2, 'beta');"
    dolt add .
    dolt commit -m "initial bare test"

    dolt backup add bac1 "file://$BATS_TEST_TMPDIR/bare_backup"
    dolt backup sync bac1

    cd "$BATS_TEST_TMPDIR"
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/bare_backup" bare_clone
    [ "$status" -eq 0 ]

    cd "bare_clone"
    run dolt sql -r csv -q "SELECT COUNT(*) FROM items;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    run dolt sql -r csv -q "SELECT name FROM items ORDER BY id;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "alpha" ]] || false
    [[ "$output" =~ "beta" ]] || false
}

@test "ssh-transfer: pull from bare backup repository" {
    mkdir "repo_bare_pull_src"
    cd "repo_bare_pull_src"
    dolt init
    dolt sql -q "CREATE TABLE t (id INT PRIMARY KEY, v TEXT);"
    dolt sql -q "INSERT INTO t VALUES (1, 'first');"
    dolt add .
    dolt commit -m "initial"

    dolt backup add bac1 "file://$BATS_TEST_TMPDIR/bare_pull_backup"
    dolt backup sync bac1

    cd "$BATS_TEST_TMPDIR"

    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/bare_pull_backup" bare_pull_clone

    # Add more data to source and re-sync to the backup.
    cd "repo_bare_pull_src"
    dolt sql -q "INSERT INTO t VALUES (2, 'second');"
    dolt add .
    dolt commit -m "add second row"
    dolt backup sync bac1

    cd "$BATS_TEST_TMPDIR/bare_pull_clone"
    run dolt pull origin
    [ "$status" -eq 0 ]

    run dolt sql -r csv -q "SELECT COUNT(*) FROM t;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false
}

@test "ssh-transfer: push to bare backup repository" {
    mkdir "repo_bare_push_src"
    cd "repo_bare_push_src"
    dolt init
    dolt sql -q "CREATE TABLE widgets (id INT PRIMARY KEY, color TEXT);"
    dolt sql -q "INSERT INTO widgets VALUES (1, 'red');"
    dolt add .
    dolt commit -m "initial"

    dolt backup add bac1 "file://$BATS_TEST_TMPDIR/bare_push_backup"
    dolt backup sync bac1

    cd "$BATS_TEST_TMPDIR"

    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/bare_push_backup" bare_push_clone
    cd "bare_push_clone"
    dolt sql -q "INSERT INTO widgets VALUES (2, 'blue');"
    dolt add .
    dolt commit -m "add blue widget"

    PUSHED_COMMIT=$(dolt log --oneline -n 1 | awk '{print $1}')
    run dolt push origin main
    [ "$status" -eq 0 ]

    cd "$BATS_TEST_TMPDIR"
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/bare_push_backup" bare_push_verify
    cd "bare_push_verify"

    run dolt sql -r csv -q "SELECT COUNT(*) FROM widgets;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$PUSHED_COMMIT" ]] || false
}

@test "ssh-transfer: push to empty directory creates bare repository" {
    mkdir "repo_push_to_empty_src"
    cd "repo_push_to_empty_src"
    dolt init
    dolt sql -q "CREATE TABLE t (id INT PRIMARY KEY, v TEXT);"
    dolt sql -q "INSERT INTO t VALUES (1, 'hello');"
    dolt add .
    dolt commit -m "initial"

    # Create an empty target directory and push to it via SSH.
    mkdir "$BATS_TEST_TMPDIR/empty_bare_target"
    dolt remote add bare "ssh://localhost$BATS_TEST_TMPDIR/empty_bare_target"
    run dolt push bare main
    [ "$status" -eq 0 ]

    # The target should now be a bare repository: NBS files directly in the
    # directory with no .dolt/ subdirectory.
    [ -f "$BATS_TEST_TMPDIR/empty_bare_target/manifest" ] || false
    [ ! -d "$BATS_TEST_TMPDIR/empty_bare_target/.dolt" ] || false

    # Clone from the bare target to verify the data is intact.
    cd "$BATS_TEST_TMPDIR"
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/empty_bare_target" bare_from_push
    [ "$status" -eq 0 ]

    cd "bare_from_push"
    run dolt sql -r csv -q "SELECT v FROM t WHERE id=1;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "hello" ]] || false

    # Restore from the bare repository using dolt backup restore.
    cd "$BATS_TEST_TMPDIR"
    run dolt backup restore "file://$BATS_TEST_TMPDIR/empty_bare_target" bare_restored
    [ "$status" -eq 0 ]

    cd "bare_restored"
    run dolt sql -r csv -q "SELECT v FROM t WHERE id=1;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "hello" ]] || false
}

# assert_no_ssh_subprocess_leak runs |query| |n| times via the sql-server and
# asserts that no transfer subprocesses remain as children of the server.
assert_no_ssh_subprocess_leak() {
    local n="$1" query="$2" i leaked
    for i in $(seq 1 "$n"); do
        dolt --host 127.0.0.1 --port "$PORT" --no-tls --use-db "$DEFAULT_DB" sql -q "$query"
        sleep 1
        leaked=$(pgrep -P "$SERVER_PID" | wc -l | tr -d ' ')
        echo "after call $i: $leaked leaked children (PID $SERVER_PID)"
    done
    sleep 2
    leaked=$(pgrep -P "$SERVER_PID" | wc -l | tr -d ' ')
    [ "$leaked" -eq 0 ]
}

@test "ssh-transfer: dolt_fetch via sql-server does not leak subprocesses" {
    # See https://github.com/dolthub/dolt/issues/10897
    mkdir "repo_leak_remote"
    cd "repo_leak_remote"
    dolt init --initial-branch main
    dolt sql -q "CREATE TABLE t (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO t VALUES (1),(2),(3);"
    dolt add .
    dolt commit -m "init"

    cd "$BATS_TEST_TMPDIR"
    mkdir "repo_leak_local"
    cd "repo_leak_local"
    dolt init --initial-branch main
    start_sql_server "repo_leak_local"

    dolt --host 127.0.0.1 --port "$PORT" --no-tls --use-db "$DEFAULT_DB" sql -q \
        "CALL dolt_remote('add', 'origin', 'ssh://localhost$BATS_TEST_TMPDIR/repo_leak_remote');"

    assert_no_ssh_subprocess_leak 3 "CALL dolt_fetch('origin', 'main');"
}

@test "ssh-transfer: dolt_push via sql-server does not leak subprocesses" {
    # See https://github.com/dolthub/dolt/issues/10897
    mkdir "repo_push_remote"
    cd "repo_push_remote"
    dolt init --initial-branch main
    dolt sql -q "CREATE TABLE t (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO t VALUES (1),(2),(3);"
    dolt add .
    dolt commit -m "init"

    cd "$BATS_TEST_TMPDIR"
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_push_remote" repo_push_local
    cd "repo_push_local"
    dolt sql -q "INSERT INTO t VALUES (4);"
    dolt add .
    dolt commit -m "add row"
    start_sql_server "repo_push_local"

    assert_no_ssh_subprocess_leak 3 "CALL dolt_push('origin', 'main');"
}
