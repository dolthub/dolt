#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

# Tests for SSH transfer operations (clone, pull, push via SSH URLs)
# Uses mock SSH to avoid requiring an SSH server

setup() {
    cd "$BATS_TEST_TMPDIR"
    
    # Create mock SSH script
    cat > "$BATS_TMPDIR/mock_ssh" <<'EOF'
#!/bin/bash
# Mock SSH script - executes dolt transfer locally
# Parses: mock_ssh [options] [user@]host command args...

COMMAND=""
HOST=""
SKIP_NEXT=false

for arg in "$@"; do
    if [[ "$SKIP_NEXT" == "true" ]]; then
        SKIP_NEXT=false
        continue
    fi
    
    case "$arg" in
        -*)
            # Skip SSH options
            if [[ "$arg" == "-o" ]] || [[ "$arg" == "-i" ]] || [[ "$arg" == "-p" ]]; then
                SKIP_NEXT=true
            fi
            ;;
        *@*)
            # user@host format
            HOST="${arg#*@}"
            ;;
        *)
            if [[ -z "$HOST" ]] && [[ -z "$COMMAND" ]]; then
                # First non-option arg is host
                HOST="$arg"
            else
                # Rest is the command
                if [[ -z "$COMMAND" ]]; then
                    COMMAND="$arg"
                else
                    COMMAND="$COMMAND $arg"
                fi
            fi
            ;;
    esac
done

# Log and execute the command locally with proper stdio
echo "Mock SSH executing: $COMMAND" >> "$BATS_TMPDIR/mock_ssh.log"
# Use exec to replace the shell process and preserve stdio connections.
# Tee stderr to a log file so tests can verify server-side log output.
exec $COMMAND 2> >(tee -a "$BATS_TMPDIR/transfer_stderr.log" >&2)
EOF
    chmod +x "$BATS_TMPDIR/mock_ssh"
    
    # Set environment for SSH operations
    export DOLT_SSH="$BATS_TMPDIR/mock_ssh"
}

teardown() {
  unset DOLT_SSH
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
    # Transfer exits non-zero when stdin closes immediately, but it should
    # start without crashing. Exit code 1 means it ran and shut down cleanly.
    [ "$status" -eq 1 ]
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
    [[ "$output" =~ "2" ]]
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
    [[ "$output" =~ "3" ]]
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
    
    cd repo_clone_user
    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]]
}

@test "ssh-transfer: clone with custom port in URL" {
    mkdir "repo_porttest"
    cd "repo_porttest"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1), (2), (3);"
    dolt add .
    dolt commit -m "test data"

    # Use a mock SSH that logs the -p flag for verification
    cat > "$BATS_TMPDIR/mock_ssh_port" <<'PORTEOF'
#!/bin/bash
echo "$@" >> "$BATS_TMPDIR/ssh_port_args.log"
exec "$BATS_TMPDIR/mock_ssh" "$@"
PORTEOF
    chmod +x "$BATS_TMPDIR/mock_ssh_port"
    export DOLT_SSH="$BATS_TMPDIR/mock_ssh_port"

    cd ..
    run dolt clone "ssh://localhost:9999$BATS_TEST_TMPDIR/repo_porttest" repo_port_clone
    [ "$status" -eq 0 ]
    [ -d repo_port_clone ]

    # Verify -p 9999 was passed to SSH
    grep -q "\-p 9999" "$BATS_TMPDIR/ssh_port_args.log"

    cd repo_port_clone
    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]]
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
    [[ "$output" =~ "2" ]]
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
    [[ "$output" =~ "$PUSH_COMMIT" ]]
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
    [[ "$output" =~ "feature_table" ]]
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
        [[ "$output" =~ "5" ]]
        cd ..
    done
}

@test "ssh-transfer: error handling for non-existent repository" {
    run dolt clone "ssh://localhost/nonexistent/repo" should_fail
    [ "$status" -ne 0 ]
    [[ "$output" =~ "repository not found" ]] || false
}

@test "ssh-transfer: verify DOLT_SSH environment variable works" {
    # Create a custom mock SSH that logs calls
    cat > "$BATS_TMPDIR/mock_ssh_logger" <<'EOF'
#!/bin/bash
echo "CUSTOM_SSH_CALLED" >> "$BATS_TMPDIR/ssh_calls.log"
exec "$BATS_TMPDIR/mock_ssh" "$@"
EOF
    chmod +x "$BATS_TMPDIR/mock_ssh_logger"
    
    export DOLT_SSH="$BATS_TMPDIR/mock_ssh_logger"
    
    mkdir "repo_env_test"
    cd "repo_env_test"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt add .
    dolt commit -m "test"
    
    cd ..
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_env_test" repo_env_clone
    [ "$status" -eq 0 ]
    
    # Verify custom SSH was used
    [ -f "$BATS_TMPDIR/ssh_calls.log" ]
    grep -q "CUSTOM_SSH_CALLED" "$BATS_TMPDIR/ssh_calls.log"
}

@test "ssh-transfer: DOLT_SSH_EXEC_PATH overrides remote dolt path" {
    mkdir "repo_exec_path"
    cd "repo_exec_path"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1), (2);"
    dolt add .
    dolt commit -m "test"

    # Create a mock SSH that logs the remote command it receives
    cat > "$BATS_TMPDIR/mock_ssh_exec" <<'MOCK'
#!/bin/bash
COMMAND=""
HOST=""
SKIP_NEXT=false

for arg in "$@"; do
    if [[ "$SKIP_NEXT" == "true" ]]; then
        SKIP_NEXT=false
        continue
    fi
    case "$arg" in
        -*)
            if [[ "$arg" == "-o" ]] || [[ "$arg" == "-i" ]] || [[ "$arg" == "-p" ]]; then
                SKIP_NEXT=true
            fi
            ;;
        *@*)
            HOST="${arg#*@}"
            ;;
        *)
            if [[ -z "$HOST" ]] && [[ -z "$COMMAND" ]]; then
                HOST="$arg"
            else
                if [[ -z "$COMMAND" ]]; then
                    COMMAND="$arg"
                else
                    COMMAND="$COMMAND $arg"
                fi
            fi
            ;;
    esac
done

# Log the remote command for verification
echo "$COMMAND" >> "$BATS_TMPDIR/exec_path.log"
# Execute locally, replacing the custom path with the real dolt
COMMAND=$(echo "$COMMAND" | sed "s|/custom/path/to/dolt|$(which dolt)|")
exec $COMMAND
MOCK
    chmod +x "$BATS_TMPDIR/mock_ssh_exec"

    export DOLT_SSH="$BATS_TMPDIR/mock_ssh_exec"
    export DOLT_SSH_EXEC_PATH="/custom/path/to/dolt"

    cd ..
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_exec_path" repo_exec_clone
    [ "$status" -eq 0 ]

    # Verify the custom exec path was used in the remote command
    [ -f "$BATS_TMPDIR/exec_path.log" ]
    grep -q "/custom/path/to/dolt --data-dir" "$BATS_TMPDIR/exec_path.log"

    # Verify data is correct
    cd repo_exec_clone
    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]]
}

@test "ssh-transfer: server-side logs are visible on stderr" {
    mkdir "repo_stderr"
    cd "repo_stderr"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1);"
    dolt add .
    dolt commit -m "test"

    cd ..
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_stderr" repo_stderr_clone

    # The mock SSH tees remote stderr to transfer_stderr.log.
    # Verify the transfer command's startup log appeared.
    [ -f "$BATS_TMPDIR/transfer_stderr.log" ]
    grep -q "transfer: serving repository" "$BATS_TMPDIR/transfer_stderr.log"
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
    [[ "$output" =~ "1,one" ]]
    [[ "$output" =~ "2,two" ]]
    [[ "$output" =~ "3,three" ]]
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
    grep -q "database is read only" "$BATS_TMPDIR/transfer_stderr.log"
}

# --- SQL procedure tests: exercise SSH transport via dolt_clone/dolt_push/dolt_pull/dolt_remote ---

@test "ssh-transfer: sql dolt_clone via SSH URL" {
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

    # Clone via SQL procedure
    dolt sql -q "CALL dolt_clone('ssh://localhost$BATS_TEST_TMPDIR/repo_sql_clone_src', 'sql_cloned');"

    # Verify cloned data
    run dolt --use-db sql_cloned sql -r csv -q "SELECT COUNT(*) FROM test;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]]
}

@test "ssh-transfer: sql dolt_push to SSH remote" {
    mkdir "repo_sql_push_src"
    cd "repo_sql_push_src"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1);"
    dolt add .
    dolt commit -m "initial"

    # Clone via CLI to get a working copy with SSH origin
    cd "$BATS_TEST_TMPDIR"
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_sql_push_src" repo_sql_push_clone

    # Start sql-server on the clone
    cd "repo_sql_push_clone"
    start_sql_server repo_sql_push_clone

    # Insert, commit, push via SQL procedures
    dolt sql -q "INSERT INTO test VALUES (10);"
    dolt sql -q "CALL dolt_add('.');"
    dolt sql -q "CALL dolt_commit('-m', 'sql push');"
    dolt sql -q "CALL dolt_push('origin', 'main');"
    stop_sql_server 1

    # Verify push landed in source
    cd "$BATS_TEST_TMPDIR/repo_sql_push_src"
    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "sql push" ]]
}

@test "ssh-transfer: sql dolt_pull from SSH remote" {
    mkdir "repo_sql_pull_src"
    cd "repo_sql_pull_src"
    dolt init
    dolt sql -q "CREATE TABLE items (id INT PRIMARY KEY, name TEXT);"
    dolt sql -q "INSERT INTO items VALUES (1, 'original');"
    dolt add .
    dolt commit -m "initial"

    # Clone via CLI
    cd "$BATS_TEST_TMPDIR"
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_sql_pull_src" repo_sql_pull_clone

    # Add new data to source
    cd "repo_sql_pull_src"
    dolt sql -q "INSERT INTO items VALUES (99, 'new_item');"
    dolt add .
    dolt commit -m "add new item"

    # Start sql-server on clone and pull
    cd "$BATS_TEST_TMPDIR/repo_sql_pull_clone"
    start_sql_server repo_sql_pull_clone
    dolt sql -q "CALL dolt_pull('origin');"

    # Verify new data visible
    run dolt sql -r csv -q "SELECT * FROM items WHERE id=99;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new_item" ]]
}

@test "ssh-transfer: sql dolt_remote add with SSH URL then push" {
    # Create source repo with data
    mkdir "repo_sql_remote_src"
    cd "repo_sql_remote_src"
    dolt init
    dolt sql -q "CREATE TABLE data (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO data VALUES (1), (2), (3);"
    dolt add .
    dolt commit -m "initial data"

    # Clone source via CLI to create a target that shares history
    cd "$BATS_TEST_TMPDIR"
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_sql_remote_src" repo_sql_remote_target

    # Create a second clone as the local working copy
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_sql_remote_src" repo_sql_remote_local
    cd "repo_sql_remote_local"

    # Start sql-server, add a new remote pointing to the target, push
    start_sql_server repo_sql_remote_local
    dolt sql -q "CALL dolt_remote('add', 'target', 'ssh://localhost$BATS_TEST_TMPDIR/repo_sql_remote_target');"
    dolt sql -q "INSERT INTO data VALUES (10);"
    dolt sql -q "CALL dolt_add('.');"
    dolt sql -q "CALL dolt_commit('-m', 'push to target');"
    dolt sql -q "CALL dolt_push('target', 'main');"
    stop_sql_server 1

    # Verify push landed in target
    cd "$BATS_TEST_TMPDIR/repo_sql_remote_target"
    run dolt log --oneline -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "push to target" ]]
}

@test "ssh-transfer: sql dolt_clone fails for non-existent SSH repo" {
    # Start sql-server in a fresh directory
    mkdir "sql_err_server"
    cd "sql_err_server"
    start_sql_server

    # Attempt clone of non-existent path
    run dolt sql -q "CALL dolt_clone('ssh://localhost/nonexistent/repo_does_not_exist');"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "repository not found" ]] || false
}

@test "ssh-transfer: sql dolt_push fails while sql-server locks target" {
    mkdir "repo_sql_lockpush_src"
    cd "repo_sql_lockpush_src"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1);"
    dolt add .
    dolt commit -m "initial"

    # Clone via CLI
    cd "$BATS_TEST_TMPDIR"
    dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_sql_lockpush_src" repo_sql_lockpush_clone

    # Lock source with sql-server
    cd "repo_sql_lockpush_src"
    start_sql_server repo_sql_lockpush_src
    SRC_PID=$SERVER_PID

    # Commit and push from clone via SQL procedure (no server on clone side)
    cd "$BATS_TEST_TMPDIR/repo_sql_lockpush_clone"
    dolt sql -q "INSERT INTO test VALUES (99);"
    dolt sql -q "CALL dolt_add('.');"
    dolt sql -q "CALL dolt_commit('-m', 'should fail');"
    run dolt sql -q "CALL dolt_push('origin', 'main');"
    [ "$status" -ne 0 ]

    # Cleanup source server (teardown kills $SERVER_PID which is still SRC_PID)
}