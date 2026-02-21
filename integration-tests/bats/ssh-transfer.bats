#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

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
            if [[ "$arg" == "-o" ]] || [[ "$arg" == "-i" ]]; then
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
# Use exec to replace the shell process and preserve stdio connections
exec $COMMAND
EOF
    chmod +x "$BATS_TMPDIR/mock_ssh"
    
    # Set environment for SSH operations
    export DOLT_SSH="$BATS_TMPDIR/mock_ssh"
}

teardown() {
    unset DOLT_SSH
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
    run timeout 2s dolt --data-dir="repo1" transfer
    # We expect it to timeout since it's waiting for input, but it should start
    [ "$status" -eq 124 ] || [ "$status" -eq 143 ]  # timeout exit codes
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
    
    run dolt push origin main
    [ "$status" -eq 0 ]
    
    cd "../repo_push_source"
    run dolt sql -q "SELECT COUNT(*) FROM data;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]]
    
    run dolt sql -q "SELECT val FROM data WHERE id = 2;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "from_clone" ]]
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
            if [[ "$arg" == "-o" ]] || [[ "$arg" == "-i" ]]; then
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