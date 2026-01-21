#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

# Tests for SSH transfer operations (clone, pull, push via SSH URLs)
# Uses mock SSH to avoid requiring an SSH server

setup() {
    setup_common
    
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

# Log and execute the command locally  
echo "Mock SSH executing: $COMMAND" >> "$BATS_TMPDIR/mock_ssh.log"
eval "$COMMAND"
echo "Mock SSH command exit code: $?" >> "$BATS_TMPDIR/mock_ssh.log"
EOF
    chmod +x "$BATS_TMPDIR/mock_ssh"
    
    # Set environment for SSH operations
    export DOLT_SSH="$BATS_TMPDIR/mock_ssh"
    export DOLT_TRANSFER_LONG_TIMEOUT=1
}

teardown() {
    teardown_common
    unset DOLT_SSH
    unset DOLT_TRANSFER_LONG_TIMEOUT
}

@test "ssh-transfer: transfer command works with --data-dir" {
    # Initialize a test repository
    mkdir "$BATS_TEST_TMPDIR/repo1"
    cd "$BATS_TEST_TMPDIR/repo1"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY, val TEXT);"
    dolt sql -q "INSERT INTO test VALUES (1, 'hello');"
    dolt add .
    dolt commit -m "initial commit"
    
    # Test transfer command with --data-dir
    cd "$BATS_TEST_TMPDIR"
    run timeout 2s dolt --data-dir="$BATS_TEST_TMPDIR/repo1" transfer
    # We expect it to timeout since it's waiting for input, but it should start
    [ "$status" -eq 124 ] || [ "$status" -eq 143 ]  # timeout exit codes
}

@test "ssh-transfer: clone via SSH URL" {
    # Create source repository
    mkdir "$BATS_TEST_TMPDIR/repo_source"
    cd "$BATS_TEST_TMPDIR/repo_source"
    dolt init
    dolt sql -q "CREATE TABLE products (id VARCHAR(36) DEFAULT (UUID()) PRIMARY KEY, name TEXT, price DECIMAL(10,2));"
    dolt sql -q "INSERT INTO products (name, price) VALUES ('Widget', 19.99), ('Gadget', 29.99);"
    dolt add .
    dolt commit -m "initial data"
    
    # Clone using SSH URL with mock SSH
    cd "$BATS_TEST_TMPDIR"
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_source" repo_clone
    [ "$status" -eq 0 ]
    [ -d repo_clone ]
    
    # Verify data was cloned
    cd repo_clone
    run dolt sql -q "SELECT COUNT(*) FROM products;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]]
}

@test "ssh-transfer: clone with user@host format" {
    # Create source repository
    mkdir "$BATS_TEST_TMPDIR/repo_usertest"
    cd "$BATS_TEST_TMPDIR/repo_usertest"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1), (2), (3);"
    dolt add .
    dolt commit -m "test data"
    
    # Clone with user@host format
    cd "$BATS_TEST_TMPDIR"
    run dolt clone "ssh://testuser@localhost$BATS_TEST_TMPDIR/repo_usertest" repo_clone_user
    [ "$status" -eq 0 ]
    [ -d repo_clone_user ]
    
    cd repo_clone_user
    run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]]
}

@test "ssh-transfer: pull changes from remote" {
    # Create source repository
    mkdir "$BATS_TEST_TMPDIR/repo_pull_source"
    cd "$BATS_TEST_TMPDIR/repo_pull_source"
    dolt init
    dolt sql -q "CREATE TABLE items (id INT PRIMARY KEY, name TEXT);"
    dolt sql -q "INSERT INTO items VALUES (1, 'item1');"
    dolt add .
    dolt commit -m "initial"
    
    # Clone repository
    cd "$BATS_TEST_TMPDIR"
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_pull_source" repo_pull_clone
    [ "$status" -eq 0 ]
    
    # Add more data in source
    cd "$BATS_TEST_TMPDIR/repo_pull_source"
    dolt sql -q "INSERT INTO items VALUES (2, 'item2');"
    dolt add .
    dolt commit -m "add item2"
    
    # Pull changes in clone
    cd "$BATS_TEST_TMPDIR/repo_pull_clone"
    run dolt pull origin
    [ "$status" -eq 0 ]
    
    # Verify new data exists
    run dolt sql -q "SELECT COUNT(*) FROM items;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]]
}

@test "ssh-transfer: push changes to remote" {
    # Create source repository
    mkdir "$BATS_TEST_TMPDIR/repo_push_source"
    cd "$BATS_TEST_TMPDIR/repo_push_source"
    dolt init
    dolt sql -q "CREATE TABLE data (id INT PRIMARY KEY, val TEXT);"
    dolt sql -q "INSERT INTO data VALUES (1, 'original');"
    dolt add .
    dolt commit -m "initial"
    
    # Clone repository
    cd "$BATS_TEST_TMPDIR"
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_push_source" repo_push_clone
    [ "$status" -eq 0 ]
    
    # Make changes in clone
    cd "$BATS_TEST_TMPDIR/repo_push_clone"
    dolt sql -q "INSERT INTO data VALUES (2, 'from_clone');"
    dolt add .
    dolt commit -m "add from clone"
    
    # Push changes
    run dolt push origin main
    [ "$status" -eq 0 ]
    
    # Verify in source repository
    cd "$BATS_TEST_TMPDIR/repo_push_source"
    run dolt sql -q "SELECT COUNT(*) FROM data;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]]
    
    run dolt sql -q "SELECT val FROM data WHERE id = 2;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "from_clone" ]]
}

@test "ssh-transfer: handle branch operations" {
    # Create source with branches
    mkdir "$BATS_TEST_TMPDIR/repo_branch_source"
    cd "$BATS_TEST_TMPDIR/repo_branch_source"
    dolt init
    dolt sql -q "CREATE TABLE main_table (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO main_table VALUES (1);"
    dolt add .
    dolt commit -m "main commit"
    
    # Create feature branch
    dolt checkout -b feature
    dolt sql -q "CREATE TABLE feature_table (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO feature_table VALUES (1);"
    dolt add .
    dolt commit -m "feature commit"
    dolt checkout main
    
    # Clone and verify branches
    cd "$BATS_TEST_TMPDIR"
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_branch_source" repo_branch_clone
    [ "$status" -eq 0 ]
    
    cd "$BATS_TEST_TMPDIR/repo_branch_clone"
    run dolt fetch origin feature
    [ "$status" -eq 0 ]
    
    run dolt checkout feature
    [ "$status" -eq 0 ]
    
    # Verify feature branch content
    run dolt sql -q "SHOW TABLES;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "feature_table" ]]
}

@test "ssh-transfer: concurrent clone operations" {
    # Create source repository
    mkdir "$BATS_TEST_TMPDIR/repo_concurrent_source"
    cd "$BATS_TEST_TMPDIR/repo_concurrent_source"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1), (2), (3), (4), (5);"
    dolt add .
    dolt commit -m "test data"
    
    # Start multiple clones concurrently
    cd "$BATS_TEST_TMPDIR"
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
    
    # All should succeed
    [ "$STATUS1" -eq 0 ]
    [ "$STATUS2" -eq 0 ]
    [ "$STATUS3" -eq 0 ]
    
    # Verify all have correct data
    for dir in repo_concurrent_1 repo_concurrent_2 repo_concurrent_3; do
        cd "$BATS_TEST_TMPDIR/$dir"
        run dolt sql -q "SELECT COUNT(*) FROM test;" -r csv
        [ "$status" -eq 0 ]
        [[ "$output" =~ "5" ]]
    done
}

@test "ssh-transfer: error handling for non-existent repository" {
    # Try to clone non-existent repository
    run dolt clone "ssh://localhost/nonexistent/repo" should_fail
    [ "$status" -ne 0 ]
    [ ! -d should_fail ]
}

@test "ssh-transfer: push with large dataset" {
    skip "Optional: Test with larger dataset if needed"
    
    # Create repository with substantial data
    mkdir "$BATS_TEST_TMPDIR/repo_large_source"
    cd "$BATS_TEST_TMPDIR/repo_large_source"
    dolt init
    dolt sql -q "CREATE TABLE large_table (id INT PRIMARY KEY, data TEXT);"
    
    # Insert many rows
    for i in {1..1000}; do
        dolt sql -q "INSERT INTO large_table VALUES ($i, 'data_$i');"
    done
    dolt add .
    dolt commit -m "large dataset"
    
    # Clone and verify
    cd "$BATS_TEST_TMPDIR"
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_large_source" repo_large_clone
    [ "$status" -eq 0 ]
    
    cd "$BATS_TEST_TMPDIR/repo_large_clone"
    run dolt sql -q "SELECT COUNT(*) FROM large_table;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1000" ]]
}

@test "ssh-transfer: verify DOLT_SSH environment variable works" {
    # Create a custom mock SSH that logs calls
    cat > "$BATS_TMPDIR/mock_ssh_logger" <<'EOF'
#!/bin/bash
echo "CUSTOM_SSH_CALLED" >> "$BATS_TMPDIR/ssh_calls.log"
exec "$BATS_TMPDIR/mock_ssh" "$@"
EOF
    chmod +x "$BATS_TMPDIR/mock_ssh_logger"
    
    # Use custom SSH
    export DOLT_SSH="$BATS_TMPDIR/mock_ssh_logger"
    
    # Create and clone repository
    mkdir "$BATS_TEST_TMPDIR/repo_env_test"
    cd "$BATS_TEST_TMPDIR/repo_env_test"
    dolt init
    dolt sql -q "CREATE TABLE test (id INT PRIMARY KEY);"
    dolt add .
    dolt commit -m "test"
    
    cd "$BATS_TEST_TMPDIR"
    run dolt clone "ssh://localhost$BATS_TEST_TMPDIR/repo_env_test" repo_env_clone
    [ "$status" -eq 0 ]
    
    # Verify custom SSH was used
    [ -f "$BATS_TMPDIR/ssh_calls.log" ]
    grep -q "CUSTOM_SSH_CALLED" "$BATS_TMPDIR/ssh_calls.log"
}