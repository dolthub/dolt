# SSH Remote Transport - Manual Test Plan

Manual verification of `dolt clone`, `push`, `pull`, and `fetch` over real SSH connections.

## Prerequisites

- **Two machines** (or a single machine with SSH to localhost configured)
- **dolt** installed and on `$PATH` on both machines
- **SSH access** from client to server (key-based auth recommended; password auth works too)
- Server machine values used throughout (substitute your own):
  - `SERVER_HOST` — hostname or IP of the remote machine
  - `SERVER_USER` — SSH username on the remote machine
  - `SERVER_PORT` — SSH port (default 22)

## 1. Basic Clone

Create a repository on the server and clone it to the client.

**Server setup:**
```bash
mkdir -p /tmp/dolt-ssh-test/basic-clone
cd /tmp/dolt-ssh-test/basic-clone
dolt init
dolt sql -q "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price DECIMAL(10,2));"
dolt sql -q "INSERT INTO products VALUES (1, 'Widget', 19.99), (2, 'Gadget', 29.99);"
dolt add .
dolt commit -m "initial data"
```

**Client command:**
```bash
dolt clone "ssh://$SERVER_USER@$SERVER_HOST/tmp/dolt-ssh-test/basic-clone" basic-clone
```

**Expected result:**
- Exit code 0
- Directory `basic-clone` created with a valid dolt repository

**Verify:**
```bash
cd basic-clone
dolt sql -q "SELECT * FROM products;" -r csv
```
Should show both rows.

## 2. Clone with Explicit Port

**Server setup:** Use the same repository from test 1.

**Client command:**
```bash
dolt clone "ssh://$SERVER_USER@$SERVER_HOST:$SERVER_PORT/tmp/dolt-ssh-test/basic-clone" clone-with-port
```

**Expected result:**
- Exit code 0
- Data matches test 1

**Verify:**
```bash
cd clone-with-port
dolt sql -q "SELECT COUNT(*) FROM products;" -r csv
```
Should return `2`.

## 3. Clone without User Prefix

Tests that the SSH client uses the current OS user when no `user@` is specified.

**Client command:**
```bash
dolt clone "ssh://$SERVER_HOST/tmp/dolt-ssh-test/basic-clone" clone-no-user
```

**Expected result:**
- Exit code 0 (assuming the current OS user has SSH access to the server)

**Verify:**
```bash
cd clone-no-user
dolt sql -q "SELECT COUNT(*) FROM products;" -r csv
```

## 4. Push Changes to Remote

**Server setup:**
```bash
mkdir -p /tmp/dolt-ssh-test/push-target
cd /tmp/dolt-ssh-test/push-target
dolt init
dolt sql -q "CREATE TABLE data (id INT PRIMARY KEY, val TEXT);"
dolt sql -q "INSERT INTO data VALUES (1, 'original');"
dolt add .
dolt commit -m "initial"
```

**Client commands:**
```bash
dolt clone "ssh://$SERVER_USER@$SERVER_HOST/tmp/dolt-ssh-test/push-target" push-clone
cd push-clone
dolt sql -q "INSERT INTO data VALUES (2, 'from_client');"
dolt add .
dolt commit -m "add from client"
dolt push origin main
```

**Expected result:**
- `dolt push` exits 0

**Verify on server:**
```bash
cd /tmp/dolt-ssh-test/push-target
dolt log --oneline -n 2
```
Should show "add from client" commit.

## 5. Pull Changes from Remote

**Server setup:**
```bash
mkdir -p /tmp/dolt-ssh-test/pull-source
cd /tmp/dolt-ssh-test/pull-source
dolt init
dolt sql -q "CREATE TABLE items (id INT PRIMARY KEY, name TEXT);"
dolt sql -q "INSERT INTO items VALUES (1, 'item1');"
dolt add .
dolt commit -m "initial"
```

**Client commands:**
```bash
dolt clone "ssh://$SERVER_USER@$SERVER_HOST/tmp/dolt-ssh-test/pull-source" pull-clone
```

**Add more data on the server:**
```bash
cd /tmp/dolt-ssh-test/pull-source
dolt sql -q "INSERT INTO items VALUES (2, 'item2');"
dolt add .
dolt commit -m "add item2"
```

**Client pull:**
```bash
cd pull-clone
dolt pull origin
```

**Expected result:**
- `dolt pull` exits 0

**Verify on client:**
```bash
dolt sql -q "SELECT COUNT(*) FROM items;" -r csv
```
Should return `2`.

## 6. Fetch Specific Branch

**Server setup:**
```bash
mkdir -p /tmp/dolt-ssh-test/branch-test
cd /tmp/dolt-ssh-test/branch-test
dolt init
dolt sql -q "CREATE TABLE main_table (id INT PRIMARY KEY);"
dolt sql -q "INSERT INTO main_table VALUES (1);"
dolt add .
dolt commit -m "main commit"
dolt checkout -b feature
dolt sql -q "CREATE TABLE feature_table (id INT PRIMARY KEY);"
dolt sql -q "INSERT INTO feature_table VALUES (100);"
dolt add .
dolt commit -m "feature commit"
dolt checkout main
```

**Client commands:**
```bash
dolt clone "ssh://$SERVER_USER@$SERVER_HOST/tmp/dolt-ssh-test/branch-test" branch-clone
cd branch-clone
dolt fetch origin feature
dolt checkout feature
```

**Expected result:**
- Both fetch and checkout exit 0

**Verify:**
```bash
dolt sql -q "SHOW TABLES;" -r csv
```
Should include `feature_table`.

## 7. DOLT_SSH with Custom SSH Binary

Tests using a wrapper script as the SSH binary (e.g., to force a specific identity file).

**Client setup — create wrapper script:**
```bash
cat > /tmp/my_ssh_wrapper <<'EOF'
#!/bin/bash
exec ssh -i ~/.ssh/my_custom_key -o StrictHostKeyChecking=no "$@"
EOF
chmod +x /tmp/my_ssh_wrapper
```

**Client command:**
```bash
DOLT_SSH=/tmp/my_ssh_wrapper dolt clone "ssh://$SERVER_USER@$SERVER_HOST/tmp/dolt-ssh-test/basic-clone" clone-custom-ssh
```

**Expected result:**
- Exit code 0
- Clone uses the specified identity file

**Verify:**
```bash
cd clone-custom-ssh
dolt sql -q "SELECT COUNT(*) FROM products;" -r csv
```

## 8. Error Cases

### 8a. Non-existent Remote Path

**Client command:**
```bash
dolt clone "ssh://$SERVER_USER@$SERVER_HOST/nonexistent/path/repo" should-fail
```

**Expected:** Non-zero exit code with an error message indicating the repository was not found.

### 8b. Remote Host Where dolt Is Not Installed

**Client command (against a host without dolt):**
```bash
dolt clone "ssh://$SERVER_USER@nodolt-host/tmp/some-repo" should-fail
```

**Expected:** Non-zero exit code. Error message should indicate the remote command failed (e.g., "dolt: command not found" from SSH stderr, or "transfer subprocess exited immediately").

### 8c. SSH Auth Failure

**Client command (with a bad identity file):**
```bash
DOLT_SSH="ssh -i /tmp/nonexistent_key -o PasswordAuthentication=no" dolt clone "ssh://$SERVER_USER@$SERVER_HOST/tmp/dolt-ssh-test/basic-clone" should-fail
```

**Expected:** Non-zero exit code. Error from SSH auth failure.

### 8d. Unreachable Host

**Client command:**
```bash
dolt clone "ssh://user@192.0.2.1/tmp/repo" should-fail
```

**Expected:** Non-zero exit code after SSH connection timeout. (Note: this may take a while depending on SSH timeout settings.)

## Cleanup

On the server:
```bash
rm -rf /tmp/dolt-ssh-test
```

On the client:
```bash
rm -rf basic-clone clone-with-port clone-no-user push-clone pull-clone branch-clone clone-custom-ssh should-fail
```
