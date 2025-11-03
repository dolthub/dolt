#!/usr/bin/env bats
# Starts MariaDB with binlog enabled, executes operations, and extracts BINLOG statements to .txt files. `mariadb` and
# `mariadb-binlog` should be available to use. This is intended to run in a unix environment, (if you're on Windows, run
# in WSL), the script will find the relative directory to `testdata`. Tests are constructed as follows: query -> flush
# -> extract_binlog_to_file.

definePORT() {
  local base_port=$((2048 + ($$ % 4096)))

  for i in {0..99}
  do
    local port=$(((base_port + i) % 6144 + 2048))
    # nc (netcat) returns 0 when it _can_ connect to a port (therefore in use), 1 otherwise.
    run nc -z localhost $port
    if [ "$status" -eq 1 ]; then
      echo $port
      break
    fi
  done
}

setup_file() {
  export TEST_DIR="$(cd "$(dirname "$BATS_TEST_FILENAME")" && pwd)"
  export MARIADB_DATADIR="$BATS_FILE_TMPDIR/mariadb_data"
  export MARIADB_BINLOG_DIR="$BATS_FILE_TMPDIR/binlog"
  export MARIADB_SOCKET="$BATS_FILE_TMPDIR/mariadb.sock"
  export MARIADB_PORT=$(definePORT)

  rm -f "$TEST_DIR"/binlog_*.txt

  mkdir -p "$MARIADB_DATADIR"
  mkdir -p "$MARIADB_BINLOG_DIR"

  mariadb-install-db --datadir="$MARIADB_DATADIR" --auth-root-authentication-method=normal

  cat >"$BATS_FILE_TMPDIR/my.cnf" <<EOF
[mariadbd]
datadir=$MARIADB_DATADIR
socket=$MARIADB_SOCKET
port=$MARIADB_PORT
pid-file=$BATS_FILE_TMPDIR/mariadb.pid

log-bin=$MARIADB_BINLOG_DIR/mariadb-bin
binlog-format=ROW
server-id=1

skip-networking=0
bind-address=127.0.0.1
EOF

  mariadbd --defaults-file="$BATS_FILE_TMPDIR/my.cnf" &
  export MARIADB_PID=$!

  for i in {1..30}; do
    if mariadb --socket="$MARIADB_SOCKET" -u root -e "SELECT 1" &>/dev/null; then
      echo "MariaDB ready after $i seconds"
      break
    fi
    sleep 1
  done
}

teardown_file() {
  if [ ! -z "$MARIADB_PID" ] && ps -p $MARIADB_PID >/dev/null 2>&1; then
    mariadb-admin --socket="$MARIADB_SOCKET" -u root shutdown 2>/dev/null || true
    wait $MARIADB_PID 2>/dev/null || true
  fi
}

extract_binlog_to_file() {
  local binlog_file="$1"
  local output_file="$2"
  local database="${3:-}"

  # Extract only BINLOG statements from mariadb-binlog output
  # This filters out SET commands, DELIMITER, comments, etc.
  if [ -n "$database" ]; then
    mariadb-binlog --database="$database" "$binlog_file" | \
      awk '/^BINLOG /{flag=1} flag{print} /'\''\/\*!\*\/;$/{flag=0}' > "$output_file"
  else
    mariadb-binlog "$binlog_file" | \
      awk '/^BINLOG /{flag=1} flag{print} /'\''\/\*!\*\/;$/{flag=0}' > "$output_file"
  fi

  [ -f "$output_file" ]
  [ -s "$output_file" ]
}

@test "binlog_maker: simple INSERT with row events" {
  mariadb --socket="$MARIADB_SOCKET" -u root <<SQL
DROP DATABASE IF EXISTS mydb;
CREATE DATABASE mydb;
USE mydb;
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100));
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');
SQL

  mariadb --socket="$MARIADB_SOCKET" -u root -e "FLUSH BINARY LOGS;"

  extract_binlog_to_file "$MARIADB_BINLOG_DIR/mariadb-bin.000001" "$TEST_DIR/binlog_insert.txt"
}

@test "binlog_maker: UPDATE with row events" {
  mariadb --socket="$MARIADB_SOCKET" -u root <<SQL
USE mydb;
UPDATE users SET name = 'Alice Smith' WHERE id = 1;
SQL

  mariadb --socket="$MARIADB_SOCKET" -u root -e "FLUSH BINARY LOGS;"

  extract_binlog_to_file "$MARIADB_BINLOG_DIR/mariadb-bin.000002" "$TEST_DIR/binlog_update.txt"
}

@test "binlog_maker: DELETE with row events" {
  mariadb --socket="$MARIADB_SOCKET" -u root <<SQL
USE mydb;
DELETE FROM users WHERE id = 2;
SQL

  mariadb --socket="$MARIADB_SOCKET" -u root -e "FLUSH BINARY LOGS;"

  extract_binlog_to_file "$MARIADB_BINLOG_DIR/mariadb-bin.000003" "$TEST_DIR/binlog_delete.txt"
}

@test "binlog_maker: FORMAT_DESCRIPTION only" {
  mariadb --socket="$MARIADB_SOCKET" -u root -e "FLUSH BINARY LOGS;"

  mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000004" | \
    awk '/^BINLOG /{flag=1} flag{print} /'\''\/\*!\*\/;$/{flag=0}' > "$TEST_DIR/binlog_format_desc.txt"

  [ -f "$TEST_DIR/binlog_format_desc.txt" ]
  [ -s "$TEST_DIR/binlog_format_desc.txt" ]
}

@test "binlog_maker: TABLE_MAP without FORMAT_DESCRIPTION (for error test)" {
  # Create a new operation to get TABLE_MAP
  mariadb --socket="$MARIADB_SOCKET" -u root <<SQL
USE mydb;
INSERT INTO users VALUES (100, 'Test', 'test@example.com');
SQL

  mariadb --socket="$MARIADB_SOCKET" -u root -e "FLUSH BINARY LOGS;"

  # Extract BINLOG statements but skip the FORMAT_DESCRIPTION to simulate error condition
  # Skip the first BINLOG statement (FORMAT_DESCRIPTION), keep the rest
  mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000005" | \
    awk '/^BINLOG /{flag=1} flag{print} /'\''\/\*!\*\/;$/{flag=0}' | \
    awk 'BEGIN{first=1} /^BINLOG /{if(first){skip=1;first=0}else{skip=0}} !skip{print}' > "$TEST_DIR/binlog_no_format_desc.txt"

  [ -f "$TEST_DIR/binlog_no_format_desc.txt" ]
  [ -s "$TEST_DIR/binlog_no_format_desc.txt" ]
}

@test "binlog_maker: transaction with multiple INSERT UPDATE DELETE" {
  mariadb --socket="$MARIADB_SOCKET" -u root <<SQL
USE mydb;
DROP TABLE IF EXISTS multi_op_test;
CREATE TABLE multi_op_test (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(100),
  value DECIMAL(10,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

BEGIN;
INSERT INTO multi_op_test (name, value) VALUES ('Item1', 99.99);
INSERT INTO multi_op_test (name, value) VALUES ('Item2', 149.50);
INSERT INTO multi_op_test (name, value) VALUES ('Item3', 75.25);
UPDATE multi_op_test SET value = value * 1.1 WHERE id <= 2;
DELETE FROM multi_op_test WHERE id = 3;
COMMIT;
SQL

  mariadb --socket="$MARIADB_SOCKET" -u root -e "FLUSH BINARY LOGS;"

  extract_binlog_to_file "$MARIADB_BINLOG_DIR/mariadb-bin.000006" "$TEST_DIR/binlog_transaction_multi_ops.txt"
}
