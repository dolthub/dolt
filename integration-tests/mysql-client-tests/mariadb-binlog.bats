#!/usr/bin/env bats
load helper/common.bash
load helper/query-server-common.bash

setup_file() {
  export MARIADB_DATADIR="$BATS_FILE_TMPDIR/mariadb_data"
  export MARIADB_BINLOG_DIR="$BATS_FILE_TMPDIR/binlog"
  export MARIADB_SOCKET="$BATS_FILE_TMPDIR/mariadb.sock"
  export MARIADB_PORT=$(definePORT)

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
binlog-format=MIXED
max_binlog_size=1048576
server-id=1

skip-networking=0
bind-address=127.0.0.1
EOF

  mariadbd --defaults-file="$BATS_FILE_TMPDIR/my.cnf" --user=root &
  export MARIADB_PID=$!

  for _ in {1..30}; do
    if mariadb --socket="$MARIADB_SOCKET" -u root -e "SELECT 1" &>/dev/null; then
      break
    fi
    sleep 1
  done

  # File 000001: Mixed format safe statements
  mariadb --socket="$MARIADB_SOCKET" -u root <<SQL
SET GLOBAL binlog_format='MIXED';
CREATE DATABASE IF NOT EXISTS binlog_test;
USE binlog_test;

-- Safe statements that use statement-based logging
CREATE TABLE stmt_test (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO stmt_test (name) VALUES ('Alice'), ('Bob'), ('Charlie');
UPDATE stmt_test SET name = CONCAT(name, '_updated') WHERE id > 0;
SQL

  mariadb --socket="$MARIADB_SOCKET" -u root -e "FLUSH BINARY LOGS;"

  # File 000002: Row-based format DML operations
  mariadb --socket="$MARIADB_SOCKET" -u root <<SQL
USE binlog_test;
SET SESSION binlog_format='ROW';

CREATE TABLE row_test (
    id INT PRIMARY KEY,
    data VARCHAR(500)
);

-- Generate multiple row operations
INSERT INTO row_test VALUES
    (1, REPEAT('a', 400)),
    (2, REPEAT('b', 400)),
    (3, REPEAT('c', 400));

UPDATE row_test SET data = REPEAT('x', 400) WHERE id IN (1, 2);
DELETE FROM row_test WHERE id = 3;
SQL

  mariadb --socket="$MARIADB_SOCKET" -u root -e "FLUSH BINARY LOGS;"

  # File 000003: Statement format with unsafe statements
  mariadb --socket="$MARIADB_SOCKET" -u root <<SQL
USE binlog_test;
SET SESSION binlog_format='STATEMENT';

CREATE TABLE limit_test (
    id INT PRIMARY KEY,
    value VARCHAR(100)
);

INSERT INTO limit_test VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');

-- Unsafe statement with LIMIT (non-deterministic)
DELETE FROM limit_test WHERE id > 0 LIMIT 2;
SQL

  mariadb --socket="$MARIADB_SOCKET" -u root -e "FLUSH BINARY LOGS;"

  # File 000004: User and GRANT statements
  mariadb --socket="$MARIADB_SOCKET" -u root <<SQL
SET SESSION binlog_format='ROW';

-- User and GRANT operations (always use statement logging)
CREATE USER IF NOT EXISTS 'testuser'@'localhost' IDENTIFIED BY 'password';
GRANT SELECT ON binlog_test.* TO 'testuser'@'localhost';
REVOKE SELECT ON binlog_test.* FROM 'testuser'@'localhost';
SQL

  mariadb --socket="$MARIADB_SOCKET" -u root -e "FLUSH BINARY LOGS;"

  # File 000005: Multi-file test data - first file
  mariadb --socket="$MARIADB_SOCKET" -u root <<SQL
USE binlog_test;
SET SESSION binlog_format='ROW';

CREATE TABLE multi_file_test1 (
    id INT PRIMARY KEY,
    file_num INT,
    description VARCHAR(100)
);

INSERT INTO multi_file_test1 VALUES
    (1, 1, 'From first binlog file'),
    (2, 1, 'Also from first file');
SQL

  mariadb --socket="$MARIADB_SOCKET" -u root -e "FLUSH BINARY LOGS;"

  # File 000006: Multi-file test data - second file
  mariadb --socket="$MARIADB_SOCKET" -u root <<SQL
USE binlog_test;
SET SESSION binlog_format='ROW';

CREATE TABLE multi_file_test2 (
    id INT PRIMARY KEY,
    file_num INT,
    description VARCHAR(100)
);

INSERT INTO multi_file_test2 VALUES
    (1, 2, 'From second binlog file'),
    (2, 2, 'Also from second file');

-- Also insert into the first table to verify table ID reuse across files
INSERT INTO multi_file_test1 VALUES (3, 2, 'Cross-file insert');
SQL

  mariadb --socket="$MARIADB_SOCKET" -u root -e "FLUSH BINARY LOGS;"

  mariadb-admin --socket="$MARIADB_SOCKET" -u root shutdown
  wait $MARIADB_PID 2>/dev/null || true

  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000001" ]
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000002" ]
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000003" ]
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000004" ]
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000005" ]
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000006" ]

  # Create dolt database to receive binlog data (shared across all tests)
  export DOLT_REPO="$BATS_TMPDIR/dolt-repo-$$"
  mkdir -p "$DOLT_REPO"
  cd "$DOLT_REPO" || false
  dolt init
}

setup() {
  cd "$DOLT_REPO" || false
  start_sql_server_with_args
}

teardown() {
  stop_sql_server
}

teardown_file() {

  if [ ! -z "$MARIADB_PID" ] && ps -p $MARIADB_PID >/dev/null 2>&1; then
    kill $MARIADB_PID 2>/dev/null || true
    wait $MARIADB_PID 2>/dev/null || true
  fi

  rm -rf "$DOLT_REPO"
}

# bats test_tags=no_lambda
@test "mixed format statements" {
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000001" ]

  # Apply binlog file through dolt sql-server using mariadb client
  # Pattern: mariadb-binlog <file> | mariadb -u root -h 127.0.0.1 -P <port>
  run mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000001"
  [ "$status" -eq 0 ]

  mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000001" | mariadb -u root -h 127.0.0.1 -P "$PORT" --skip-ssl

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SHOW DATABASES"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "binlog_test" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "USE binlog_test; SHOW TABLES"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "stmt_test" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT COUNT(*) FROM binlog_test.stmt_test"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "3" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT name FROM binlog_test.stmt_test WHERE id = 1"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Alice_updated" ]] || false
}

# bats test_tags=no_lambda
@test "row-based DML operations" {
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000002" ]

  run mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000002"
  [ "$status" -eq 0 ]

  mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000002" | mariadb -u root -h 127.0.0.1 -P "$PORT" --skip-ssl

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "USE binlog_test; SHOW TABLES"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "row_test" ]] || false

  # INSERT: 3 rows initially inserted, but 1 deleted later, so should have 2 rows
  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT COUNT(*) FROM binlog_test.row_test"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "2" ]] || false

  # UPDATE: id=1 should have been updated from 'aaa...' to 'xxx...'
  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT data FROM binlog_test.row_test WHERE id = 1"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "xxxxxxxx" ]] || false

  # UPDATE: id=2 should also have been updated from 'bbb...' to 'xxx...'
  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT data FROM binlog_test.row_test WHERE id = 2"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "xxxxxxxx" ]] || false

  # DELETE: id=3 should not exist
  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT COUNT(*) FROM binlog_test.row_test WHERE id = 3"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "0" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT id FROM binlog_test.row_test ORDER BY id"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "1" ]] || false
  [[ "$output" =~ "2" ]] || false
  [[ ! "$output" =~ "3" ]] || false
}

# bats test_tags=no_lambda
@test "statement format with LIMIT" {
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000003" ]

  run mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000003"
  [ "$status" -eq 0 ]

  mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000003" | mariadb -u root -h 127.0.0.1 -P "$PORT" --skip-ssl

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "USE binlog_test; SHOW TABLES"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "limit_test" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT COUNT(*) FROM binlog_test.limit_test"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "3" ]] || false
}

# bats test_tags=no_lambda
@test "user and grants" {
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000004" ]

  run mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000004"
  [ "$status" -eq 0 ]

  mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000004" | mariadb -u root -h 127.0.0.1 -P "$PORT" --skip-ssl

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT user, host FROM mysql.user WHERE user = 'testuser'"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "testuser" ]] || false
  [[ "$output" =~ "localhost" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SHOW GRANTS FOR 'testuser'@'localhost'"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "USAGE" ]] || false
  [[ ! "$output" =~ "SELECT.*binlog_test" ]] || false
}

# bats test_tags=no_lambda
@test "privilege enforcement" {
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000001" ]

  mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "CREATE USER IF NOT EXISTS 'unprivileged'@'localhost' IDENTIFIED BY 'test'"
  mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "GRANT SELECT ON *.* TO 'unprivileged'@'localhost'"

  run bash -c "mariadb-binlog '$MARIADB_BINLOG_DIR/mariadb-bin.000001' | mariadb -u unprivileged -ptest -h 127.0.0.1 -P '$PORT' --skip-ssl 2>&1"
  [ "$status" -ne 0 ]
  [[ "$output" =~ "Access denied" ]] || [[ "$output" =~ "command denied" ]] || false

  mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "GRANT SUPER ON *.* TO 'unprivileged'@'localhost'"

  run bash -c "mariadb-binlog '$MARIADB_BINLOG_DIR/mariadb-bin.000001' | mariadb -u unprivileged -ptest -h 127.0.0.1 -P '$PORT' --skip-ssl --force 2>&1"
  [ "$status" -eq 0 ]
  [[ ! "$output" =~ "Access denied" ]] || [[ ! "$output" =~ "command denied" ]] || false

  mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "DROP USER IF EXISTS 'unprivileged'@'localhost'"
}

# bats test_tags=no_lambda
@test "multiple binlog files in one command" {
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000005" ]
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000006" ]

  mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000005" "$MARIADB_BINLOG_DIR/mariadb-bin.000006" | mariadb -u root -h 127.0.0.1 -P "$PORT" --skip-ssl

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "USE binlog_test; SHOW TABLES"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "multi_file_test1" ]] || false
  [[ "$output" =~ "multi_file_test2" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT COUNT(*) FROM binlog_test.multi_file_test1 WHERE file_num = 1"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "2" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT description FROM binlog_test.multi_file_test1 WHERE id = 1"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "From first binlog file" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT COUNT(*) FROM binlog_test.multi_file_test2 WHERE file_num = 2"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "2" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT description FROM binlog_test.multi_file_test2 WHERE id = 1"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "From second binlog file" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT COUNT(*) FROM binlog_test.multi_file_test1"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "3" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT description FROM binlog_test.multi_file_test1 WHERE id = 3"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "Cross-file insert" ]] || false
}

