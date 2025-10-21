#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

# https://mariadb.com/docs/server/server-management/server-monitoring-logs/binary-log/binary-log-formats

setup() {
  setup_no_dolt_init

  # Create dedicated directory for MariaDB data and binlog dirs
  MARIADB_DATADIR="$BATS_FILE_TMPDIR/mariadb_data"
  MARIADB_BINLOG_DIR="$BATS_FILE_TMPDIR/binlog"
  MARIADB_SOCKET="$BATS_FILE_TMPDIR/mariadb.sock"
  MARIADB_PORT=$(definePORT)

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

  # Start MariaDB server
  mariadbd --defaults-file="$BATS_FILE_TMPDIR/my.cnf" &
  MARIADB_PID=$!

  # Wait for MariaDB to be ready
  for i in {1..30}; do
    if mariadb --socket="$MARIADB_SOCKET" -u root -e "SELECT 1" &>/dev/null; then
      break
    fi
    sleep 1
  done

  # File 000001: Mixed format safe statements
  mariadb --socket="$MARIADB_SOCKET" -u root <<SQL
SET GLOBAL binlog_format='MIXED';
CREATE DATABASE binlog_test;
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

  # File 000004: mysql database operations and GRANT statements
  mariadb --socket="$MARIADB_SOCKET" -u root <<SQL
SET SESSION binlog_format='ROW';

-- Direct edit of mysql database (should use statement logging regardless)
USE mysql;
CREATE TABLE IF NOT EXISTS test_direct (id INT PRIMARY KEY, data VARCHAR(50));
INSERT INTO test_direct VALUES (1, 'direct_edit');

-- Indirect edit via GRANT (always uses statement logging)
CREATE USER IF NOT EXISTS 'testuser'@'localhost' IDENTIFIED BY 'password';
GRANT SELECT ON binlog_test.* TO 'testuser'@'localhost';
REVOKE SELECT ON binlog_test.* FROM 'testuser'@'localhost';
SQL

  mariadb --socket="$MARIADB_SOCKET" -u root -e "FLUSH BINARY LOGS;"

  mariadb-admin --socket="$MARIADB_SOCKET" -u root shutdown
  wait $MARIADB_PID 2>/dev/null || true

  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000001" ]
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000002" ]
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000003" ]
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000004" ]

  # Create dolt database to receive binlog data
  cd "$BATS_TMPDIR/dolt-repo-$$"
  dolt init
  start_sql_server "dolt-repo-$$"
}

teardown() {
  stop_sql_server 1

  if [ ! -z "$MARIADB_PID" ] && ps -p $MARIADB_PID >/dev/null 2>&1; then
    kill $MARIADB_PID 2>/dev/null || true
    wait $MARIADB_PID 2>/dev/null || true
  fi

  rm -rf "$BATS_TMPDIR/dolt-repo-$$"
}

@test "mariadb-binlog: mixed format statements" {
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000001" ]

  # Apply binlog file through dolt sql-server using mariadb client
  # Pattern: mariadb-binlog <file> | mariadb -u root -h 127.0.0.1 -P <port>
  run mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000001"
  [ "$status" -eq 0 ]

  mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000001" | mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl

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

@test "mariadb-binlog: row-based DML operations" {
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000002" ]

  run mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000002"
  [ "$status" -eq 0 ]

  mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000002" | mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "USE binlog_test; SHOW TABLES"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "row_test" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT COUNT(*) FROM binlog_test.row_test"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "2" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT data FROM binlog_test.row_test WHERE id = 1"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "xxxxxxxx" ]] || false
}

@test "mariadb-binlog: statement format with LIMIT" {
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000003" ]

  run mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000003"
  [ "$status" -eq 0 ]

  mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000003" | mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "USE binlog_test; SHOW TABLES"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "limit_test" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT COUNT(*) FROM binlog_test.limit_test"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "3" ]] || false
}

@test "mariadb-binlog: mysql database and grants" {
  [ -f "$MARIADB_BINLOG_DIR/mariadb-bin.000004" ]

  run mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000004"
  [ "$status" -eq 0 ]

  mariadb-binlog "$MARIADB_BINLOG_DIR/mariadb-bin.000004" | mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "USE mysql; SHOW TABLES"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "test_direct" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT data FROM mysql.test_direct WHERE id = 1"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "direct_edit" ]] || false

  run mariadb -u root -h 127.0.0.1 -P $PORT --skip-ssl -e "SELECT user FROM mysql.user WHERE user = 'testuser'"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "testuser" ]] || false
}
