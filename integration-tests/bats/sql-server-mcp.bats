#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir "$1"
  cd "$1"
  dolt init
  cd ..
}

setup() {
  skiponwindows "tests are flaky on Windows"
  setup_no_dolt_init
  make_repo repo1
}

teardown() {
  stop_sql_server 1 && sleep 0.5
  rm -rf $BATS_TMPDIR/sql-server-mcp-test$$
  teardown_common
}

# Helper: wait for an MCP HTTP server to accept TCP connections on the given port
# wait_for_mcp_port <PORT> <TIMEOUT_MS>
wait_for_mcp_port() {
  port=$1
  timeout_ms=$2
  end_time=$((SECONDS+($timeout_ms/1000)))
  while [ $SECONDS -lt $end_time ]; do
    nc -z localhost "$port" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

@test "sql-server-mcp: --mcp-database connects to specified database" {
  cd repo1

  # Create target database before starting server
  dolt sql -q "CREATE DATABASE mcpdb"

  MCP_PORT=$( definePORT )
  start_sql_server_with_args --host 0.0.0.0 --mcp-port "$MCP_PORT" --mcp-user root --mcp-database mcpdb
  run wait_for_mcp_port "$MCP_PORT" 8500
  [ $status -eq 0 ]

  # initialize and list via MCP
  INIT_FILE=$BATS_TMPDIR/mcp_init_md_$$.json
  OUT_INIT=$BATS_TMPDIR/mcp_out_init_md_$$.json
  echo '{"jsonrpc":"2.0","id":"1","method":"initialize","params":{"clientInfo":{"name":"bats","version":"0.0.0"},"capabilities":{}}}' > "$INIT_FILE"
  run bash -c "curl -sS -D $BATS_TMPDIR/mcp_headers_md_$$.txt -H 'Content-Type: application/json' --data-binary @'$INIT_FILE' http://127.0.0.1:${MCP_PORT}/ > '$OUT_INIT'"
  [ $status -eq 0 ]
  SESSION=$(grep -i '^Mcp-Session-Id:' $BATS_TMPDIR/mcp_headers_md_$$.txt | awk -F': ' '{print $2}' | tr -d '\r')
  [ -n "$SESSION" ]

  CALL_FILE=$BATS_TMPDIR/mcp_call_md_$$.json
  OUT_CALL=$BATS_TMPDIR/mcp_out_call_md_$$.json
  echo '{"jsonrpc":"2.0","id":"2","method":"tools/call","params":{"name":"list_databases","arguments":{}}}' > "$CALL_FILE"
  run bash -c "curl -sS -H 'Content-Type: application/json' -H 'Mcp-Session-Id: '$SESSION --data-binary @'$CALL_FILE' http://127.0.0.1:${MCP_PORT}/ > '$OUT_CALL'"
  [ $status -eq 0 ]
  run grep -E "mcpdb" "$OUT_CALL"
  [ $status -eq 0 ]
}

@test "sql-server-mcp: starts MCP HTTP server on --mcp-port and serves alongside SQL" {
  cd repo1

  # Choose distinct ports for SQL and MCP
  MCP_PORT=$( definePORT )

  # Start the sql-server with MCP enabled
  start_sql_server_with_args --host 0.0.0.0 --mcp-port="$MCP_PORT" --mcp-user root

  # Verify SQL server accepts connections on $PORT
  run dolt sql -q "SELECT 1;"
  [ $status -eq 0 ]

  # Verify MCP HTTP server port opens
  run wait_for_mcp_port "$MCP_PORT" 8500
  [ $status -eq 0 ]

  # If wget exists, optionally probe a health endpoint (implementation may add this)
  if command -v wget >/dev/null 2>&1; then
    run wget --quiet --tries=1 --spider "http://127.0.0.1:${MCP_PORT}/health"
    # health endpoint may not exist yet; don't assert success here
  fi
}

@test "sql-server-mcp: without --mcp-port, no MCP port is opened" {
  cd repo1

  # Pick a free port and ensure server doesn't bind it as MCP when not requested
  MCP_PORT=$( definePORT )

  # Start the sql-server WITHOUT MCP
  start_sql_server

  # Verify SQL server accepts connections on $PORT
  run dolt sql -q "SELECT 1;"
  [ $status -eq 0 ]

  # Verify MCP_PORT is not in use (best-effort; we chose an unused port)
  run nc -z localhost "$MCP_PORT"
  [ $status -ne 0 ]
}

@test "sql-server-mcp: HTTP initialize and call list_databases tool" {
  cd repo1

  if ! command -v curl >/dev/null 2>&1; then
    skip "curl not available"
  fi

  MCP_PORT=$( definePORT )

  # Start server with MCP enabled
  start_sql_server_with_args --host 0.0.0.0 --mcp-port="$MCP_PORT" --mcp-user root

  # Wait for MCP port to accept connections
  run wait_for_mcp_port "$MCP_PORT" 8500
  [ $status -eq 0 ]

  # Initialize session
  INIT_FILE=$BATS_TMPDIR/mcp_init_$$.json
  OUT_INIT=$BATS_TMPDIR/mcp_out_init_$$.json
  cat > "$INIT_FILE" <<'EOF'
{"jsonrpc":"2.0","id":"1","method":"initialize","params":{"clientInfo":{"name":"bats","version":"0.0.0"},"capabilities":{}}}
EOF
  run bash -c "curl -sS -D $BATS_TMPDIR/mcp_headers_$$.txt -H 'Content-Type: application/json' --data-binary @'$INIT_FILE' http://127.0.0.1:${MCP_PORT}/ > '$OUT_INIT'"
  [ $status -eq 0 ]

  # Extract session id from response headers
  SESSION=$(grep -i '^Mcp-Session-Id:' $BATS_TMPDIR/mcp_headers_$$.txt | awk -F': ' '{print $2}' | tr -d '\r')
  [ -n "$SESSION" ]

  # Call list_databases with session header
  CALL_FILE=$BATS_TMPDIR/mcp_call_$$.json
  OUT_CALL=$BATS_TMPDIR/mcp_out_call_$$.json
  cat > "$CALL_FILE" <<'EOF'
{"jsonrpc":"2.0","id":"2","method":"tools/call","params":{"name":"list_databases","arguments":{}}}
EOF
  run bash -c "curl -sS -H 'Content-Type: application/json' -H 'Mcp-Session-Id: '$SESSION --data-binary @'$CALL_FILE' http://127.0.0.1:${MCP_PORT}/ > '$OUT_CALL'"
  [ $status -eq 0 ]

  # Expect known databases in response payload (markdown or text content)
  run grep -E "information_schema|mysql|repo1" "$OUT_CALL"
  [ $status -eq 0 ]
}

@test "sql-server-mcp: invalid --mcp-port values (0 and >65535)" {
  cd repo1

  # mcp-port = 0
  SQL_PORT=$( definePORT )
  run dolt sql-server --host 0.0.0.0 --port="$SQL_PORT" --socket "dolt.$SQL_PORT.sock" --mcp-port 0 --mcp-user root
  [ $status -ne 0 ]

  # mcp-port > 65535
  SQL_PORT=$( definePORT )
  run dolt sql-server --host 0.0.0.0 --port="$SQL_PORT" --socket "dolt.$SQL_PORT.sock" --mcp-port 70000 --mcp-user root
  [ $status -ne 0 ]
}

@test "sql-server-mcp: --mcp-port identical to --port fails startup" {
  cd repo1
  PORT=$( definePORT )
  run dolt sql-server --host 0.0.0.0 --port="$PORT" --socket "dolt.$PORT.sock" --mcp-port "$PORT" --mcp-user root
  [ $status -ne 0 ]
}

@test "sql-server-mcp: multiple --mcp-port flags result in error" {
  cd repo1
  SQL_PORT=$( definePORT )
  # Provide duplicate mcp-port flags; expect failure
  run dolt sql-server --host 0.0.0.0 --port="$SQL_PORT" --socket "dolt.$SQL_PORT.sock" --mcp-port 45001 --mcp-user root --mcp-port 45002 --mcp-user root
  [ $status -ne 0 ]
}

@test "sql-server-mcp: any MCP arg without --mcp-port errors" {
  cd repo1
  SQL_PORT=$( definePORT )
  # --mcp-user without --mcp-port should fail
  run dolt sql-server --host 0.0.0.0 --port="$SQL_PORT" --socket "dolt.$SQL_PORT.sock" --mcp-user root
  [ $status -ne 0 ]
  # --mcp-password without --mcp-port should fail
  run dolt sql-server --host 0.0.0.0 --port="$SQL_PORT" --socket "dolt.$SQL_PORT.sock" --mcp-password secret
  [ $status -ne 0 ]
  # --mcp-database without --mcp-port should fail
  run dolt sql-server --host 0.0.0.0 --port="$SQL_PORT" --socket "dolt.$SQL_PORT.sock" --mcp-database somedb
  [ $status -ne 0 ]
}

@test "sql-server-mcp: restart with same --mcp-port succeeds after stop" {
  cd repo1
  MCP_PORT=$( definePORT )
  start_sql_server_with_args --host 0.0.0.0 --mcp-port "$MCP_PORT" --mcp-user root
  run wait_for_mcp_port "$MCP_PORT" 8500
  [ $status -eq 0 ]
  stop_sql_server 1

  # Immediate restart on same MCP port
  start_sql_server_with_args --host 0.0.0.0 --mcp-port "$MCP_PORT" --mcp-user root
  run wait_for_mcp_port "$MCP_PORT" 8500
  [ $status -eq 0 ]

  # initialize and list
  INIT_FILE=$BATS_TMPDIR/mcp_init_rs_$$.json
  OUT_INIT=$BATS_TMPDIR/mcp_out_init_rs_$$.json
  echo '{"jsonrpc":"2.0","id":"1","method":"initialize","params":{"clientInfo":{"name":"bats","version":"0.0.0"},"capabilities":{}}}' > "$INIT_FILE"
  run bash -c "curl -sS -D $BATS_TMPDIR/mcp_headers_rs_$$.txt -H 'Content-Type: application/json' --data-binary @'$INIT_FILE' http://127.0.0.1:${MCP_PORT}/ > '$OUT_INIT'"
  [ $status -eq 0 ]
  SESSION=$(grep -i '^Mcp-Session-Id:' $BATS_TMPDIR/mcp_headers_rs_$$.txt | awk -F': ' '{print $2}' | tr -d '\r')
  [ -n "$SESSION" ]
  CALL_FILE=$BATS_TMPDIR/mcp_call_rs_$$.json
  OUT_CALL=$BATS_TMPDIR/mcp_out_call_rs_$$.json
  echo '{"jsonrpc":"2.0","id":"2","method":"tools/call","params":{"name":"list_databases","arguments":{}}}' > "$CALL_FILE"
  run bash -c "curl -sS -H 'Content-Type: application/json' -H 'Mcp-Session-Id: '$SESSION --data-binary @'$CALL_FILE' http://127.0.0.1:${MCP_PORT}/ > '$OUT_CALL'"
  [ $status -eq 0 ]
  run grep -E "information_schema|mysql|repo1" "$OUT_CALL"
  [ $status -eq 0 ]
}

@test "sql-server-mcp: forceful termination closes servers cleanly; restart works" {
  cd repo1
  MCP_PORT=$( definePORT )
  start_sql_server_with_args --host 0.0.0.0 --mcp-port "$MCP_PORT" --mcp-user root
  run wait_for_mcp_port "$MCP_PORT" 8500
  [ $status -eq 0 ]

  # Force kill the server
  kill -9 $SERVER_PID
  # Give OS a moment to release ports
  sleep 1

  # Restart with same MCP port
  start_sql_server_with_args --host 0.0.0.0 --mcp-port "$MCP_PORT" --mcp-user root
  run wait_for_mcp_port "$MCP_PORT" 8500
  [ $status -eq 0 ]

  # initialize and list
  INIT_FILE=$BATS_TMPDIR/mcp_init_fk_$$.json
  OUT_INIT=$BATS_TMPDIR/mcp_out_init_fk_$$.json
  echo '{"jsonrpc":"2.0","id":"1","method":"initialize","params":{"clientInfo":{"name":"bats","version":"0.0.0"},"capabilities":{}}}' > "$INIT_FILE"
  run bash -c "curl -sS -D $BATS_TMPDIR/mcp_headers_fk_$$.txt -H 'Content-Type: application/json' --data-binary @'$INIT_FILE' http://127.0.0.1:${MCP_PORT}/ > '$OUT_INIT'"
  [ $status -eq 0 ]
  SESSION=$(grep -i '^Mcp-Session-Id:' $BATS_TMPDIR/mcp_headers_fk_$$.txt | awk -F': ' '{print $2}' | tr -d '\r')
  [ -n "$SESSION" ]
  CALL_FILE=$BATS_TMPDIR/mcp_call_fk_$$.json
  OUT_CALL=$BATS_TMPDIR/mcp_out_call_fk_$$.json
  echo '{"jsonrpc":"2.0","id":"2","method":"tools/call","params":{"name":"list_databases","arguments":{}}}' > "$CALL_FILE"
  run bash -c "curl -sS -H 'Content-Type: application/json' -H 'Mcp-Session-Id: '$SESSION --data-binary @'$CALL_FILE' http://127.0.0.1:${MCP_PORT}/ > '$OUT_CALL'"
  [ $status -eq 0 ]
  run grep -E "information_schema|mysql|repo1" "$OUT_CALL"
  [ $status -eq 0 ]
}
