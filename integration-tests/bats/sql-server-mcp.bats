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

@test "sql-server mcp: starts MCP HTTP server on --mcp-port and serves alongside SQL" {
  skip "--mcp-port is not yet implemented."
  cd repo1

  # Choose distinct ports for SQL and MCP
  MCP_PORT=$( definePORT )

  # Start the sql-server with MCP enabled
  start_sql_server_with_args --host 0.0.0.0 --mcp-port="$MCP_PORT"

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

@test "sql-server mcp: without --mcp-port, no MCP port is opened" {
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

@test "sql-server mcp: fails startup when MCP port already in use" {
  cd repo1
  MCP_PORT=$( definePORT )
  SQL_PORT=$( definePORT )

  # Occupy MCP_PORT with a simple HTTP server
  python3 -m http.server "$MCP_PORT" >/dev/null 2>&1 &
  BLOCKER_PID=$!
  sleep 1

  # Attempt to start sql-server with occupied MCP port should fail
  if [ "$IS_WINDOWS" == true ]; then
    run dolt sql-server --host 0.0.0.0 --port="$SQL_PORT" --mcp-port="$MCP_PORT"
  else
    run dolt sql-server --host 0.0.0.0 --port="$SQL_PORT" --socket "dolt.$SQL_PORT.sock" --mcp-port="$MCP_PORT"
  fi
  [ $status -ne 0 ]

  # Cleanup blocker
  kill "$BLOCKER_PID" 2>/dev/null || true
}

@test "sql-server mcp: HTTP initialize and call list_databases tool" {
  skip "--mcp-port is not yet implemented."
  cd repo1

  if ! command -v curl >/dev/null 2>&1; then
    skip "curl not available"
  fi

  MCP_PORT=$( definePORT )

  # Start server with MCP enabled
  start_sql_server_with_args --host 0.0.0.0 --mcp-port="$MCP_PORT"

  # Prepare NDJSON stream: initialize then call list_databases
  NDJSON_FILE=$BATS_TMPDIR/mcp_ndjson_$$.txt
  OUT_FILE=$BATS_TMPDIR/mcp_out_$$.txt
  cat > "$NDJSON_FILE" <<'EOF'
{"jsonrpc":"2.0","id":"1","method":"initialize","params":{"clientInfo":{"name":"bats","version":"0.0.0"},"capabilities":{}}}
{"jsonrpc":"2.0","id":"2","method":"tools/call","params":{"name":"list_databases","arguments":{}}}
EOF

  # Send stream to MCP server
  run bash -c "curl -sS -N -H 'Content-Type: application/x-ndjson' --data-binary @'$NDJSON_FILE' http://127.0.0.1:${MCP_PORT}/mcp > '$OUT_FILE'"
  [ $status -eq 0 ]

  # Expect known databases in response payload (markdown or text content)
  run grep -E "information_schema|mysql|repo1" "$OUT_FILE"
  [ $status -eq 0 ]
}
