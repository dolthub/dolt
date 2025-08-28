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
  rm -rf $BATS_TMPDIR/sql-server-mcp-config-test$$
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

@test "sql-server-mcp-config: starts MCP via config file mcp_server" {
  cd repo1

  SQL_PORT=$( definePORT )
  MCP_PORT=$( definePORT )

  # Prepare config that includes an mcp_server section
  cat > config.yml <<EOF2
listener:
  host: "0.0.0.0"
  port: ${SQL_PORT}

mcp_server:
  port: ${MCP_PORT}
  user: root
EOF2

  # Start server from config file
  if [ "$IS_WINDOWS" == true ]; then
    PORT=$SQL_PORT dolt sql-server --config ./config.yml &
  else
    PORT=$SQL_PORT dolt sql-server --config ./config.yml --socket "dolt.$SQL_PORT.sock" &
  fi
  SERVER_PID=$!

  # Wait for MCP to open
  run wait_for_mcp_port "$MCP_PORT" 8500
  [ $status -eq 0 ]
}

@test "sql-server-mcp-config: mcp_server.database selects DB for MCP connection" {
  cd repo1

  dolt sql -q "CREATE DATABASE mcpdb"

  SQL_PORT=$( definePORT )
  MCP_PORT=$( definePORT )

  cat > config.yml <<EOF2
listener:
  host: "0.0.0.0"
  port: ${SQL_PORT}

mcp_server:
  port: ${MCP_PORT}
  user: root
  database: mcpdb
EOF2

  if [ "$IS_WINDOWS" == true ]; then
    PORT=$SQL_PORT dolt sql-server --config ./config.yml &
  else
    PORT=$SQL_PORT dolt sql-server --config ./config.yml --socket "dolt.$SQL_PORT.sock" &
  fi
  SERVER_PID=$!

  run wait_for_mcp_port "$MCP_PORT" 8500
  [ $status -eq 0 ]

  # Optionally initialize and call a primitive tool endpoint via HTTP
  if command -v curl >/dev/null 2>&1; then
    INIT_FILE=$BATS_TMPDIR/mcp_init_cfgdb_$$.json
    OUT_INIT=$BATS_TMPDIR/mcp_out_init_cfgdb_$$.json
    echo '{"jsonrpc":"2.0","id":"1","method":"initialize","params":{"clientInfo":{"name":"bats","version":"0.0.0"},"capabilities":{}}}' > "$INIT_FILE"
    run bash -c "curl -sS -D $BATS_TMPDIR/mcp_headers_cfgdb_$$.txt -H 'Content-Type: application/json' --data-binary @'$INIT_FILE' http://127.0.0.1:${MCP_PORT}/ > '$OUT_INIT'"
    [ $status -eq 0 ]

    SESSION=$(grep -i '^Mcp-Session-Id:' $BATS_TMPDIR/mcp_headers_cfgdb_$$.txt | awk -F': ' '{print $2}' | tr -d '\r')
    [ -n "$SESSION" ]

    CALL_FILE=$BATS_TMPDIR/mcp_call_cfgdb_$$.json
    OUT_CALL=$BATS_TMPDIR/mcp_out_call_cfgdb_$$.json
    echo '{"jsonrpc":"2.0","id":"2","method":"tools/call","params":{"name":"list_databases","arguments":{}}}' > "$CALL_FILE"
    run bash -c "curl -sS -H 'Content-Type: application/json' -H 'Mcp-Session-Id: '$SESSION --data-binary @'$CALL_FILE' http://127.0.0.1:${MCP_PORT}/ > '$OUT_CALL'"
    [ $status -eq 0 ]
    run grep -E "mcpdb" "$OUT_CALL"
    [ $status -eq 0 ]
  fi
}

@test "sql-server-mcp-config: missing mcp_server.user with mcp_server.port fails" {
  cd repo1

  SQL_PORT=$( definePORT )
  MCP_PORT=$( definePORT )

  cat > config.yml <<EOF2
listener:
  host: "0.0.0.0"
  port: ${SQL_PORT}

mcp_server:
  port: ${MCP_PORT}
  # user intentionally omitted
EOF2

  if [ "$IS_WINDOWS" == true ]; then
    run env PORT=$SQL_PORT dolt sql-server --config ./config.yml
  else
    run env PORT=$SQL_PORT dolt sql-server --config ./config.yml --socket "dolt.$SQL_PORT.sock"
  fi
  [ $status -ne 0 ]
}

@test "sql-server-mcp-config: CLI flags override config mcp_server" {
  cd repo1

  SQL_PORT=$( definePORT )
  MCP_PORT_YAML=$( definePORT )
  MCP_PORT_CLI=$( definePORT )

  # Write config with one MCP port
  cat > config.yml <<EOF2
listener:
  host: "0.0.0.0"
  port: ${SQL_PORT}

mcp_server:
  port: ${MCP_PORT_YAML}
  user: root
EOF2

  # Start with CLI override for MCP port
  if [ "$IS_WINDOWS" == true ]; then
    PORT=$SQL_PORT dolt sql-server --config ./config.yml --mcp-port ${MCP_PORT_CLI} --mcp-user root &
  else
    PORT=$SQL_PORT dolt sql-server --config ./config.yml --socket "dolt.$SQL_PORT.sock" --mcp-port ${MCP_PORT_CLI} --mcp-user root &
  fi
  SERVER_PID=$!

  # SQL should accept connections
  run wait_for_connection $SQL_PORT 8500
  [ $status -eq 0 ]

  # MCP should be listening on CLI port, not YAML port
  run wait_for_mcp_port "$MCP_PORT_CLI" 8500
  [ $status -eq 0 ]

  run nc -z localhost "$MCP_PORT_YAML"
  [ $status -ne 0 ]
}
