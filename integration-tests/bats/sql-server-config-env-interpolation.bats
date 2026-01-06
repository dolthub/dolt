#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir "$1"
  cd "$1"
  dolt init
  cd ..
}

# wait_for_http_ok <url> <timeout_ms>
wait_for_http_ok() {
  url="$1"
  timeout_ms="$2"
  end_time=$((SECONDS+($timeout_ms/1000)))
  while [ $SECONDS -lt $end_time ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

setup() {
  skiponwindows "tests are flaky on Windows"
  if [ "$SQL_ENGINE" = "remote-engine" ]; then
    skip "This test tests remote connections directly, SQL_ENGINE is not needed."
  fi
  setup_no_dolt_init
  make_repo repo1
}

teardown() {
  stop_sql_server 1 && sleep 0.5
  rm -rf "$BATS_TMPDIR/sql-server-config-env-interpolation-test$$"
  teardown_common
}

# bats test_tags=no_lambda
@test "sql-server-config-env-interpolation: required env var expands listener.port" {
  cd repo1

  SQL_PORT=$(definePORT)

  # Use a single-quoted heredoc so bash does not expand ${...}
  cat > config.yml <<'EOF'
listener:
  host: "0.0.0.0"
  port: ${DOLT_TEST_SQLSERVER_PORT}
EOF

  if [ "$IS_WINDOWS" == true ]; then
    DOLT_TEST_SQLSERVER_PORT=$SQL_PORT PORT=$SQL_PORT dolt sql-server --config ./config.yml &
  else
    DOLT_TEST_SQLSERVER_PORT=$SQL_PORT PORT=$SQL_PORT dolt sql-server --config ./config.yml --socket "dolt.$SQL_PORT.sock" &
  fi
  SERVER_PID=$!

  run wait_for_connection "$SQL_PORT" 8500
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "sql-server-config-env-interpolation: default expression uses default when unset/empty" {
  cd repo1

  SQL_PORT=$(definePORT)
  unset DOLT_TEST_SQLSERVER_PORT

  # Leave ${DOLT_TEST_SQLSERVER_PORT:-<default>} unexpanded for Dolt, but expand $SQL_PORT here.
  cat > config.yml <<EOF
listener:
  host: "0.0.0.0"
  port: \${DOLT_TEST_SQLSERVER_PORT:-$SQL_PORT}
EOF

  if [ "$IS_WINDOWS" == true ]; then
    PORT=$SQL_PORT dolt sql-server --config ./config.yml &
  else
    PORT=$SQL_PORT dolt sql-server --config ./config.yml --socket "dolt.$SQL_PORT.sock" &
  fi
  SERVER_PID=$!

  run wait_for_connection "$SQL_PORT" 8500
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "sql-server-config-env-interpolation: missing env var fails with clear error" {
  cd repo1

  unset DOLT_TEST_SQLSERVER_MISSING

  # Use a single-quoted heredoc so bash does not expand ${...}
  cat > config.yml <<'EOF'
listener:
  host: "0.0.0.0"
  port: ${DOLT_TEST_SQLSERVER_MISSING}
EOF

  run dolt sql-server --config ./config.yml
  [ $status -ne 0 ]
  log_output_has "Failed to interpolate environment variables in yaml file"
  log_output_has "DOLT_TEST_SQLSERVER_MISSING"
}

# bats test_tags=no_lambda
@test "sql-server-config-env-interpolation: dollar escaping and env composition works (metrics labels)" {
  cd repo1

  SQL_PORT=$(definePORT)
  METRICS_PORT=$(definePORT)
  export DOLT_TEST_LABEL="foo"

  # We want the YAML to contain $$${DOLT_TEST_LABEL} literally:
  # - $$ becomes a literal '$'
  # - ${DOLT_TEST_LABEL} expands to "foo"
  # => final label value should be "$foo"
  cat > config.yml <<EOF
listener:
  host: "0.0.0.0"
  port: $SQL_PORT

metrics:
  host: "127.0.0.1"
  port: $METRICS_PORT
  labels:
    dollar: "\$\$\${DOLT_TEST_LABEL}"
EOF

  if [ "$IS_WINDOWS" == true ]; then
    PORT=$SQL_PORT dolt sql-server --config ./config.yml &
  else
    PORT=$SQL_PORT dolt sql-server --config ./config.yml --socket "dolt.$SQL_PORT.sock" &
  fi
  SERVER_PID=$!

  run wait_for_connection "$SQL_PORT" 8500
  [ $status -eq 0 ]

  metrics_url="http://127.0.0.1:$METRICS_PORT/metrics"
  run wait_for_http_ok "$metrics_url" 8500
  [ $status -eq 0 ]

  # Trigger at least one query to ensure metrics are being emitted.
  dolt sql -q "select 1"

  expectedLabel="dollar=\"\$${DOLT_TEST_LABEL}\""
  found=0
  for i in {1..10}; do
    curl -fsS "$metrics_url" > "$BATS_TMPDIR/metrics_env_interp_$$.out" 2>/dev/null || true
    if grep -F "$expectedLabel" "$BATS_TMPDIR/metrics_env_interp_$$.out" >/dev/null 2>&1; then
      found=1
      break
    fi
    sleep 1
  done

  [ "$found" -eq 1 ]
}

