# Sql-server lifecycle helpers for the cross-version compatibility suite.
# Pair with query-server-common.bash for definePORT and IS_WINDOWS, and with
# compat-common.bash for old_dolt and new_dolt:
#
#   bats_load_library common.bash
#   bats_load_library query-server-common.bash
#   bats_load_library compat-common.bash
#   bats_load_library compat-server.bash
#
# After start_old_sql_server, these are exported into the calling scope:
#   PORT       the random port the running sql-server is bound to.
#   DB_NAME    the database the client should target on that server.
#   SERVER_PID the running sql-server's process id.

new_dolt_cli() {
  new_dolt --host 127.0.0.1 --port "$PORT" --user root --password "" --no-tls --use-db "$DB_NAME" "$@"
}

# wait_for_old_server(<port>, <timeout_ms>) returns 0 once a connection
# succeeds and 1 if |timeout_ms| elapses first.
wait_for_old_server() {
  local port="$1"
  local timeout_ms="$2"
  local end_time=$((SECONDS + (timeout_ms / 1000)))

  while [ $SECONDS -lt $end_time ]; do
    if new_dolt_cli sql -q "SELECT 1;" > /dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "wait_for_old_server: failed to connect on port $port within ${timeout_ms}ms" >&2
  return 1
}

start_old_sql_server() {
  PORT=$(definePORT)
  DB_NAME=$(basename "$PWD")

  if [ "$IS_WINDOWS" != true ]; then
    old_dolt sql-server --host 0.0.0.0 --port="$PORT" --socket "dolt.$PORT.sock" > server.log 2>&1 3>&- &
  else
    old_dolt sql-server --host 0.0.0.0 --port="$PORT" > server.log 2>&1 3>&- &
  fi
  SERVER_PID=$!

  wait_for_old_server "$PORT" 8500
}

stop_old_sql_server() {
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    while ps -p "$SERVER_PID" > /dev/null 2>&1; do
      sleep 0.1
    done
  fi
  SERVER_PID=""
  if [ -n "$PORT" ] && [ -f "dolt.$PORT.sock" ]; then
    rm -f "dolt.$PORT.sock"
  fi
}

# latest_commit queries dolt_log via new_dolt_cli using only legacy columns
# so it works against old sql-servers that do not expose the fixed schema.
latest_commit() {
  new_dolt_cli sql -r csv -q "SELECT commit_hash FROM dolt_log ORDER BY date DESC LIMIT 1;" | tail -n1 | tr -d '\r'
}
