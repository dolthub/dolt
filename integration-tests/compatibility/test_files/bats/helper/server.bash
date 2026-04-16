# Server lifecycle and output helpers for SQL server compatibility tests.
# Load this file alongside helper/common in any test file that starts a dolt sql-server.

# define_port finds an available TCP port in the range 2048-6143 and prints it.
define_port() {
  local port
  for i in {0..99}; do
    port=$((RANDOM % 4096 + 2048))
    if ! nc -z localhost "$port" 2>/dev/null; then
      echo "$port"
      return 0
    fi
  done
  echo "define_port: no available port found after 100 attempts" >&2
  return 1
}

# wait_for_server_connection polls the SQL client until it connects or the timeout (in ms) elapses.
# Requires $dolt_client to be set.
wait_for_server_connection() {
  local port="$1"
  local timeout_ms="$2"
  local end_time=$((SECONDS + (timeout_ms / 1000)))

  while [ $SECONDS -lt $end_time ]; do
    if "$dolt_client" sql -q "SELECT 1;" > /dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "wait_for_server_connection: failed to connect on port $port within ${timeout_ms}ms" >&2
  return 1
}

# start_sql_server starts a SQL server using $dolt_server on a randomly chosen available port.
# Sets PORT, DB_NAME, and SERVER_PID in the calling scope.
# Requires $dolt_server and $dolt_client to be set.
start_sql_server() {
  PORT=$(define_port)
  DB_NAME=$(basename "$PWD")

  if [ "$IS_WINDOWS" != true ]; then
    "$dolt_server" sql-server --host 0.0.0.0 --port="$PORT" --socket "dolt.$PORT.sock" > server.log 2>&1 3>&- &
  else
    "$dolt_server" sql-server --host 0.0.0.0 --port="$PORT" > server.log 2>&1 3>&- &
  fi
  SERVER_PID=$!

  wait_for_server_connection "$PORT" 8500
}

# stop_sql_server terminates the running SQL server and removes the socket file.
# Reads SERVER_PID and PORT from the calling scope.
stop_sql_server() {
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

# skip_if_server_lt skips the current test if the server binary version is older than min_version.
# Requires $dolt_server to be set.
skip_if_server_lt() {
  local min_version="$1"
  local reason="$2"
  local server_version
  server_version=$("$dolt_server" version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1)
  if [ -z "$server_version" ]; then
    return 0
  fi
  if [ "$(printf '%s\n' "$server_version" "$min_version" | sort -V | head -n1)" != "$min_version" ]; then
    skip "$reason (server version: $server_version)"
  fi
}

# strip_ansi removes ANSI color escape sequences from a string.
strip_ansi() {
  printf "%s\n" "$1" | sed 's/\x1b\[[0-9;]*m//g'
}

# extract_commit_hash returns the first commit hash from dolt log output,
# stripping any ANSI escape sequences first.
extract_commit_hash() {
  printf "%s\n" "$1" | sed 's/\x1b\[[0-9;]*m//g' | grep -m1 '^commit ' | awk '{print $2}'
}

# latest_commit returns the hash of the most recent commit using $dolt_client.
# Requires $dolt_client to be set.
latest_commit() {
  local log_output
  log_output=$("$dolt_client" log 2>&1)
  extract_commit_hash "$log_output"
}
