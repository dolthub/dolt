#!/usr/bin/env bats

load $BATS_TEST_DIRNAME/helper/common.bash

# These tests validate docker/docker-entrypoint.sh using a Docker image built from
# docker/serverDockerfile in the repo root. They follow existing integration test conventions.

setup() {
  setup_no_dolt_init

  # Compute workspace root from integration-tests/bats directory
  WORKSPACE_ROOT=$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)
  export WORKSPACE_ROOT

  DOLT_DOCKER_TEST_VERSION=${DOLT_DOCKER_TEST_VERSION:-source}
  TEST_IMAGE="dolt-entrypoint-it:${DOLT_DOCKER_TEST_VERSION}"
  TEST_PREFIX="dolt-entrypoint-it-$$-"

  # Build from source only once per test run, check img already exists
  if ! docker image inspect "$TEST_IMAGE" >/dev/null 2>&1; then
    echo "Building Dolt from source for integration tests..."
    docker build -f "$WORKSPACE_ROOT/dolt/docker/serverDockerfile" --build-arg DOLT_VERSION=$DOLT_DOCKER_TEST_VERSION -t "$TEST_IMAGE" "$WORKSPACE_ROOT"
  else
    echo "Using existing source-built image: $TEST_IMAGE"
  fi
}

teardown() {
  docker ps -a --filter "name=$TEST_PREFIX" --format '{{.Names}}' | xargs -r docker rm -f >/dev/null 2>&1 || true
  teardown_common
}

# Helper to run a container and wait for server to be ready
run_container() {
  name="$1"; shift
  docker run -d --name "$name" "$@" "$TEST_IMAGE" >/dev/null
  wait_for_log "$name" "Server ready. Accepting connections."
  wait_for_log "$name" "Dolt init process done. Ready for connections."

  # Verify container is running
  run docker ps --filter "name=$name" --format "{{.Names}}"
  [ $status -eq 0 ]
  [[ "$output" =~ ^"$name"$ ]] || false

  wait_for_server_ready "$name"
}

# Helper to run a container with port mapping and wait for server to be ready
run_container_with_port() {
  name="$1"; shift
  port="$1"; shift
  docker run -d --name "$name" -p "$port:3306" "$@" "$TEST_IMAGE" >/dev/null
  wait_for_log "$name" "Server ready. Accepting connections."
  wait_for_log "$name" "Dolt init process done. Ready for connections."

  # Verify container is running
  run docker ps --filter "name=$name" --format "{{.Names}}"
  [ $status -eq 0 ]
  [[ "$output" =~ ^"$name"$ ]] || false

  wait_for_server_ready "$name"
}

# Helper to wait for server to be ready for queries (following Dolt patterns)
wait_for_server_ready() {
  name="$1"
  timeout_ms=8500  # Match standard Dolt timeout
  end_time=$((SECONDS+($timeout_ms/1000)))

  while [ $SECONDS -lt $end_time ]; do
    if docker exec "$name" dolt sql -q "SELECT 1;" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# Helper to wait for a log line to appear
wait_for_log() {
  name="$1"; shift
  pattern="$1"; shift
  timeout="${1:-15}"
  i=0
  while [ $i -lt $timeout ]; do
    if docker logs "$name" 2>&1 | grep -q "$pattern"; then
      sleep 1
      return 0
    fi
    sleep 1
    i=$((i+1))
  done
  return 1
}

# bats test_tags=no_lambda
@test "docker-entrypoint: env USER=root is rejected with clear error" {
  cname="${TEST_PREFIX}root-env"
  run docker run -d --name "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_USER=root -e DOLT_PASSWORD=anything "$TEST_IMAGE"

  wait_for_log "$cname" "cannot be used for the root user"
  docker logs "$cname" >/tmp/${cname}.log 2>&1
  run grep -F "cannot be used for the root user" /tmp/"${cname}".log
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "docker-entrypoint: password without user warns and is ignored" {
  cname="${TEST_PREFIX}pass-no-user"
  run_container "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_PASSWORD=orphan
  docker logs "$cname" >/tmp/${cname}.log 2>&1
  run grep -i "password will be ignored" /tmp/"${cname}".log
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "docker-entrypoint: DOLT_ROOT_HOST creates root@% with grants" {
  cname="${TEST_PREFIX}root-host"
  run_container "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=%

  run docker exec "$cname" dolt sql -q "SHOW GRANTS FOR 'root'@'%';"
  [ $status -eq 0 ]
  [[ "$output" =~ "WITH GRANT OPTION" ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: reserved keywords in params are escaped correctly" {
  cname="${TEST_PREFIX}kw-params"
  kw_db="select"
  kw_user="from"
  run_container "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=% -e DOLT_DATABASE="$kw_db" -e DOLT_USER="$kw_user" -e DOLT_PASSWORD=pass

  run docker exec "$cname" dolt sql --result-format csv -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$kw_db" >/dev/null

  run docker exec "$cname" dolt sql --result-format csv -q "SELECT User FROM mysql.user;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$kw_user" >/dev/null

  run docker exec "$cname" dolt sql --result-format csv -q "SHOW GRANTS FOR '$kw_user'@'%';"
  [ $status -eq 0 ]
  printf -v p1 '`%s`\\.\\* TO' "$kw_db"
  [[ "$output" =~ $p1 ]] || false
  p2="\`$kw_user\`@\`%\`"
  [[ "$output" =~ $p2 ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: reserved keyword 'versioning' database name support" {
  cname="${TEST_PREFIX}versioning"
  kw_db="versioning"
  usr="testuser"
  pwd="testpass"
  run_container "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_DATABASE="$kw_db" -e DOLT_USER="$usr" -e DOLT_PASSWORD="$pwd"

  run docker exec "$cname" dolt sql --result-format csv -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$kw_db" >/dev/null

  # DBeaver checks tables without `` escaping
  run docker exec "$cname" dolt sql --result-format csv -q "SHOW FULL TABLES FROM versioning;"
  [ $status -eq 0 ]
  [[ "$output" =~ "Tables_in_versioning,Table_type" ]] || false

  # Can use the database for operations
  run docker exec "$cname" dolt sql -q "USE \`$kw_db\`; CREATE TABLE test_table (id INT);"
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "docker-entrypoint: user and database creation works" {
  cname="${TEST_PREFIX}user-db"
  db="testdb"
  usr="testuser"
  pwd="testpass"
  run_container "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=% -e DOLT_DATABASE="$db" -e DOLT_USER="$usr" -e DOLT_PASSWORD="$pwd"

  run docker exec "$cname" dolt sql --result-format csv -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$db" >/dev/null

  run docker exec "$cname" dolt sql --result-format csv -q "SELECT User FROM mysql.user WHERE User='$usr';"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$usr" >/dev/null

  run docker exec "$cname" dolt sql --result-format csv -q "SHOW GRANTS FOR '$usr'@'%';"
  [ $status -eq 0 ]
  printf -v p1 '`%s`\\.\\* TO' "$db"
  [[ "$output" =~ $p1 ]] || false
  p2="\`$usr\`@\`%\`"
  [[ "$output" =~ $p2 ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: setup with no configuration" {
  cname="${TEST_PREFIX}setup"
  run_container "$cname"

  # Root user can execute queries immediately
  run docker exec "$cname" dolt sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]

  # Root user can create databases and tables immediately
  run docker exec "$cname" dolt sql -q "CREATE DATABASE quick_test; USE quick_test; CREATE TABLE test (id INT);"
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "docker-entrypoint: root functionality and privileges" {
  cname="${TEST_PREFIX}root-fallback"
  run_container "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=%

  # Root user has full privileges
  run docker exec "$cname" dolt sql -q "SHOW GRANTS FOR 'root'@'localhost';"
  [ $status -eq 0 ]
  [[ "$output" =~ "WITH GRANT OPTION" ]] || false

  # Root user can create databases and perform operations
  run docker exec "$cname" dolt sql -q "CREATE DATABASE root_test; USE root_test; CREATE TABLE test (id INT); INSERT INTO test VALUES (1);"
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "docker-entrypoint: SQL error reporting without suppression" {
  cname="${TEST_PREFIX}sql-error-reportin"
  run_container "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_USER=testuser -e DOLT_PASSWORD=testpass

  # Create the user first time (should succeed)
  run docker exec "$cname" dolt sql -q "CREATE USER IF NOT EXISTS 'testuser'@'%' IDENTIFIED BY 'testpass';"
  [ $status -eq 0 ]

  # Try to create the same user again (should fail with detailed error)
  run docker exec "$cname" dolt sql -q "CREATE USER 'testuser'@'%' IDENTIFIED BY 'testpass';"
  [ $status -ne 0 ]
  # The error should contain detailed information (not suppressed)
  [[ "$output" =~ [Oo]peration.*failed|[Uu]ser.*already.*exists|[Dd]uplicate ]] || false
}


# bats test_tags=no_lambda
@test "docker-entrypoint: empty DOLT_ROOT_PASSWORD is allowed" {
  cname="${TEST_PREFIX}empty-password-allowed"
  run_container "$cname" -e DOLT_ROOT_PASSWORD="" -e DOLT_ROOT_HOST=%

  run docker exec "$cname" dolt sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "information_schema" ]] || false

  run docker exec "$cname" dolt -u root -p "" sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "information_schema" ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: valid server config file handling" {
  cname="${TEST_PREFIX}valid-server-config"
  temp_dir="/tmp/test-config-$$"
  mkdir -p $temp_dir
  cat > $temp_dir/test.yaml << 'EOF'
log_level: info
behavior:
  read_only: false
  autocommit: true
listener:
  host: localhost
  port: 3306
  max_connections: 100
data_dir: .
cfg_dir: .doltcfg
EOF

  run_container "$cname" -v $temp_dir:/etc/dolt/servercfg.d:ro

  rm -rf $temp_dir
}

# bats test_tags=no_lambda
@test "docker-entrypoint: wrong password authentication" {
  cname="${TEST_PREFIX}wrong-pass"
  usr="testuser"
  pwd="testpass"

  # Run container with custom user
  run_container_with_port "$cname" 3306 \
    -e DOLT_ROOT_PASSWORD=rootpass \
    -e DOLT_ROOT_HOST=% \
    -e DOLT_USER="$usr" \
    -e DOLT_PASSWORD="$pwd"

  run docker exec "$cname" dolt -u "$usr" -p "wrongpass" sql -q "SHOW DATABASES;"
  [ $status -ne 0 ]
  [[ "$output" =~ "Error 1045 (28000): Access denied for user 'testuser'" ]] || false

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$usr" --password="wrongpass" -e "SHOW DATABASES;"
  [ $status -ne 0 ]
  [[ "$output" =~ "ERROR 1045 (28000): Access denied for user 'testuser'" ]] || false

  run docker exec "$cname" dolt -u root -p wrongpass sql -q "SHOW DATABASES;"
  [ $status -ne 0 ]
  [[ "$output" =~ "Error 1045 (28000): Access denied for user 'root'" ]] || false

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u root --password=wrongpass -e "SHOW DATABASES;"
  [ $status -ne 0 ]
  [[ "$output" =~ "ERROR 1045 (28000): Access denied for user 'root'" ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: DOLT_USER_HOST and MYSQL_USER_HOST creates user with specific host" {
  cname="${TEST_PREFIX}user-host"
  run_container_with_port "$cname" 3306 -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=localhost -e DOLT_USER=testuser -e DOLT_PASSWORD=testpass -e DOLT_USER_HOST=%

  run docker exec "$cname" dolt sql -q "GRANT ALL PRIVILEGES ON mysql.* TO 'testuser'@'%';"
  [ $status -eq 0 ]

  run docker exec "$cname" dolt -u testuser -p testpass sql --result-format csv -q "SELECT User, Host FROM mysql.user"
  [ $status -eq 0 ]
  [[ "$output" =~ "testuser,%" ]] || false

  run docker exec "$cname" bash -c "dolt sql --result-format csv -q \"SELECT User, Host FROM mysql.user WHERE User='root';\""
  [ $status -eq 0 ]
  [[ "$output" =~ "root,localhost" ]] || false

  run docker exec "$cname" bash -c "dolt sql -q \"SHOW GRANTS FOR 'testuser'@'%';\""
  [ $status -eq 0 ]
  [[ "$output" =~ "GRANT USAGE" ]] || false

  run docker stop "$cname"
  [ $status -eq 0 ]
  [[ $output = "$cname" ]] || false

  # Test MYSQL_USER_HOST variant
  cname2="${TEST_PREFIX}user-host-2"
  run_container_with_port "$cname2" 3306 -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=% -e DOLT_USER=testuser2 -e DOLT_PASSWORD=testpass2 -e MYSQL_USER_HOST=%

  run docker exec "$cname2" dolt -u root -p rootpass sql --result-format csv -q "SELECT User, Host FROM mysql.user WHERE User='testuser2';"
  [ $status -eq 0 ]
  [[ "$output" =~ "testuser2,%" ]] || false

  run docker exec "$cname2" dolt -u root -p rootpass sql --result-format csv -q "SELECT User, Host FROM mysql.user WHERE User='root';"
  [ $status -eq 0 ]
  [[ "$output" =~ "root,%" ]] || false

  run docker exec "$cname2" dolt -u root -p rootpass sql -q "SHOW GRANTS FOR 'testuser2'@'%';"
  [ $status -eq 0 ]
  [[ "$output" =~ "GRANT USAGE" ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: custom database with root and user access" {
  cname="${TEST_PREFIX}dolt-auth"
  db="testdb"
  usr="testuser"
  pwd="testpass"

  run_container_with_port "$cname" 3306 \
    -e DOLT_ROOT_PASSWORD=rootpass \
    -e DOLT_ROOT_HOST=% \
    -e DOLT_DATABASE="$db" \
    -e DOLT_USER="$usr" \
    -e DOLT_PASSWORD="$pwd"

  run docker exec "$cname" dolt -u root -p rootpass sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "$db" ]] || false

  run docker exec "$cname" dolt -u root -p rootpass sql -q "USE \`$db\`; CREATE TABLE root_table (id INT PRIMARY KEY, data VARCHAR(100));"
  [ $status -eq 0 ]

  run docker exec "$cname" dolt -u root -p rootpass sql -q "USE \`$db\`; INSERT INTO root_table VALUES (1, 'root data');"
  [ $status -eq 0 ]

  run docker exec "$cname" dolt -u "$usr" -p "$pwd" sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "$db" ]] || false

  run docker exec "$cname" dolt -u "$usr" -p "$pwd" sql -q "USE \`$db\`; CREATE TABLE user_table (id INT PRIMARY KEY, data VARCHAR(100));"
  [ $status -eq 0 ]

  run docker exec "$cname" dolt -u "$usr" -p "$pwd" sql -q "USE \`$db\`; INSERT INTO user_table VALUES (1, 'user data');"
  [ $status -eq 0 ]

  run docker exec "$cname" dolt -u "$usr" -p "$pwd" sql -q "USE \`$db\`; SELECT * FROM user_table;"
  [ $status -eq 0 ]
  [[ "$output" =~ "user data" ]] || false

  # Test that custom user can see root's table (both have access to the same database)
  run docker exec "$cname" dolt -u "$usr" -p "$pwd" sql -q "USE \`$db\`; SELECT * FROM root_table;"
  [ $status -eq 0 ]
  [[ "$output" =~ "root data" ]] || false

  # Test that root can see user's table
  run docker exec "$cname" dolt -u root -p rootpass sql -q "USE \`$db\`; SELECT * FROM user_table;"
  [ $status -eq 0 ]
  [[ "$output" =~ "user data" ]] || false

  # Test that custom user cannot access other databases (if any exist)
  run docker exec "$cname" dolt -u "$usr" -p "$pwd" sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  # Should only see the custom database and information_schema
  db_count=$(echo "$output" | grep -c "testdb\|information_schema" || true)
  [ "$db_count" -eq 2 ] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: MySQL client connectivity via TCP" {
  cname="${TEST_PREFIX}mysql-client"
  db="testdb"
  usr="testuser"
  pwd="testpass"

  run_container_with_port "$cname" 3306 -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=% -e DOLT_DATABASE="$db" -e DOLT_USER="$usr" -e DOLT_PASSWORD="$pwd"

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u root --password=rootpass -e "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$db" >/dev/null

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$usr" --password="$pwd" -e "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$db" >/dev/null

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$usr" --password="$pwd" -e "USE \`$db\`; CREATE TABLE mysql_test (id INT PRIMARY KEY, name VARCHAR(50));"
  [ $status -eq 0 ]

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$usr" --password="$pwd" -e "USE \`$db\`; INSERT INTO mysql_test VALUES (1, 'mysql_test_data');"
  [ $status -eq 0 ]

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$usr" --password="$pwd" -e "USE \`$db\`; SELECT * FROM mysql_test;"
  [ $status -eq 0 ]
  [[ "$output" =~ "mysql_test_data" ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: MySQL client with reserved keywords" {
  cname="${TEST_PREFIX}mysql-kw"
  kw_db="select"
  kw_user="from"
  pwd="testpass"

  run_container_with_port "$cname" 3306 -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=% -e DOLT_DATABASE="$kw_db" -e DOLT_USER="$kw_user" -e DOLT_PASSWORD="$pwd"

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u root --password=rootpass -e "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$kw_db" >/dev/null

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$kw_user" --password="$pwd" -e "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$kw_db" >/dev/null

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$kw_user" --password="$pwd" -e "USE \`$kw_db\`; CREATE TABLE test_table (id INT);"
  [ $status -eq 0 ]

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$kw_user" --password="$pwd" -e "USE \`$kw_db\`; INSERT INTO test_table VALUES (1);"
  [ $status -eq 0 ]

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$kw_user" --password="$pwd" -e "USE \`$kw_db\`; SELECT * FROM test_table;"
  [ $status -eq 0 ]
  [[ "$output" =~ "1" ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: MySQL client custom database with root and user access" {
  cname="${TEST_PREFIX}custom-d"
  db="testdb"
  usr="testuser"
  pwd="testpass"

  run_container_with_port "$cname" 3306 \
    -e DOLT_ROOT_PASSWORD=rootpass \
    -e DOLT_ROOT_HOST=% \
    -e DOLT_DATABASE="$db" \
    -e DOLT_USER="$usr" \
    -e DOLT_PASSWORD="$pwd"

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u root --password=rootpass -e "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "$db" ]] || false

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u root --password=rootpass -e "USE \`$db\`; CREATE TABLE root_table (id INT PRIMARY KEY, data VARCHAR(100));"
  [ $status -eq 0 ]

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u root --password=rootpass -e "USE \`$db\`; INSERT INTO root_table VALUES (1, 'root data');"
  [ $status -eq 0 ]

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$usr" --password="$pwd" -e "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "$db" ]] || false

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$usr" --password="$pwd" -e "USE \`$db\`; CREATE TABLE user_table (id INT PRIMARY KEY, data VARCHAR(100));"
  [ $status -eq 0 ]

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$usr" --password="$pwd" -e "USE \`$db\`; INSERT INTO user_table VALUES (1, 'user data');"
  [ $status -eq 0 ]

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$usr" --password="$pwd" -e "USE \`$db\`; SELECT * FROM user_table;"
  [ $status -eq 0 ]
  [[ "$output" =~ "user data" ]] || false

  # Test that custom user can see root's table (both have access to the same database)
  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$usr" --password="$pwd" -e "USE \`$db\`; SELECT * FROM root_table;"
  [ $status -eq 0 ]
  [[ "$output" =~ "root data" ]] || false

  # Test that root can see user's table
  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u root --password=rootpass -e "USE \`$db\`; SELECT * FROM user_table;"
  [ $status -eq 0 ]
  [[ "$output" =~ "user data" ]] || false

  # Test that custom user cannot access other databases (if any exist)
  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u "$usr" --password="$pwd" -e "SHOW DATABASES;"
  [ $status -eq 0 ]
  # Should only see the custom database and information_schema
  db_count=$(echo "$output" | grep -c "testdb\|information_schema" || true)
  [ "$db_count" -eq 2 ] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: docker-entrypoint-initdb.d script execution" {
  cname="${TEST_PREFIX}initdb-scripts"

  local temp_dir="/tmp/initdb-test-$$"
  mkdir -p "$temp_dir"

  cat > "$temp_dir/01-create-table.sql" << 'EOF'
CREATE DATABASE IF NOT EXISTS testinit;
USE testinit;
CREATE TABLE init_test (id INT, message VARCHAR(100));
INSERT INTO init_test VALUES (1, 'SQL script executed');
EOF

  cat > "$temp_dir/02-bash-script.sh" << 'EOF'
#!/bin/bash
echo "Bash script executed"
dolt sql -q "USE testinit; INSERT INTO init_test VALUES (2, 'Bash script executed');"
EOF
  chmod +x "$temp_dir/02-bash-script.sh"

  cat > "$temp_dir/03-data.sql" << 'EOF'
USE testinit;
INSERT INTO init_test VALUES (3, 'Compressed SQL executed');
EOF
  gzip "$temp_dir/03-data.sql"

  run_container_with_port "$cname" 3306 -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=% -v "$temp_dir:/docker-entrypoint-initdb.d"

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u root --password=rootpass -e "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "testinit" ]] || false

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u root --password=rootpass -e "USE testinit; SELECT * FROM init_test ORDER BY id;"
  [ $status -eq 0 ]
  [[ "$output" =~ "SQL script executed" ]] || false
  [[ "$output" =~ "Bash script executed" ]] || false
  [[ "$output" =~ "Compressed SQL executed" ]] || false

  rm -rf "$temp_dir"
}

# bats test_tags=no_lambda
@test "docker-entrypoint: .sql.gz gzip compressed file processing" {
  cname="${TEST_PREFIX}gzip-sql"

  local temp_dir="/tmp/gzip-sql-test-$$"
  mkdir -p "$temp_dir"

  cat > "$temp_dir/01-init.sql" << 'EOF'
CREATE DATABASE IF NOT EXISTS gzip_sql_db;
USE gzip_sql_db;
CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2));
INSERT INTO products VALUES (1, 'Widget', 19.99);
INSERT INTO products VALUES (2, 'Gadget', 29.99);
INSERT INTO products VALUES (3, 'Doohickey', 39.99);
CREATE USER IF NOT EXISTS 'gzipuser'@'%' IDENTIFIED BY 'gzippass';
GRANT ALL ON `gzip_sql_db`.* TO 'gzipuser'@'%';
GRANT USAGE ON *.* TO 'gzipuser'@'%';
EOF
  gzip "$temp_dir/01-init.sql"

  run_container_with_port "$cname" 3306 -e DOLT_ROOT_HOST=% -v "$temp_dir:/docker-entrypoint-initdb.d"

  run docker exec "$cname" dolt sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "gzip_sql_db" ]] || false

  run docker exec "$cname" dolt sql -q "USE gzip_sql_db; SELECT COUNT(*) as count FROM products;"
  [ $status -eq 0 ]
  [[ "$output" =~ "3" ]] || false

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u gzipuser --password=gzippass -e "USE gzip_sql_db; SELECT name FROM products WHERE price > 25.00;"
  [ $status -eq 0 ]
  [[ "$output" =~ "Gadget" ]] || false
  [[ "$output" =~ "Doohickey" ]] || false

  rm -rf "$temp_dir"
}

# bats test_tags=no_lambda
@test "docker-entrypoint: .sql.bz2 bzip2 compressed file processing" {
  cname="${TEST_PREFIX}bzip2-sql"

  local temp_dir="/tmp/bzip2-sql-test-$$"
  mkdir -p "$temp_dir"

  cat > "$temp_dir/01-init.sql" << 'EOF'
CREATE DATABASE IF NOT EXISTS bzip2_sql_db;
USE bzip2_sql_db;
CREATE TABLE orders (order_id INT PRIMARY KEY, customer VARCHAR(100), total DECIMAL(10,2), status VARCHAR(20));
INSERT INTO orders VALUES (1, 'John Doe', 150.00, 'completed');
INSERT INTO orders VALUES (2, 'Jane Smith', 250.50, 'pending');
INSERT INTO orders VALUES (3, 'Bob Johnson', 75.25, 'completed');
CREATE USER IF NOT EXISTS 'bzip2user'@'%' IDENTIFIED BY 'bzip2pass';
GRANT ALL ON `bzip2_sql_db`.* TO 'bzip2user'@'%';
GRANT USAGE ON *.* TO 'bzip2user'@'%';
EOF
  bzip2 "$temp_dir/01-init.sql"

  run_container_with_port "$cname" 3306 -e DOLT_ROOT_HOST=% -v "$temp_dir:/docker-entrypoint-initdb.d"

  run docker exec "$cname" dolt sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "bzip2_sql_db" ]] || false

  run docker exec "$cname" dolt sql -q "USE bzip2_sql_db; SELECT SUM(total) as total FROM orders;"
  [ $status -eq 0 ]
  [[ "$output" =~ "475.75" ]] || false

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u bzip2user --password=bzip2pass -e "USE bzip2_sql_db; INSERT INTO orders VALUES (4, 'Alice Brown', 99.99, 'shipped');"
  [ $status -eq 0 ]

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u bzip2user --password=bzip2pass -e "USE bzip2_sql_db; SELECT customer FROM orders WHERE status='completed';"
  [ $status -eq 0 ]
  [[ "$output" =~ "John Doe" ]] || false
  [[ "$output" =~ "Bob Johnson" ]] || false

  rm -rf "$temp_dir"
}

# bats test_tags=no_lambda
@test "docker-entrypoint: .sql.xz xz compressed file processing" {
  cname="${TEST_PREFIX}xz-sql"

  local temp_dir="/tmp/xz-sql-test-$$"
  mkdir -p "$temp_dir"

  cat > "$temp_dir/01-init.sql" << 'EOF'
CREATE DATABASE IF NOT EXISTS xz_sql_db;
USE xz_sql_db;
CREATE TABLE inventory (item_id INT PRIMARY KEY, item_name VARCHAR(100), quantity INT, location VARCHAR(50));
INSERT INTO inventory VALUES (1, 'Laptop', 50, 'Warehouse A');
INSERT INTO inventory VALUES (2, 'Mouse', 200, 'Warehouse B');
INSERT INTO inventory VALUES (3, 'Keyboard', 150, 'Warehouse A');
INSERT INTO inventory VALUES (4, 'Monitor', 75, 'Warehouse C');
CREATE USER IF NOT EXISTS 'xzuser'@'%' IDENTIFIED BY 'xzpass';
GRANT ALL ON `xz_sql_db`.* TO 'xzuser'@'%';
GRANT USAGE ON *.* TO 'xzuser'@'%';
EOF
  xz "$temp_dir/01-init.sql"

  run_container_with_port "$cname" 3306 -e DOLT_ROOT_HOST=% -v "$temp_dir:/docker-entrypoint-initdb.d"

  run docker exec "$cname" dolt sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "xz_sql_db" ]] || false

  run docker exec "$cname" dolt sql -q "USE xz_sql_db; SELECT COUNT(*) as count FROM inventory;"
  [ $status -eq 0 ]
  [[ "$output" =~ "4" ]] || false

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u xzuser --password=xzpass -e "USE xz_sql_db; UPDATE inventory SET quantity = 60 WHERE item_name='Laptop';"
  [ $status -eq 0 ]

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u xzuser --password=xzpass -e "USE xz_sql_db; SELECT item_name, location FROM inventory WHERE location='Warehouse A';"
  [ $status -eq 0 ]
  [[ "$output" =~ "Laptop" ]] || false
  [[ "$output" =~ "Keyboard" ]] || false

  rm -rf "$temp_dir"
}

# bats test_tags=no_lambda
@test "docker-entrypoint: .sql.zst zstd compressed file processing" {
  cname="${TEST_PREFIX}zstd-sql"

  local temp_dir="/tmp/zstd-sql-test-$$"
  mkdir -p "$temp_dir"

  cat > "$temp_dir/01-init.sql" << 'EOF'
CREATE DATABASE IF NOT EXISTS zstd_sql_db;
USE zstd_sql_db;
CREATE TABLE employees (emp_id INT PRIMARY KEY, name VARCHAR(100), department VARCHAR(50), salary DECIMAL(10,2));
INSERT INTO employees VALUES (1, 'Alice Johnson', 'Engineering', 95000.00);
INSERT INTO employees VALUES (2, 'Bob Smith', 'Marketing', 75000.00);
INSERT INTO employees VALUES (3, 'Carol Williams', 'Engineering', 88000.00);
INSERT INTO employees VALUES (4, 'David Brown', 'Sales', 82000.00);
INSERT INTO employees VALUES (5, 'Eve Davis', 'HR', 70000.00);
CREATE USER IF NOT EXISTS 'zstduser'@'%' IDENTIFIED BY 'zstdpass';
GRANT ALL ON `zstd_sql_db`.* TO 'zstduser'@'%';
GRANT USAGE ON *.* TO 'zstduser'@'%';
EOF
  zstd -q "$temp_dir/01-init.sql"
  rm -f "$temp_dir/01-init.sql"  # zstd keeps original by default

  run_container_with_port "$cname" 3306 -e DOLT_ROOT_HOST=% -v "$temp_dir:/docker-entrypoint-initdb.d"

  run docker exec "$cname" dolt sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "zstd_sql_db" ]] || false

  run docker exec "$cname" dolt sql -q "USE zstd_sql_db; SELECT COUNT(*) as count FROM employees;"
  [ $status -eq 0 ]
  [[ "$output" =~ "5" ]] || false

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u zstduser --password=zstdpass -e "USE zstd_sql_db; SELECT name FROM employees WHERE department='Engineering' AND salary > 90000;"
  [ $status -eq 0 ]
  [[ "$output" =~ "Alice Johnson" ]] || false

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u zstduser --password=zstdpass -e "USE zstd_sql_db; SELECT department, COUNT(*) as count FROM employees GROUP BY department ORDER BY count DESC;"
  [ $status -eq 0 ]
  [[ "$output" =~ "Engineering" ]] || false

  rm -rf "$temp_dir"
}

# bats test_tags=no_lambda
@test "docker-entrypoint: load employees database dump (.sql)" {
  cname="${TEST_PREFIX}dump"
  local temp_dir="/tmp/employees-dump-test-$$"
  mkdir -p "$temp_dir"

  curl -sL "https://raw.githubusercontent.com/datacharmer/test_db/master/load_employees.dump" -o "$temp_dir/load_employees.sql"
  sed -i '1iCREATE TABLE employees (id int, dob text, fn text, ln text, g text, dod text);' "$temp_dir/load_employees.sql"

  run_container_with_port "$cname" 3306 \
    -e DOLT_DATABASE=test \
    -v "$temp_dir:/docker-entrypoint-initdb.d"

  run docker exec "$cname" dolt sql -q "SHOW TABLES;"
  [ $status -eq 0 ]
  [[ "$output" =~ test.*employees ]] || false

  run docker exec "$cname" dolt sql -q "SELECT COUNT(*) > 0 AS has_rows FROM employees;"
  [ $status -eq 0 ]
  [[ "$output" =~ "1" ]] || false

  rm -rf "$temp_dir"
}

# bats test_tags=no_lambda
@test "docker-entrypoint: error on invalid database dump (.sql)" {
  cname="${TEST_PREFIX}bad-dump"
  local temp_dir="/tmp/employees-dump-$$"
  mkdir -p "$temp_dir"

  curl -sL "https://raw.githubusercontent.com/datacharmer/test_db/master/load_employees.dump" -o "$temp_dir/load_employees.sql"

  run docker run -d --name "$cname" -v "$temp_dir:/docker-entrypoint-initdb.d" "$TEST_IMAGE"
  [ $status -eq 0 ]

  docker wait "$cname"
  docker logs "$cname" >/tmp/${cname}.log 2>&1
  run grep -F "ERROR" /tmp/${cname}.log
  [ $status -eq 0 ]

  rm -rf "$temp_dir"
}

# bats test_tags=no_lambda
@test "docker-entrypoint: GRANT privileges in init script does not require CREATE USER" {
  # Customer error: "error on line 5 for query GRANT ALL PRIVILEGES ON *.* TO 'nautobot'@'%':
  #                  You are not allowed to create a user with GRANT"

  cname="${TEST_PREFIX}grant-init"

  local temp_dir="/tmp/grant-init-$$"
  mkdir -p "$temp_dir"

  cat > "$temp_dir/01-correct-grants.sql" << 'EOF'
CREATE DATABASE IF NOT EXISTS nautobot_db;
GRANT ALL PRIVILEGES ON *.* TO 'nautobot'@'%';
FLUSH PRIVILEGES;
EOF

  run_container_with_port "$cname" 3306 -e DOLT_USER_HOST=% -e DOLT_USER=nautobot -e DOLT_PASSWORD=nautobotpass -v "$temp_dir:/docker-entrypoint-initdb.d"

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u nautobot --password=nautobotpass -e "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "nautobot_db" ]] || false

  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3306 -u nautobot --password=nautobotpass -e "CREATE DATABASE nautobot_test; USE nautobot_test; CREATE TABLE test (id INT);"
  [ $status -eq 0 ]

  rm -rf "$temp_dir"
}

# bats test_tags=no_lambda
@test "docker-entrypoint: CREATE SCHEMA without database name" {
  # DBeaver creates schemas (databases) without specifying a database name
  cname="${TEST_PREFIX}create-schema"
  usr="testuser"
  pwd="testpass"
  
  run_container_with_port "$cname" 3306 \
    -e DOLT_ROOT_PASSWORD=rootpass \
    -e DOLT_ROOT_HOST=% \
    -e DOLT_USER="$usr" \
    -e DOLT_PASSWORD="$pwd"

  run docker exec "$cname" dolt -u "root" -p "rootpass" sql -q "CREATE SCHEMA dbeaver_test;"
  [ $status -eq 0 ]

  run docker exec "$cname" dolt -u "root" -p "rootpass" sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "dbeaver_test" ]] || false

  run docker exec "$cname" dolt -u "root" -p "rootpass" sql -q "USE dbeaver_test; CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(50));"
  [ $status -eq 0 ]

  run docker exec "$cname" dolt -u "root" -p "rootpass" sql -q "USE dbeaver_test; INSERT INTO test_table VALUES (1, 'test data');"
  [ $status -eq 0 ]

  run docker exec "$cname" dolt -u "root" -p "rootpass" sql -q "USE dbeaver_test; SELECT * FROM test_table;"
  [ $status -eq 0 ]
  [[ "$output" =~ "test data" ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: latest binary build from dolt directory" {
  cname="${TEST_PREFIX}latest-docker"
  
  LATEST_IMAGE="dolt-entrypoint-latest:test"
  EXPECTED_VERSION=$(curl -s https://api.github.com/repos/dolthub/dolt/releases/latest | grep '"tag_name"' | cut -d'"' -f4 | sed 's/^v//')
  cd "$WORKSPACE_ROOT/dolt"
  docker build -f docker/serverDockerfile --build-arg DOLT_VERSION=latest -t "$LATEST_IMAGE" .
  
  docker run -d --name "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=% "$LATEST_IMAGE" >/dev/null
  wait_for_log "$cname" "Server ready. Accepting connections."
  wait_for_log "$cname" "Dolt init process done. Ready for connections."
  
  run docker exec "$cname" dolt version
  [ $status -eq 0 ]
  
  INSTALLED_VERSION=$(docker exec "$cname" dolt version | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1)
  [ "$INSTALLED_VERSION" = "$EXPECTED_VERSION" ]

  run docker exec "$cname" dolt -u "root" -p "rootpass" sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]

  docker rmi "$LATEST_IMAGE" >/dev/null 2>&1 || true
}

# bats test_tags=no_lambda
@test "docker-entrypoint: specific version binary build from dolt directory" {
  cname="${TEST_PREFIX}specific-version"
  
  SPECIFIC_IMAGE="dolt-entrypoint-specific:test"
  SPECIFIC_VERSION="1.34.0"
  echo "Building Dolt from docker directory with specific version: $SPECIFIC_VERSION"
  cd "$WORKSPACE_ROOT/dolt"
  docker build -f docker/serverDockerfile --build-arg DOLT_VERSION="$SPECIFIC_VERSION" -t "$SPECIFIC_IMAGE" .
  
  docker run -d --name "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=% "$SPECIFIC_IMAGE" >/dev/null
  wait_for_log "$name" "Server ready. Accepting connections."
  wait_for_log "$name" "Dolt init process done. Ready for connections."
  
  run docker exec "$cname" dolt version
  [ $status -eq 0 ]
  
  INSTALLED_VERSION=$(docker exec "$cname" dolt version | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1)
  [ "$INSTALLED_VERSION" = "$SPECIFIC_VERSION" ]
  
  run docker exec "$cname" dolt sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]

  docker rmi "$SPECIFIC_IMAGE" >/dev/null 2>&1 || true
}

# bats test_tags=no_lambda
@test "docker-entrypoint: multiple server startups at the same time to confirm no race conditions" {
  cname="${TEST_PREFIX}multi"
  db="testdb"
  usr="testuser"
  pwd="testpass"

  # Start all containers at sequentially, this stresses the system and causes timing issues if
  # hardware resources are in use. The containers should still start correctly with the wait
  # logic in place.
  local pids=()
  for cycle in {1..40}; do
    db_name="${db}_${cycle}_$$"

    (
      docker run -d --name "$cname-$cycle" \
        -v /var/lib/dolt \
        -e DOLT_ROOT_PASSWORD=rootpass \
        -e DOLT_ROOT_HOST=% \
        -e DOLT_DATABASE="$db_name" \
        -e DOLT_USER="$usr" \
        -e DOLT_PASSWORD="$pwd" \
        "$TEST_IMAGE" >/dev/null 2>&1
    ) &
    pids+=($!)
  done

  for pid in "${pids[@]}"; do
    wait $pid
  done

  # Wait for all servers to be ready
  for cycle in {1..40}; do
    if ! wait_for_log "$cname-$cycle" "Server ready. Accepting connections." 30; then
      docker logs "$cname-$cycle"
      false
    fi

    if ! wait_for_log "$cname-$cycle" "Dolt init process done. Ready for connections." 15; then
      docker logs "$cname-$cycle"
      false
    fi
  done

  for cycle in {1..40}; do
    run docker logs "$cname-$cycle" 2>&1
    [ $status -eq 0 ]
    if echo "$output" | grep -i "ERROR" >/dev/null; then
      echo "$output"
      false
    fi
    if ! [[ "$output" =~ "Server ready. Accepting connections" ]]; then
      echo "$output"
      false
    fi
    if ! [[ "$output" =~ "Dolt init process done. Ready for connections." ]]; then
      echo "$output"
      false
    fi
  done

  local stop_pids=()
  for cycle in {1..40}; do
    docker stop "$cname-$cycle" >/dev/null &
    stop_pids+=($!)
  done

  for pid in "${stop_pids[@]}"; do
    wait $pid
  done
}

# bats test_tags=no_lambda
@test "docker-entrypoint: server logs are parsed into entrypoint log format" {
  cname="${TEST_PREFIX}server-log"

  run_container_with_port "$cname" 3306
  docker logs "$cname" >/tmp/${cname}.log 2>&1
  run grep -F "[0] [System] [Dolt] [Server]" /tmp/"${cname}".log | grep -F -v "level="
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "docker-entrypoint: DOLT_RAW is respected" {
  cname="${TEST_PREFIX}dolt-raw"

  run_container_with_port "$cname" 3306 -e DOLT_RAW=1
  docker logs "$cname" >/tmp/${cname}.log 2>&1
  run grep -F "level=" /tmp/"${cname}".log | grep -F -v "[0] [System] [Dolt] [Server]"
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "docker-entrypoint: server debug logs passthrough unmodified" {
  cname="${TEST_PREFIX}debug"

  run docker run -d --name "$cname" "$TEST_IMAGE" -l debug
  wait_for_log "$cname" "Dolt init process done. Ready for connections."
  docker logs "$cname" >/tmp/${cname}.log 2>&1

  run grep -F "level=debug" /tmp/"${cname}".log
  [ "$status" -eq 0 ]

  run grep -F "[0] [System] [Dolt] [Server]" /tmp/"${cname}".log
  [ "$status" -eq 0 ]
}

# bats test_tags=no_lambda
@test "docker-entrypoint: init sql file prints table contents to stdout" {
  cname="${TEST_PREFIX}plain-sql"
  temp_dir="/tmp/plain-sql-$$"
  mkdir -p "$temp_dir"

  cat > "$temp_dir/01-init.sql" << 'EOF'
CREATE DATABASE IF NOT EXISTS plain_sql_db;
USE plain_sql_db;
CREATE TABLE users_table (id INT PRIMARY KEY, username VARCHAR(50), email VARCHAR(100));
INSERT INTO users_table VALUES (1, 'alice', 'alice@example.com');
INSERT INTO users_table VALUES (2, 'bob', 'bob@example.com');
SELECT * FROM users_table;
EOF

  run_container_with_port "$cname" 3306 -e DOLT_ROOT_HOST=% -v "$temp_dir:/docker-entrypoint-initdb.d"
  docker logs "$cname" >/tmp/${cname}.log 2>&1

  # Verify the table header
  run grep -F "| id | username | email             |" /tmp/"${cname}".log
  [ "$status" -eq 0 ]

  # Verify rows
  run grep -F "| 1  | alice    | alice@example.com |" /tmp/"${cname}".log
  [ "$status" -eq 0 ]

  run grep -F "| 2  | bob      | bob@example.com   |" /tmp/"${cname}".log
  [ "$status" -eq 0 ]

  rm -rf "$temp_dir"
}
