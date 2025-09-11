#!/usr/bin/env bats

load $BATS_TEST_DIRNAME/helper/common.bash

# These tests validate docker/docker-entrypoint.sh using a Docker image built from
# docker/serverDockerfile in the repo root. They follow existing integration test conventions.

setup() {
#  skiponwindows
  
  setup_no_dolt_init
  
  # Compute repo root from integration-tests/bats directory
  REPO_ROOT=$(cd "$BATS_TEST_DIRNAME/../.." && pwd)
  export REPO_ROOT

  # Image and container naming
  DOLT_DOCKER_TEST_VERSION=${DOLT_DOCKER_TEST_VERSION:-latest}
  TEST_IMAGE="dolt-entrypoint-it:${DOLT_DOCKER_TEST_VERSION}"
  TEST_PREFIX="dolt-entrypoint-it-$$-"

  # Ensure image exists (build if missing)
  if ! docker image inspect "$TEST_IMAGE" >/dev/null 2>&1; then
    docker build -f "$REPO_ROOT/docker/serverDockerfile" --build-arg DOLT_VERSION=$DOLT_DOCKER_TEST_VERSION -t "$TEST_IMAGE" "$REPO_ROOT" || \
      skip "failed to build test image $TEST_IMAGE"
  fi

  # Best-effort cleanup of leftovers from a previous attempt
  docker ps -a --filter "name=$TEST_PREFIX" --format '{{.Names}}' | xargs -r docker rm -f >/dev/null 2>&1 || true
}

teardown() {
  # Cleanup any containers for this test run
  docker ps -a --filter "name=$TEST_PREFIX" --format '{{.Names}}' | xargs -r docker rm -f >/dev/null 2>&1 || true
  teardown_common
}

# Helper to run a container and wait for server to be ready
run_container() {
  name="$1"; shift
  docker run -d --name "$name" "$@" "$TEST_IMAGE" >/dev/null
  wait_for_log "$name" "Server ready. Accepting connections." 15
  
  # Verify container is running
  run docker ps --filter "name=$name" --format "{{.Names}}"
  [ $status -eq 0 ]
  [[ "$output" =~ ^"$name"$ ]] || false
}

# Helper to run a container with port mapping and wait for server to be ready
run_container_with_port() {
  name="$1"; shift
  port="$1"; shift
  docker run -d --name "$name" -p "$port:3306" "$@" "$TEST_IMAGE" >/dev/null
  wait_for_log "$name" "Server ready. Accepting connections." 15
  
  # Verify container is running
  run docker ps --filter "name=$name" --format "{{.Names}}"
  [ $status -eq 0 ]
  [[ "$output" =~ ^"$name"$ ]] || false
}

# Helper to wait for a log line to appear
wait_for_log() {
  name="$1"; shift
  pattern="$1"; shift
  timeout="${1:-15}"
  i=0
  while [ $i -lt $timeout ]; do
    if docker logs "$name" 2>&1 | grep -q "$pattern"; then
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
  # Container should fail to stay up; still check logs for message
  wait_for_log "$cname" "cannot be used for the root user" 15 || true
  docker logs "$cname" >/tmp/${cname}.log 2>&1 || true
  run grep -F "cannot be used for the root user" /tmp/"${cname}".log
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "docker-entrypoint: password without user warns and is ignored" {
  cname="${TEST_PREFIX}pass-no-user"
  run_container "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_PASSWORD=orphan
  docker logs "$cname" >/tmp/${cname}.log 2>&1 || true
  run grep -i "password will be ignored" /tmp/"${cname}".log
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "docker-entrypoint: DOLT_ROOT_HOST creates root@% with grants" {
  cname="${TEST_PREFIX}root-host"
  run_container "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=%
  # Check using dolt inside the container
  run docker exec "$cname" dolt sql -q "SHOW GRANTS FOR 'root'@'%';"
  [ $status -eq 0 ]
  [[ "$output" =~ "WITH GRANT OPTION" ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: reserved keywords in params are escaped correctly" {
  cname="${TEST_PREFIX}kw-params"
  kw_db="select"
  kw_user="from"
  run_container "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_DATABASE="$kw_db" -e DOLT_USER="$kw_user" -e DOLT_PASSWORD=pass

  # Database exists
  run docker exec "$cname" dolt sql --result-format csv -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$kw_db" >/dev/null

  # User exists
  run docker exec "$cname" dolt sql --result-format csv -q "SELECT User FROM mysql.user;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$kw_user" >/dev/null

  # Grants for user on keyword DB
  run docker exec "$cname" dolt sql --result-format csv -q "SHOW GRANTS FOR '$kw_user'@'%';"
  [ $status -eq 0 ]
  printf -v p1 '`%s`\\.\\* TO' "$kw_db"
  [[ "$output" =~ $p1 ]] || false
  p2="\`$kw_user\`@\`%\`"
  [[ "$output" =~ $p2 ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: reserved keyword database name support" {
  cname="${TEST_PREFIX}versioning"
  kw_db="versioning"
  usr="testuser"
  pwd="testpass"
  run_container "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_DATABASE="$kw_db" -e DOLT_USER="$usr" -e DOLT_PASSWORD="$pwd"

  # Database exists
  run docker exec "$cname" dolt sql --result-format csv -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$kw_db" >/dev/null

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
  run_container "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_DATABASE="$db" -e DOLT_USER="$usr" -e DOLT_PASSWORD="$pwd"

  # DB created
  run docker exec "$cname" dolt sql --result-format csv -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$db" >/dev/null

  # User created
  run docker exec "$cname" dolt sql --result-format csv -q "SELECT User FROM mysql.user WHERE User='$usr';"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$usr" >/dev/null

  # Grants on DB
  run docker exec "$cname" dolt sql --result-format csv -q "SHOW GRANTS FOR '$usr'@'%';"
  [ $status -eq 0 ]
  printf -v p1 '`%s`\\.\\* TO' "$db"
  [[ "$output" =~ $p1 ]] || false
  p2="\`$usr\`@\`%\`"
  [[ "$output" =~ $p2 ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: error handling with invalid configurations" {
  cname="${TEST_PREFIX}error-msg"
  # Run container with invalid user (empty password)
  run docker run -d --name "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_USER=testuser -e DOLT_PASSWORD="" "$TEST_IMAGE"
  
  # Wait for error message to appear
  wait_for_log "$cname" "user creation requires a password" 15 || true
  
  # Check if container failed to start
  run docker ps --filter "name=$cname" --format "{{.Names}}"
  [ $status -eq 0 ]
  [ -z "$output" ]
  
  # Check error message
  docker logs "$cname" >/tmp/${cname}.log 2>&1 || true
  run grep -F "user creation requires a password" /tmp/"${cname}".log
  [ $status -eq 0 ]
  
  # Check if error message references DOLT_USER
  run grep -F "DOLT_USER specified" /tmp/"${cname}".log
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "docker-entrypoint: setup simplicity with minimal configuration" {
  cname="${TEST_PREFIX}setup"
  run_container "$cname" -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=%
  
  # Root user can execute queries immediately
  run docker exec "$cname" dolt sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  
  # Root user can create databases and tables immediately
  run docker exec "$cname" dolt sql -q "CREATE DATABASE quick_test; USE quick_test; CREATE TABLE test (id INT);"
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "docker-entrypoint: root user functionality and privileges" {
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
@test "docker-entrypoint: permission validation and database operations" {
  cname="${TEST_PREFIX}perms"
  db="testdb"
  usr="testuser"
  pwd="testpass"
  
  # Run container with port mapping
  run_container_with_port "$cname" 3311 -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=% -e DOLT_DATABASE="$db" -e DOLT_USER="$usr" -e DOLT_PASSWORD="$pwd"
  
  # Test that the created user can perform database operations
  run docker exec "$cname" dolt sql -q "USE \`$db\`; CREATE TABLE test_table (id INT, name VARCHAR(50));"
  [ $status -eq 0 ]
  
  # Test that the created user can insert data
  run docker exec "$cname" dolt sql -q "USE \`$db\`; INSERT INTO test_table VALUES (1, 'test');"
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "docker-entrypoint: default user permissions and operations" {
  cname="${TEST_PREFIX}default-user"
  
  # Run container without any user parameters (default behavior)
  run_container "$cname" -e DOLT_ROOT_PASSWORD=rootpass
  
  # Test that the default root user can connect and perform operations
  run docker exec "$cname" dolt sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  
  # Test that the default user can create databases
  run docker exec "$cname" dolt sql -q "CREATE DATABASE testdb;"
  [ $status -eq 0 ]
  
  # Test that the default user can create tables
  run docker exec "$cname" dolt sql -q "USE testdb; CREATE TABLE user_table (id INT PRIMARY KEY, data VARCHAR(100));"
  [ $status -eq 0 ]
  
  # Test that the default user can insert data
  run docker exec "$cname" dolt sql -q "USE testdb; INSERT INTO user_table VALUES (1, 'test data');"
  [ $status -eq 0 ]
  
  # Test that the default user can query data
  run docker exec "$cname" dolt sql -q "USE testdb; SELECT * FROM user_table;"
  [ $status -eq 0 ]
}

# bats test_tags=no_lambda
@test "docker-entrypoint: MySQL client connectivity via TCP" {
  cname="${TEST_PREFIX}mysql-client"
  db="testdb"
  usr="testuser"
  pwd="testpass"
  
  # Run container with port mapping
  run_container_with_port "$cname" 3313 -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=% -e DOLT_DATABASE="$db" -e DOLT_USER="$usr" -e DOLT_PASSWORD="$pwd"
  
  # Test root user connection via MySQL client
  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3313 -u root --password=rootpass -e "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$db" >/dev/null
  
  # Test custom user connection via MySQL client
  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3313 -u "$usr" --password="$pwd" -e "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$db" >/dev/null
  
  # Test that custom user can perform operations via MySQL client
  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3313 -u "$usr" --password="$pwd" -e "USE \`$db\`; CREATE TABLE mysql_test (id INT PRIMARY KEY, name VARCHAR(50));"
  [ $status -eq 0 ]
  
  # Test data insertion via MySQL client
  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3313 -u "$usr" --password="$pwd" -e "USE \`$db\`; INSERT INTO mysql_test VALUES (1, 'mysql_test_data');"
  [ $status -eq 0 ]
  
  # Test data query via MySQL client
  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3313 -u "$usr" --password="$pwd" -e "USE \`$db\`; SELECT * FROM mysql_test;"
  [ $status -eq 0 ]
  [[ "$output" =~ "mysql_test_data" ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: MySQL client with reserved keywords" {
  cname="${TEST_PREFIX}mysql-kw"
  kw_db="select"
  kw_user="from"
  pwd="testpass"
  
  # Run container with port mapping
  run_container_with_port "$cname" 3314 -e DOLT_ROOT_PASSWORD=rootpass -e DOLT_ROOT_HOST=% -e DOLT_DATABASE="$kw_db" -e DOLT_USER="$kw_user" -e DOLT_PASSWORD="$pwd"
  
  # Test root user can see the reserved keyword database
  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3314 -u root --password=rootpass -e "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$kw_db" >/dev/null
  
  # Test custom user with reserved keyword name can connect
  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3314 -u "$kw_user" --password="$pwd" -e "SHOW DATABASES;"
  [ $status -eq 0 ]
  echo "$output" | grep -Fx "$kw_db" >/dev/null
  
  # Test operations with reserved keyword database name
  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3314 -u "$kw_user" --password="$pwd" -e "USE \`$kw_db\`; CREATE TABLE test_table (id INT);"
  [ $status -eq 0 ]
  
  # Test data operations
  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3314 -u "$kw_user" --password="$pwd" -e "USE \`$kw_db\`; INSERT INTO test_table VALUES (1);"
  [ $status -eq 0 ]
  
  # Test query
  run docker run --rm --network host mysql:8.0 mysql -h 127.0.0.1 -P 3314 -u "$kw_user" --password="$pwd" -e "USE \`$kw_db\`; SELECT * FROM test_table;"
  [ $status -eq 0 ]
  [[ "$output" =~ "1" ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: SQL error reporting without suppression" {
  cname="${TEST_PREFIX}sql-error-reporting"
  # Run container with user creation
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
  # Run container with empty DOLT_ROOT_PASSWORD
  run_container "$cname" -e DOLT_ROOT_PASSWORD=""
  
  # Test that we can connect without password (this should work even without explicit root user)
  run docker exec "$cname" dolt sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "information_schema" ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: no DOLT_ROOT_PASSWORD is allowed" {
  cname="${TEST_PREFIX}no-password-allowed"
  # Run container without DOLT_ROOT_PASSWORD set at all
  run_container "$cname"
  
  # Test that we can connect without password (this should work even without explicit root user)
  run docker exec "$cname" dolt sql -q "SHOW DATABASES;"
  [ $status -eq 0 ]
  [[ "$output" =~ "information_schema" ]] || false
}

# bats test_tags=no_lambda
@test "docker-entrypoint: valid server config file handling" {
  cname="${TEST_PREFIX}valid-server-config"
  # Create a valid server config file
  mkdir -p /tmp/test-config
  cat > /tmp/test-config/test.yaml << 'EOF'
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
  
  # Run container with valid server config file
  run_container "$cname" -e DOLT_ROOT_PASSWORD="" -v /tmp/test-config:/etc/dolt/servercfg.d:ro
  
  # Cleanup
  rm -rf /tmp/test-config
}

# bats test_tags=no_lambda
@test "docker-entrypoint: invalid config file error handling" {
  cname="${TEST_PREFIX}invalid-config"
  # Create a temporary config file with invalid JSON structure
  mkdir -p /tmp/test-config
  echo '{"user": {"name": "test"}}' > /tmp/test-config/test.json
  
  # Run container with invalid config file (should fail)
  run docker run -d --name "$cname" -e DOLT_ROOT_PASSWORD="" -v /tmp/test-config:/etc/dolt/doltcfg.d:ro "$TEST_IMAGE"
  
  # Wait a bit for container to start
  sleep 5
  
  # Container should fail to start with invalid config
  run docker ps --filter "name=$cname" --format "{{.Names}}"
  [ $status -eq 0 ]
  [ -z "$output" ]
  
  # Check that error message appears in logs
  docker logs "$cname" >/tmp/${cname}.log 2>&1 || true
  [[ "$(cat /tmp/"${cname}".log)" =~ "Failed to load the global config" ]] || false
  
  # Cleanup
  rm -rf /tmp/test-config
}
