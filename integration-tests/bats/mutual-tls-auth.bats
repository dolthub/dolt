#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

REQUIRE_CLIENT_CERT=false

setup() {
    skiponwindows "tests are flaky on Windows"
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test tests remote connections directly, SQL_ENGINE is not needed."
    fi

    export CERTS_DIR=$PWD/certs
    setup_no_dolt_init
    dolt init
}

teardown() {
    stop_sql_server 1 && sleep 0.5
    rm -rf $BATS_TMPDIR/sql-server-test$$
    teardown_common
}

start_sql_server_with_TLS() {
    PORT=$( definePORT )
    cat >config.yml <<EOF
log_level: debug
behavior:
  disable_client_multi_statements: true
listener:
  host: "0.0.0.0"
  port: $PORT
  require_client_cert: $REQUIRE_CLIENT_CERT
  tls_cert:  $CERTS_DIR/server-cert.pem
  tls_key:   $CERTS_DIR/server-key.pem
EOF

    dolt sql-server --config ./config.yml --socket "dolt.$PORT.sock" &
    SERVER_PID=$!
    sleep 1
}

start_sql_server_with_TLS_and_CA() {
    PORT=$( definePORT )
    cat >config.yml <<EOF
log_level: debug
behavior:
  disable_client_multi_statements: true
listener:
  host: "0.0.0.0"
  port: $PORT
  require_client_cert: $REQUIRE_CLIENT_CERT
  ca_cert:   $CERTS_DIR/ca.pem
  tls_cert:  $CERTS_DIR/server-cert.pem
  tls_key:   $CERTS_DIR/server-key.pem
EOF

    dolt sql-server --config ./config.yml --socket "dolt.$PORT.sock" &
    SERVER_PID=$!
    sleep 1
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth fails when SSL is required, but not present" {
  start_sql_server
  dolt sql -q "create user user1@'%' REQUIRE SSL;"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT --ssl_mode=DISABLED -e "SELECT 123;"
  [ "$status" -ne 0 ]
  [[ "$output" =~ "ERROR 1045 (28000): Access denied for user 'user1'" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth works when SSL is required" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' REQUIRE SSL;"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;"
  echo "PWD: $(pwd)"
  echo "OUTPUT: $output"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "123" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth fails when X509 is required, but no TLS connection" {
  start_sql_server
  dolt sql -q "create user user1@'%' REQUIRE X509;"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT --ssl_mode=DISABLED -e "SELECT 123;"
  [ "$status" -ne 0 ]
  [[ "$output" =~ "ERROR 1045 (28000): Access denied for user 'user1'" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth fails when X509 is required, but no client cert" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' REQUIRE X509;"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;"
  [ "$status" -ne 0 ]
  [[ "$output" =~ "ERROR 1045 (28000): Access denied for user 'user1'" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: client cert auth works without a password (mysql_native_password)" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' IDENTIFIED WITH mysql_native_password REQUIRE X509;"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
    --ssl-mode=VERIFY_CA \
    --ssl-ca=$CERTS_DIR/ca.pem \
    --ssl-cert=$CERTS_DIR/client-cert.pem \
    --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -eq 0 ]
  [[ "$output" =~ "123" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: client cert auth works with a password (mysql_native_password)" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' IDENTIFIED WITH mysql_native_password BY 'pass1' REQUIRE X509;"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
      --ssl-mode=VERIFY_CA -ppass1 \
      --ssl-ca=$CERTS_DIR/ca.pem \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -eq 0 ]
  [[ "$output" =~ "123" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: client cert auth fails with wrong password (mysql_native_password)" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' IDENTIFIED WITH mysql_native_password BY 'pass1' REQUIRE X509;"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
      --ssl-mode=VERIFY_CA -pwrongpassword \
      --ssl-ca=$CERTS_DIR/ca.pem \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -ne 0 ]
  [[ "$output" =~ "ERROR 1045 (28000): Access denied for user 'user1'" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: client cert auth works without a password (caching_sha2_password)" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' IDENTIFIED WITH caching_sha2_password REQUIRE X509;"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
    --ssl-mode=VERIFY_CA \
    --ssl-ca=$CERTS_DIR/ca.pem \
    --ssl-cert=$CERTS_DIR/client-cert.pem \
    --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -eq 0 ]
  [[ "$output" =~ "123" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: client cert auth works with a password (caching_sha2_password)" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' IDENTIFIED WITH caching_sha2_password BY 'pass1' REQUIRE X509;"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
      --ssl-mode=VERIFY_CA -ppass1 \
      --ssl-ca=$CERTS_DIR/ca.pem \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -eq 0 ]
  [[ "$output" =~ "123" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: client cert auth fails with wrong password (caching_sha2_password)" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' IDENTIFIED WITH caching_sha2_password BY 'pass1' REQUIRE X509;"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
      --ssl-mode=VERIFY_CA -pwrongpassword \
      --ssl-ca=$CERTS_DIR/ca.pem \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -ne 0 ]
  [[ "$output" =~ "ERROR 1045 (28000): Access denied for user 'user1'" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth fails when the client cert is expired" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' REQUIRE X509;"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
      --ssl-mode=VERIFY_CA \
      --ssl-ca=$CERTS_DIR/ca.pem \
      --ssl-cert=$CERTS_DIR/expired-cert.pem \
      --ssl-key=$CERTS_DIR/expired-key.pem
  [ "$status" -ne 0 ]
  # NOTE: This is the same error message returned by MySQL
  [[ "$output" =~ "ERROR 2013 (HY000): Lost connection to MySQL server at 'reading authorization packet'" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth fails when client cert uses different CA" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' REQUIRE X509;"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
      --ssl-mode=VERIFY_CA -pwrongpassword \
      --ssl-ca=$CERTS_DIR/alt-ca-cert.pem \
      --ssl-cert=$CERTS_DIR/alt-client-cert.pem \
      --ssl-key=$CERTS_DIR/alt-client-key.pem
  [ "$status" -ne 0 ]
  # NOTE: this is the exact same error message returned from MySQL
  [[ "$output" =~ "ERROR 2026 (HY000): SSL connection error: error:0A000086:SSL routines::certificate verify failed" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth fails for different SSL cipher" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' REQUIRE CIPHER 'unknown-cipher';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
      --tls-ciphersuites=TLS_AES_128_GCM_SHA256 \
      --ssl-mode=VERIFY_CA \
      --ssl-ca=$CERTS_DIR/ca.pem \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -ne 0 ]
  [[ "$output" =~ "ERROR 1045 (28000): Access denied for user 'user1'" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth works for matching SSL cipher (no password)" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' REQUIRE CIPHER 'TLS_AES_128_GCM_SHA256';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
      --tls-ciphersuites=TLS_AES_128_GCM_SHA256 \
      --ssl-mode=VERIFY_CA \
      --ssl-ca=$CERTS_DIR/ca.pem \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -eq 0 ]
  [[ "$output" =~ "123" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth works for matching SSL cipher (with password)" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' IDENTIFIED BY 'pass1' REQUIRE CIPHER 'TLS_AES_128_GCM_SHA256';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -ppass1 -e "SELECT 123;" \
      --tls-ciphersuites=TLS_AES_128_GCM_SHA256 \
      --ssl-mode=VERIFY_CA \
      --ssl-ca=$CERTS_DIR/ca.pem \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -eq 0 ]
  [[ "$output" =~ "123" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth fails for different cert issuer" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' REQUIRE ISSUER 'wrong-issuer';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
      --ssl-mode=VERIFY_CA \
      --ssl-ca=$CERTS_DIR/ca.pem \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -ne 0 ]
  [[ "$output" =~ "ERROR 1045 (28000): Access denied for user 'user1'" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth works for matching cert issuer (no password)" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' REQUIRE issuer '/C=US/ST=Washington/L=Seattle/O=Test CA/CN=MySQL Test CA';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
      --ssl-mode=VERIFY_CA \
      --ssl-ca=$CERTS_DIR/ca.pem \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -eq 0 ]
  [[ "$output" =~ "123" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth works for matching cert issuer (with password)" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' IDENTIFIED BY 'passw0rd' REQUIRE issuer '/C=US/ST=Washington/L=Seattle/O=Test CA/CN=MySQL Test CA';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -ppassw0rd -e "SELECT 123;" \
      --ssl-mode=VERIFY_CA \
      --ssl-ca=$CERTS_DIR/ca.pem \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -eq 0 ]
  [[ "$output" =~ "123" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth fails for different cert subject" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' REQUIRE SUBJECT 'wrong-subject';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
      --ssl-mode=VERIFY_CA \
      --ssl-ca=$CERTS_DIR/ca.pem \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -ne 0 ]
  [[ "$output" =~ "ERROR 1045 (28000): Access denied for user 'user1'" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth works for matching cert subject (no password)" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' REQUIRE SUBJECT '/C=US/ST=Washington/L=Seattle/O=Test Client/CN=testclient';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
      --ssl-mode=VERIFY_CA \
      --ssl-ca=$CERTS_DIR/ca.pem \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -eq 0 ]
  [[ "$output" =~ "123" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth works for matching cert subject (with password)" {
  start_sql_server_with_TLS_and_CA
  dolt sql -q "create user user1@'%' IDENTIFIED BY 'passpass' REQUIRE SUBJECT '/C=US/ST=Washington/L=Seattle/O=Test Client/CN=testclient';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  run mysql -uuser1 --protocol TCP --port $PORT -ppasspass -e "SELECT 123;" \
      --ssl-mode=VERIFY_CA \
      --ssl-ca=$CERTS_DIR/ca.pem \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -eq 0 ]
  [[ "$output" =~ "123" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth works with require_client_cert (with cert verification)" {
  dolt sql -q "create user user1@'%';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  REQUIRE_CLIENT_CERT=true
  start_sql_server_with_TLS_and_CA

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -eq 0 ]
  [[ "$output" =~ "123" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth works with require_client_cert (without cert verification)" {
  dolt sql -q "create user user1@'%';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  REQUIRE_CLIENT_CERT=true
  start_sql_server_with_TLS

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;" \
      --ssl-cert=$CERTS_DIR/client-cert.pem \
      --ssl-key=$CERTS_DIR/client-key.pem
  [ "$status" -eq 0 ]
  [[ "$output" =~ "123" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth fails with require_client_cert when TLS is disabled" {
  dolt sql -q "create user user1@'%';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  REQUIRE_CLIENT_CERT=true
  start_sql_server_with_TLS_and_CA

  run mysql --ssl-mode DISABLED -uuser1 --protocol TCP --port $PORT -e "SELECT 123;"
  [ "$status" -ne 0 ]
  [[ "$output" =~ "UNAVAILABLE" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth fails with require_client_cert (with cert verification)" {
  dolt sql -q "create user user1@'%';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  REQUIRE_CLIENT_CERT=true
  start_sql_server_with_TLS_and_CA

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;"
  [ "$status" -ne 0 ]
  [[ "$output" =~ "ERROR 2013 (HY000): Lost connection to MySQL server at 'reading authorization packet'" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: auth fails with require_client_cert (without cert verification)" {
  dolt sql -q "create user user1@'%';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  REQUIRE_CLIENT_CERT=true
  start_sql_server_with_TLS

  run mysql -uuser1 --protocol TCP --port $PORT -e "SELECT 123;"
  [ "$status" -ne 0 ]
  [[ "$output" =~ "ERROR 2013 (HY000): Lost connection to MySQL server at 'reading authorization packet'" ]] || false
}

# When a sql server is running, dolt sql will connect to it over the network. However, we don't have a
# valid client cert to use, so dolt sql isn't able to connect when require_client_cert is enabled.
# bats test_tags=no_lambda
@test "mutual-tls-auth: dolt sql doesn't work with a running server using require_client_cert" {
  dolt sql -q "create user user1@'%';"
  dolt sql -q "grant all privileges on *.* to user1@'%';"

  REQUIRE_CLIENT_CERT=true
  start_sql_server_with_TLS_and_CA

  run dolt sql -q "SELECT 1;"
  [ "$status" -ne 0 ]
  [[ "$output" =~ "UNAVAILABLE" ]] || false
}

# bats test_tags=no_lambda
@test "mutual-tls-auth: error case: require_client_cert set without tls cert and key" {
  PORT=$( definePORT )
  cat >config.yml <<EOF
listener:
  port: $PORT
  require_client_cert: true
EOF

  run dolt sql-server --config ./config.yml --socket "dolt.$PORT.sock"
  [ "$status" -ne 0 ]
  [[ "$output" =~ "must supply tls_cert and tls_key when require_client_cert is enabled" ]] || false
}
