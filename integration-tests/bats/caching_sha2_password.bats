#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    skiponwindows "sql-server tests are missing dependencies on Windows"
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test tests remote connections directly, SQL_ENGINE is not needed."
    fi

    BATS_TEST_DIR=$PWD
    PROJECT_ROOT=$BATS_TEST_DIR/../..
    CERTS_DIR=$PROJECT_ROOT/go/libraries/doltcore/servercfg/testdata
    setup_no_dolt_init
    dolt init
    cp $CERTS_DIR/chain_cert.pem $CERTS_DIR/chain_key.pem .
}

teardown() {
    stop_sql_server 1 && sleep 0.5
    rm -rf $BATS_TMPDIR/sql-server-test$$
    teardown_common
}

start_sql_server() {
    PORT=$( definePORT )
    cat >config.yml <<EOF
log_level: debug
behavior:
  disable_client_multi_statements: true
user:
  name: dolt
listener:
  host: "0.0.0.0"
  port: $PORT
  tls_cert:  chain_cert.pem
  tls_key: chain_key.pem
  require_secure_transport: true
EOF

    dolt sql -q "CREATE USER userWithPassword@localhost IDENTIFIED WITH caching_sha2_password BY 'pass3';"
    dolt sql -q "CREATE USER userWithNoPassword@localhost IDENTIFIED WITH caching_sha2_password;"

    dolt sql-server --config ./config.yml --socket "dolt.$PORT.sock" &
    SERVER_PID=$!
    sleep 1
}

# The mysql command line client is not available in Lambda
# bats test_tags=no_lambda
@test "caching_sha2_password: user does not exist" {
    start_sql_server

    # Invalid account
    run mysql -uinvalid --port $PORT --protocol TCP --default-auth=caching_sha2_password -pwrong
    [ "$status" -ne 0 ]
    [[ "$output" =~ "No authentication methods available for authentication" ]] || false
}

# The mysql command line client is not available in Lambda
# bats test_tags=no_lambda
@test "caching_sha2_password: password account, no auth renegotiation" {
    start_sql_server

    # Account with password, no auth method renegotiation, wrong password
    run mysql -uuserWithPassword --port $PORT --protocol TCP --default-auth=caching_sha2_password -pwrong
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Access denied for user 'userWithPassword'" ]] || false

    # Account with password, no auth method renegotiation, right password
    run mysql -uuserWithPassword --port $PORT --protocol TCP --default-auth=caching_sha2_password -ppass3 -e "SELECT 'SUCCESS';"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "SUCCESS" ]] || false

    # Account with password, no auth method renegotiation, no password
    run mysql -uuserWithPassword --port $PORT --protocol TCP --default-auth=caching_sha2_password
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Access denied for user 'userWithPassword'" ]] || false

    # Account with password, no auth method renegotiation, empty password
    run mysql -uuserWithPassword --port $PORT --protocol TCP --default-auth=caching_sha2_password --password=
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Access denied for user 'userWithPassword'" ]] || false
}

# The mysql command line client is not available in Lambda
# bats test_tags=no_lambda
@test "caching_sha2_password: password account, with auth renegotiation" {
    start_sql_server

    # Account with password, auth method renegotiation, wrong password
    run mysql -uuserWithPassword --port $PORT --protocol TCP --default-auth=mysql_native_password -pwrong
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Access denied for user 'userWithPassword'" ]] || false

    # Account with password, auth method renegotiation, right password
    run mysql -uuserWithPassword --port $PORT --protocol TCP --default-auth=mysql_native_password -ppass3 -e "SELECT 'SUCCESS';"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "SUCCESS" ]] || false

    # Account with password, auth method renegotiation, no password
    run mysql -uuserWithPassword --port $PORT --protocol TCP --default-auth=mysql_native_password
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Access denied for user 'userWithPassword'" ]] || false

    # Account with password, auth method renegotiation, empty password
    run mysql -uuserWithPassword --port $PORT --protocol TCP --default-auth=mysql_native_password --password=
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Access denied for user 'userWithPassword'" ]] || false
}

# The mysql command line client is not available in Lambda
# bats test_tags=no_lambda
@test "caching_sha2_password: passwordless account, no auth renegotiation" {
    start_sql_server

    # Account without password, no auth method renegotiation, wrong password
    run mysql -uuserWithNoPassword --port $PORT --protocol TCP --default-auth=caching_sha2_password -pwrong
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Access denied for user 'userWithNoPassword'" ]] || false

    # Account without password, no auth method renegotiation, correct (empty) password specified
    run mysql -uuserWithNoPassword --port $PORT --protocol TCP --default-auth=caching_sha2_password -e "SELECT 'SUCCESS';" --password=
    [ "$status" -eq 0 ]
    [[ "$output" =~ "SUCCESS" ]] || false

    # Account without password, no auth method renegotiation, no password
    run mysql -uuserWithNoPassword --port $PORT --protocol TCP --default-auth=caching_sha2_password -e "SELECT 'SUCCESS';"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "SUCCESS" ]] || false
}

# The mysql command line client is not available in Lambda
# bats test_tags=no_lambda
@test "caching_sha2_password: passwordless account, with auth renegotiation" {
    start_sql_server

    # Account without password, auth method renegotiation, wrong password
    run mysql -uuserWithNoPassword --port $PORT --protocol TCP --default-auth=mysql_native_password -pwrong
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Access denied for user 'userWithNoPassword'" ]] || false

    # Account without password, auth method renegotiation, right password
    run mysql -uuserWithNoPassword --port $PORT --protocol TCP --default-auth=mysql_native_password -e "SELECT 'SUCCESS';" --password=
    [ "$status" -eq 0 ]
    [[ "$output" =~ "SUCCESS" ]] || false

    # Account without password, auth method renegotiation, no password
    run mysql -uuserWithNoPassword --port $PORT --protocol TCP --default-auth=mysql_native_password -e "SELECT 'SUCCESS';"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "SUCCESS" ]] || false
}
