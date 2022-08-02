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
    skiponwindows "Missing dependencies"
    setup_no_dolt_init
    make_repo repo1
    (cd "$BATS_TEST_DIRNAME"/../../go/libraries/utils/jwtauth/gen_keys && go run .)
}

teardown() {
    stop_sql_server
    teardown_common
    rm -rf "$BATS_TEST_DIRNAME/../../go/libraries/utils/jwtauth/gen_keys/token.jwt"
    rm -rf "$BATS_TEST_DIRNAME/../../go/libraries/utils/jwtauth/gen_keys/test_jwks.json"
}


@test "sql-jwt-auth: jwt auth from config" {
    cd repo1
    cp "$BATS_TEST_DIRNAME"/../../go/cmd/dolt/commands/sqlserver/testdata/chain_key.pem .
    cp "$BATS_TEST_DIRNAME"/../../go/cmd/dolt/commands/sqlserver/testdata/chain_cert.pem .
    cp "$BATS_TEST_DIRNAME"/../../go/libraries/utils/jwtauth/gen_keys/test_jwks.json .
    let PORT="$$ % (65536-1024) + 1024"
    TOKEN="`cat $BATS_TEST_DIRNAME/../../go/libraries/utils/jwtauth/gen_keys/token.jwt`"

    cat >config.yml <<EOF
log_level: debug
user:
  name: dolt
listener:
  host: "0.0.0.0"
  port: $PORT
  tls_cert: chain_cert.pem
  tls_key: chain_key.pem
  require_secure_transport: true
privilege_file: privs.json
jwks:
- name: jwksname
  location_url: file:///test_jwks.json
  claims: 
    alg: RS256
    aud: my_resource
    sub: test_jwt_user
    iss: dolthub.com
  fields_to_log: [on_behalf_of,id]
EOF

    dolt sql --privilege-file=privs.json -q "CREATE USER dolt@'127.0.0.1'"
    dolt sql --privilege-file=privs.json -q "GRANT ALL ON *.* TO dolt@'127.0.0.1' WITH GRANT OPTION"
    dolt sql --privilege-file=privs.json -q "CREATE USER test_jwt_user@'127.0.0.1' IDENTIFIED WITH authentication_dolt_jwt AS 'jwks=jwksname,sub=test_jwt_user,iss=dolthub.com,aud=my_resource'"
    dolt sql --privilege-file=privs.json -q "GRANT ALL ON *.* TO test_jwt_user@'127.0.0.1' WITH GRANT OPTION"

    dolt sql-server --config ./config.yml &
    SERVER_PID=$!


    # We do things manually here because we need TLS support.
    python3 -c '
import mysql.connector
import sys
import time
import os
i=0

os.environ["LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN"] = "1"
args = sys.argv[sys.argv.index("--") + 1:]
password = args[0]

while True:
  try:
    with mysql.connector.connect(
      host="127.0.0.1",
      user="test_jwt_user",
      password=password,
      port='"$PORT"',
      database="repo1",
      connection_timeout=1
    ) as c:
      cursor = c.cursor()
      cursor.execute("show tables")
      for (t) in cursor:
        print(t)
      sys.exit(0)
  except mysql.connector.Error as err:
    if err.errno != 2003:
      raise err
    else:
      i += 1
      time.sleep(1)
      if i == 10:
        raise err
' -- "$TOKEN"
}