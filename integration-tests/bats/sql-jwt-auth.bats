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
    setup_no_dolt_init
    make_repo repo1
    make_repo repo2
}

teardown() {
    stop_sql_server
    teardown_common
}


@test "sql-server: jwt auth from config" {
  cd repo1
  echo "
privilege_file: privs.json
jwks:
- name: jwksname
  location_url: file://$BATS_TEST_DIRNAME/helper/test_jwks.json
  claims: 
    alg: RS256
    aud: my_resource
    sub: test_jwt_user
    iss: dolthub.com
  fields_to_log: [on_behalf_of,id]" > server.yaml

  dolt sql --privilege-file=privs.json -q "CREATE USER dolt@'127.0.0.1'"
  dolt sql --privilege-file=privs.json -q "CREATE USER test_jwt_user@'127.0.0.1' IDENTIFIED WITH authentication_dolt_jwt AS 'jwks=jwksname,sub=test_jwt_user,iss=dolthub.com,aud=my_resource'"
  dolt sql --privilege-file=privs.json -q "GRANT ALL ON *.* TO test_jwt_user@'127.0.0.1' WITH GRANT OPTION"

  start_sql_server_with_config "" server.yaml
  
  run dolt sql-client --host=127.0.0.1 --port=$PORT --allow-cleartext-passwords=true --user=test_jwt_user --password=eyJhbGciOiJSUzI1NiIsImtpZCI6ImUwNjA2Y2QwLTkwNWQtNGFiYS05MjBjLTZlNTE0YTFjYmIyNiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsibXlfcmVzb3VyY2UiXSwiZXhwIjoxNjU4Mjc1OTAzLCJpYXQiOjE2NTgyNzU4NzMsImlzcyI6ImRvbHRodWIuY29tIiwianRpIjoiN2ViZTg3YmMtOTkzMi00ZTljLTk5N2EtNjQzMDk0NTBkMWVjIiwib25fYmVoYWxmX29mIjoibXlfdXNlciIsInN1YiI6InRlc3RfdXNlciJ9.u2cUGUkQ2hk4AaxtNQB-6Jcdf5LtehFA7XX2FG8LGgTf6KfwE3cuuGaBIU8Jz9ktD9g8TjAbfAfbrNaFNYnKG6SnDUHp0t7VbfLdgfNDQqSyH0nOK2UF8ffxqa46PRxeMwTSJv8prE07rcmiZNL9Ie4vSGYLncJfMzo_RdE-A-PH7z-ZyZ_TxOMhkgMFq2Af5Px3zFuAKq-Y-PrQNopSuzjPJc0DQ93Q7EcIHfU6Fx6gOVTkzHxnOFcg3Nj-4HhqBSvBa_BdMYEzHJKx3F_9rrCCPqEGUFnxXAqFFmnZUQuQKpN2yW_zhviCVqrvbP7vOCIXmxi8YXLiGiV-4KlxHA<<SQL
USE repo1;
SHOW TABLES;
SQL

   echo $output
   [ "$status" -eq 0 ]
}