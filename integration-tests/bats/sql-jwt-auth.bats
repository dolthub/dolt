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
  
  run dolt sql-client --host=127.0.0.1 --port=$PORT --allow-cleartext-passwords=true --user=test_jwt_user --password=eyJhbGciOiJSUzI1NiIsImtpZCI6ImViYmMxY2ZlLTZhZjMtNDZmOC1iMmQxLWZiNjkyZDNhZGJjYiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsibXlfcmVzb3VyY2UiXSwiZXhwIjoxNjU4NDI4MzcxLCJpYXQiOjE2NTg0MjY1NzEsImlzcyI6ImRvbHRodWIuY29tIiwianRpIjoiYmU3YTg2MzctNmM5MC00OWFhLTgyY2MtODE2ZDIwMDdkZTNiIiwib25fYmVoYWxmX29mIjoibXlfdXNlciIsInN1YiI6InRlc3RfdXNlciJ9.L0HBebCbjQhHKVssJrqX6uRwZfx48j4tP121pYTW83xawAIhadgtxSTDZf4wDXySemTfmaRlIxpw9gYL1p2YLLz_xDM6ho4LOhZhm1yRl8F4LHxw30G-8oUNmp-F9Jcs7NJkDOkZBa4sPhNs8zJABHomztNzQ1ZQ2xiiKeYnRrvG3AQu7qCMikx9nYIh4TWbJPwbZvtaxaCgct6vVOvoZLyaSA-IE_EEOzUoOPUnmgU_Xlxv6CWeR7oRbBPTIFR-573qU79ydcSUKBJsRMMTE-PxwKXddd2GvX3O7vmXjQICwzrfyN7shJfqusmtAs5GVTUONHDD3gI9i4eWLK9PEg<<SQL
SHOW DATABASES;
SQL

  echo $output
  [ "$status" -eq 0 ]
  [ "${lines[0]}" = '# Welcome to the Dolt MySQL client.' ]
  [ "${lines[1]}" = "# Statements must be terminated with ';'." ]
  [ "${lines[2]}" = '# "exit" or "quit" (or Ctrl-D) to exit.' ]
  [ "${lines[3]}" = '+--------------------+' ]
  [ "${lines[4]}" = '| Database           |' ]
  [ "${lines[5]}" = '+--------------------+' ]
  [ "${lines[6]}" = '| information_schema |' ]
  [ "${lines[7]}" = '| repo1              |' ]
  [ "${lines[8]}" = '+--------------------+' ]

}