#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

# working dir will be test_db
make_test_repo() {
    rm -rf test_db
    mkdir test_db
    cd test_db
    dolt init
}

# working dir will be test_db
make_multi_test_repo() {
    rm -rf test_db
    mkdir test_db
    cd test_db

    mkdir db_dir
    cd db_dir

    mkdir db1
    cd db1
    dolt init
    cd ..

    mkdir db2
    cd db2
    dolt init
    cd ..

    mkdir db3
    cd db3
    dolt init
    cd ..

    cd ..
}

# working dir will be dolt_repo$$
delete_test_repo() {
    stop_sql_server
    cd ..
    rm -rf test_db
}

setup() {
    setup_no_dolt_init
}

teardown() {
    delete_test_repo
    teardown_common
}

@test "sql-privs: default user is root. create new user destroys default user." {
    make_test_repo
    PORT=$( definePORT )
    dolt sql-server --host 0.0.0.0 --port=$PORT &
    SERVER_PID=$! # will get killed by teardown_common
    sleep 5 # not using python wait so this works on windows

    server_query test_db 1 root "" "select user from mysql.user order by user" "User\nroot"
    server_query test_db 1 root "" "create user new_user" ""
    server_query test_db 1 root "" "select user from mysql.user order by user" "User\nnew_user\nroot"

    stop_sql_server
    rm -f .dolt/sql-server.lock

    # restarting server
    PORT=$( definePORT )
    dolt sql-server --host 0.0.0.0 --port=$PORT &
    SERVER_PID=$! # will get killed by teardown_common
    sleep 5 # not using python wait so this works on windows

    server_query test_db 1 root "select user from mysql.user order by user" "" 1
}

@test "sql-privs: starting server with empty config works" {
    make_test_repo
    touch server.yaml

    start_sql_server_with_config test_db server.yaml

    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt"
    server_query test_db 1 dolt "" "create user new_user" ""
    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nnew_user"

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    run ls .doltcfg
    [[ "$output" =~ "privileges.db" ]] || false
}

@test "sql-privs: yaml with no user is replaced with command line user" {
    make_test_repo
    touch server.yaml
    PORT=$( definePORT )

    echo "log_level: debug

listener:
    host: 0.0.0.0
    port: $PORT
    max_connections: 10

behavior:
    autocommit: false
" > server.yaml

    dolt sql-server --port=$PORT --config server.yaml --user cmddolt &
    SERVER_PID=$!

    server_query test_db 1 cmddolt "" "select user from mysql.user order by user" "User\ncmddolt"
}

@test "sql-privs: yaml with user is also replaced with command line user" {
    make_test_repo
    touch server.yaml
    PORT=$( definePORT )

    echo "log_level: debug
user:
  name: yamldolt

listener:
    host: 0.0.0.0
    port: $PORT
    max_connections: 10

behavior:
    autocommit: false
" > server.yaml

    dolt sql-server --port=$PORT --config server.yaml --user cmddolt &
    SERVER_PID=$!

    server_query test_db 1 cmddolt "" "select user from mysql.user order by user" "User\ncmddolt"
}

@test "sql-privs: yaml specifies doltcfg dir" {
    make_test_repo
    touch server.yaml
    echo "cfg_dir: \"doltcfgdir\"" > server.yaml

    start_sql_server_with_config test_db server.yaml

    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt"
    server_query test_db 1 dolt "" "create user new_user" ""
    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nnew_user"

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    run ls doltcfgdir
    [[ "$output" =~ "privileges.db" ]] || false
}

@test "sql-privs: yaml specifies privilege file" {
    make_test_repo
    touch server.yaml
    echo "privilege_file: \"privs.db\"" > server.yaml

    start_sql_server_with_config test_db server.yaml

    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt"
    server_query test_db 1 dolt "" "create user new_user" ""
    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nnew_user"

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "privs.db" ]] || false
}

@test "sql-privs: can read json privilege files and convert them" {
    make_test_repo
    cp $BATS_TEST_DIRNAME/privs.json .

    # Test that privs.json file is in json format
    run cat privs.json
    [[ "$output" =~ "\"User\":\"privs_user\"" ]] || false

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --privilege-file=privs.json

    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nprivs_user"
    server_query test_db 1 dolt "" "create user new_user" ""
    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nnew_user\nprivs_user"

    # Test that privs.json file is not in json format
    run cat privs.json
    ! [[ "$output" =~ "\"User\":\"privs_user\"" ]] || false

    # Restart server
    rm -f ./.dolt/sql-server.lock
    stop_sql_server
    start_sql_server_with_args --host 0.0.0.0 --user=dolt --privilege-file=privs.json
    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nnew_user\nprivs_user"
}

@test "sql-privs: errors instead of panic when reading badly formatted privilege file" {
    make_test_repo
    touch privs.db
    echo "garbage" > privs.db

    run start_sql_server_with_args --host 0.0.0.0 --user=dolt --privilege-file=privs.db
    [ "$status" -eq 1 ]
    [[ "$output" =~ "ill formatted privileges file" ]] || false
}

@test "sql-privs: default options" {
    make_test_repo

    start_sql_server test_db

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt"
    server_query test_db 1 dolt "" "create user new_user" ""
    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nnew_user"

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    run ls .doltcfg
    [[ "$output" =~ "privileges.db" ]] || false
}

@test "sql-privs: host option doesn't affect user" {
    make_test_repo

    start_sql_server_with_args --host 127.0.0.1 --user=dolt
    server_query test_db 1 dolt "" "select user, host from mysql.user order by user" "User,Host\ndolt,%"
}

@test "sql-privs: multiple doltcfg directories causes error" {
    # setup repo
    rm -rf test_db
    mkdir test_db
    cd test_db

    mkdir .doltcfg

    mkdir inner_db
    cd inner_db
    mkdir .doltcfg

    run start_sql_server inner_db
    [ "$status" -eq 1 ]
    [[ "$output" =~ "multiple .doltcfg directories detected" ]] || false

    cd ..
}

@test "sql-privs: sql-server specify data-dir" {
    make_multi_test_repo

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --data-dir=db_dir

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    server_query db1 1 dolt "" "show databases" "Database\ndb1\ndb2\ndb3\ninformation_schema\nmysql"
    server_query db1 1 dolt "" "select user from mysql.user order by user" "User\ndolt"
    server_query db1 1 dolt "" "create user new_user" ""
    server_query db1 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nnew_user"

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls -a db_dir
    [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls db_dir/.doltcfg
    [[ "$output" =~ "privileges.db" ]] || false
}

@test "sql-privs: specify doltcfg directory" {
    make_test_repo

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --doltcfg-dir=doltcfgdir

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false

    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt"
    server_query test_db 1 dolt "" "create user new_user" ""
    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nnew_user"

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    run ls doltcfgdir
    [[ "$output" =~ "privileges.db" ]] || false
}

@test "sql-privs: specify privilege file" {
    make_test_repo

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --privilege-file=privs.db

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt"
    server_query test_db 1 dolt "" "create user new_user" ""
    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nnew_user"

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "privs.db" ]] || false
}

@test "sql-privs: specify data-dir and doltcfg-dir" {
    make_multi_test_repo

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    server_query db1 1 dolt "" "show databases" "Database\ndb1\ndb2\ndb3\ninformation_schema\nmysql"
    server_query db1 1 dolt "" "select user from mysql.user order by user" "User\ndolt"
    server_query db1 1 dolt "" "create user new_user" ""
    server_query db1 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nnew_user"

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls doltcfgdir
    [[ "$output" =~ "privileges.db" ]] || false
}

@test "sql-privs: specify data-dir and privilege-file" {
    make_multi_test_repo

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --data-dir=db_dir --privilege-file=privs.db

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    server_query db1 1 dolt "" "show databases" "Database\ndb1\ndb2\ndb3\ninformation_schema\nmysql"
    server_query db1 1 dolt "" "select user from mysql.user order by user" "User\ndolt"
    server_query db1 1 dolt "" "create user new_user" ""
    server_query db1 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nnew_user"

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "privs.db" ]] || false

    run ls -a db_dir
    [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    run ls db_dir/.doltcfg
    ! [[ "$output" =~ "privs.db" ]] || false
}

@test "sql-privs: specify doltcfg-dir and privilege-file" {
    make_test_repo

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --doltcfg-dir=doltcfgdir --privilege-file=privs.db

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt"
    server_query test_db 1 dolt "" "create user new_user" ""
    server_query test_db 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nnew_user"

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false
    [[ "$output" =~ "privs.db" ]] || false

    run ls doltcfgdir
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false
}

@test "sql-privs: specify data-dir, doltcfg-dir, and privileges-file" {
    make_multi_test_repo

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    server_query db1 1 dolt "" "show databases" "Database\ndb1\ndb2\ndb3\ninformation_schema\nmysql"
    server_query db1 1 dolt "" "select user from mysql.user order by user" "User\ndolt"
    server_query db1 1 dolt "" "create user new_user" ""
    server_query db1 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nnew_user"

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false
    [[ "$output" =~ "privs.db" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    run ls doltcfgdir
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false
}

@test "sql-privs: default to parent privilege file if current is missing" {
    make_multi_test_repo

    dolt init
    start_sql_server_with_args --host 0.0.0.0 --user=dolt

    server_query test_db 1 dolt "" "create user new_user" ""
    stop_sql_server
    sleep 1
    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false
    run ls -a .doltcfg
    [[ "$output" =~ "privileges.db" ]] || false

    cd db_dir
    start_sql_server_with_args --host 0.0.0.0 --user=dolt
    server_query db1 1 dolt "" "select user from mysql.user order by user" "User\ndolt\nnew_user"
}

@test "sql-privs: basic lack of privileges tests" {
     make_test_repo
     start_sql_server

     server_query test_db 1 dolt "" "create table t1(c1 int)"
     server_query test_db 1 dolt "" "create user test"
     server_query test_db 1 dolt "" "grant select on test_db.* to test"

     # Should only see test_db database
     server_query "" 1 test "" "show databases" "Database\ntest_db"
     server_query test_db 1 test "" "show tables" "Tables_in_test_db\nt1"
     
     # Revoke works as expected
     server_query test_db 1 dolt "" "revoke select on test_db.* from test"
     server_query test_db 1 test "" "show tables" "" 1

     # Host in privileges is respected
     server_query test_db 1 dolt "" "drop user test"
     server_query test_db 1 dolt "" "create user test@'127.0.0.1'"
     server_query test_db 1 dolt "" "grant select on test_db.* to test@'127.0.0.1'"
     server_query test_db 1 test "" "show tables" "Tables_in_test_db\nt1"
     server_query test_db 1 dolt "" "drop user test@'127.0.0.1'"
     server_query test_db 1 dolt "" "create user test@'10.10.10.10'"
     server_query test_db 1 dolt "" "grant select on test_db.* to test@'10.10.10.10'"
     server_query test_db 1 test "" "show tables" "" 1
}

@test "sql-privs: creating user identified by password" {
     make_test_repo
     start_sql_server

     server_query test_db 1 dolt "" "create user test identified by 'test'" ""
     server_query test_db 1 dolt "" "grant select on mysql.user to test" ""

     # Should not be able to connect to test_db
     server_query test_db 1 test test "select user from mysql.user order by user" "" 1

     server_query "" 1 test test "select user from mysql.user order by user" "User\ndolt\ntest"

     # Bad password can't connect
     server_query "" 1 test bad "select user from mysql.user order by user" "" 1
     
     # Should only see mysql database
     server_query "" 1 test test "show databases" "Database\nmysql"
}

@test "sql-privs: deleting user prevents access by that user" {
     make_test_repo
     start_sql_server

     server_query test_db 1 dolt "" "create user test"
     server_query test_db 1 dolt "" "grant select on test_db.* to test" ""

     server_query test_db 1 test "" "show tables" ""

     server_query test_db 1 dolt "" "drop user test"

     server_query test_db 1 test "" "show tables" "" 1
}
