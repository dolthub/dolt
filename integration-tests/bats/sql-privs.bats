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

@test "sql-privs: default user is root" {
    make_test_repo
    let PORT="$$ % (65536-1024) + 1024"
    dolt sql-server --host 0.0.0.0 --port=$PORT &
    SERVER_PID=$! # will get killed by teardown_common
    sleep 5 # not using python wait so this works on windows

    server_query_with_user test_db 1 root "select user from mysql.user order by user" "User\nroot"
    server_query_with_user test_db 1 root "create user new_user" ""
    server_query_with_user test_db 1 root "select user from mysql.user order by user" "User\nnew_user\nroot"

    stop_sql_server
    rm -f .dolt/sql-server.lock

    # restarting server
    let PORT="$$ % (65536-1024) + 1024"
    dolt sql-server --host 0.0.0.0 --port=$PORT &
    SERVER_PID=$! # will get killed by teardown_common
    sleep 5 # not using python wait so this works on windows

    run server_query_with_user test_db 1 root "select user from mysql.user order by user" ""
    [ "$status" -eq 1 ]
}

@test "sql-privs: starting server with empty config works" {
    make_test_repo
    touch server.yaml

    start_sql_server_with_config test_db server.yaml

    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt"
    server_query test_db 1 "create user new_user" ""
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    run ls .doltcfg
    [[ "$output" =~ "privileges.db" ]] || false
}

@test "sql-privs: yaml specifies doltcfg dir" {
    make_test_repo
    touch server.yaml
    echo "cfg_dir: \"doltcfgdir\"" > server.yaml

    start_sql_server_with_config test_db server.yaml

    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt"
    server_query test_db 1 "create user new_user" ""
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

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

    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt"
    server_query test_db 1 "create user new_user" ""
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

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

    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nprivs_user"
    server_query test_db 1 "create user new_user" ""
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nnew_user\nprivs_user"

    # Test that privs.json file is not in json format
    run cat privs.json
    ! [[ "$output" =~ "\"User\":\"privs_user\"" ]] || false

    # Restart server
    rm -f ./.dolt/sql-server.lock
    stop_sql_server
    start_sql_server_with_args --host 0.0.0.0 --user=dolt --privilege-file=privs.json
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nnew_user\nprivs_user"
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

    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt"
    server_query test_db 1 "create user new_user" ""
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    run ls .doltcfg
    [[ "$output" =~ "privileges.db" ]] || false
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

    server_query db1 1 "show databases" "Database\ndb1\ndb2\ndb3\ninformation_schema"
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt"
    server_query db1 1 "create user new_user" ""
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

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

    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt"
    server_query test_db 1 "create user new_user" ""
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

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

    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt"
    server_query test_db 1 "create user new_user" ""
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

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

    server_query db1 1 "show databases" "Database\ndb1\ndb2\ndb3\ninformation_schema"
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt"
    server_query db1 1 "create user new_user" ""
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

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

    server_query db1 1 "show databases" "Database\ndb1\ndb2\ndb3\ninformation_schema"
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt"
    server_query db1 1 "create user new_user" ""
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

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

    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt"
    server_query test_db 1 "create user new_user" ""
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

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

    server_query db1 1 "show databases" "Database\ndb1\ndb2\ndb3\ninformation_schema"
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt"
    server_query db1 1 "create user new_user" ""
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

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