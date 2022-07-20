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

@test "sql-privs: starting server with empty config works" {
    make_test_repo
    touch server.yaml

    start_sql_server_with_config test_db server.yaml

    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt"
    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    server_query test_db 1 "create user new_user" ""
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

    run ls .doltcfg
    [[ "$output" =~ "privileges.db" ]] || false
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

    # expect only dolt user
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt"

    # check for config directory
    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    # create user
    server_query test_db 1 "create user new_user" ""

    # expect dolt user and new_user
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"
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

    # expect start server to fail
    run start_sql_server inner_db
    [ "$status" -eq 1 ]
    [[ "$output" =~ "multiple .doltcfg directories detected" ]] || false

    cd ..
}

@test "sql-privs: sql-server specify data-dir" {
    make_multi_test_repo

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --data-dir=db_dir

    # show databases, expect all
    server_query db1 1 "show databases" "Database\ndb1\ndb2\ndb3\ninformation_schema"

    # show users, expect just root user
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt"

    # expect no .doltcfg in current directory
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # expect .doltcfg in $datadir
    run ls -a db_dir
    [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    server_query db1 1 "create user new_user" ""

    # show users, expect root user and new_user
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

    # expect no privileges.db in current directory
    run ls
    ! [[ "$output" =~ "privileges.db" ]] || false

    # expect no privileges.db in $datadir directory
    run ls db_dir
    ! [[ "$output" =~ "privileges.db" ]] || false

    # expect privileges.db in $datadir/.doltcfg
    run ls db_dir/.doltcfg
    [[ "$output" =~ "privileges.db" ]] || false
}

@test "sql-privs: specify doltcfg directory" {
    make_test_repo

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --doltcfg-dir=doltcfgdir

    # show users, expect just root user
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt"

    # expect only custom doltcfgdir
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    # create new_user
    server_query test_db 1 "create user new_user" ""

    # show users, expect root user and new_user
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

    # expect privileges file in doltcfgdir
    run ls doltcfgdir
    [[ "$output" =~ "privileges.db" ]] || false
}

@test "sql-privs: specify privilege file" {
    make_test_repo

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --privilege-file=privs.db

    # show users, expect just root user
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt"

    # expect default doltcfg directory
    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    # create new_user
    server_query test_db 1 "create user new_user" ""

    # show users, expect root user and new_user
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

    # expect custom privilege file current directory
    run ls
    [[ "$output" =~ "privs.db" ]] || false
}

@test "sql-privs: specify data-dir and doltcfg-dir" {
    make_multi_test_repo

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir

    # show databases, expect all
    server_query db1 1 "show databases" "Database\ndb1\ndb2\ndb3\ninformation_schema"

    # show users, expect just root user
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt"

    # expect custom doltcfg in current directory
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    # expect no .doltcfg in $datadir
    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    server_query db1 1 "create user new_user" ""

    # show users, expect root user and new_user
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

    # expect no privileges.db in current directory
    run ls
    ! [[ "$output" =~ "privileges.db" ]] || false

    # expect no privileges.db in $datadir directory
    run ls db_dir
    ! [[ "$output" =~ "privileges.db" ]] || false

    # expect privileges.db in $doltcfg directory
    run ls doltcfgdir
    [[ "$output" =~ "privileges.db" ]] || false
}

@test "sql-privs: specify data-dir and privilege-file" {
    make_multi_test_repo

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --data-dir=db_dir --privilege-file=privs.db

    # show databases, expect all
    server_query db1 1 "show databases" "Database\ndb1\ndb2\ndb3\ninformation_schema"

    # show users, expect just root user
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt"

    # expect no .doltcfg in current directory
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # expect .doltcfg in $datadir
    run ls -a db_dir
    [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    server_query db1 1 "create user new_user" ""

    # show users, expect root user and new_user
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

    # expect privs.db in current directory
    run ls
    [[ "$output" =~ "privs.db" ]] || false

    # expect no privileges.db in $datadir directory
    run ls db_dir
    ! [[ "$output" =~ "privs.db" ]] || false

    # expect no privs.db in $doltcfg directory
    run ls db_dir/.doltcfg
    ! [[ "$output" =~ "privs.db" ]] || false
}

@test "sql-privs: specify doltcfg-dir and privilege-file" {
    make_test_repo

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --doltcfg-dir=doltcfgdir --privilege-file=privs.db

    # expect only dolt user
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt"

    # expect custom doltcfgdir
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    # create user
    server_query test_db 1 "create user new_user" ""

    # expect dolt user and new_user
    server_query test_db 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

    # expect privileges file
    run ls
    [[ "$output" =~ "privs.db" ]] || false

    # expect no privileges file in doltcfgdir
    run ls doltcfgdir
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false
}

@test "sql-privs: specify data-dir, doltcfg-dir, and privileges-file" {
    make_multi_test_repo

    start_sql_server_with_args --host 0.0.0.0 --user=dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db

    # show databases, expect all
    server_query db1 1 "show databases" "Database\ndb1\ndb2\ndb3\ninformation_schema"

    # show users, expect just root user
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt"

    # expect custom doltcfg in current directory
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    # expect no .doltcfg in $datadir
    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    server_query db1 1 "create user new_user" ""

    # show users, expect root user and new_user
    server_query db1 1 "select user from mysql.user order by user" "User\ndolt\nnew_user"

    # expect privs.db in current directory
    run ls
    ! [[ "$output" =~ "privileges.db" ]] || false
    [[ "$output" =~ "privs.db" ]] || false

    # expect no privileges.db in $datadir directory
    run ls db_dir
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    # expect no privileges.db in $doltcfg directory
    run ls doltcfgdir
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false
}