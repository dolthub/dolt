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

# Asserts that the root@localhost superuser is automatically created when a sql-server is started
# for the first time and no users are defined yet. As additional users are created, the
# root@localhost superuser remains and can be manually removed without coming back.
@test "sql-privs: implicit root superuser doesn't disappear after adding users" {
    PORT=$( definePORT )
    dolt sql-server --port $PORT &
    SERVER_PID=$!
    sleep 1

    # Assert that the root user can log in and run a query
    run dolt -u root sql -q "select user, host from mysql.user where user='root';"
    [ $status -eq 0 ]
    [[ $output =~ "| root | localhost |" ]] || false

    # Create a new user
    dolt -u root sql -q "CREATE USER user1@localhost; GRANT ALL PRIVILEGES on *.* to user1@localhost;"

    # Restart the SQL server
    stop_sql_server 1 && sleep 0.5
    dolt sql-server --port $PORT &
    SERVER_PID=$!
    sleep 1

    # Assert that both users are still present
    run dolt -u root sql -q "select user, host from mysql.user where user in ('root', 'user1');"
    [ $status -eq 0 ]
    [[ $output =~ "| root  | localhost |" ]] || false
    [[ $output =~ "| user1 | localhost |" ]] || false

    # Delete the root user
    dolt -u root sql -q "DROP USER root@localhost;"

    # Restart the SQL server
    stop_sql_server 1 && sleep 0.5
    dolt sql-server --port $PORT &
    SERVER_PID=$!
    sleep 1

    # Assert that the root user is gone
    run dolt -u user1 sql -q "select user, host from mysql.user where user in ('root', 'user1');"
    [ $status -eq 0 ]
    ! [[ $output =~ "root" ]] || false
    [[ $output =~ "| user1 | localhost |" ]] || false
}

# Asserts that the root superuser host and password can be overridden through the DOLT_ROOT_HOST
# and DOLT_ROOT_PASSWORD environment variables, analogues to MySQL's MYSQL_ROOT_HOST and
# MYSQL_ROOT_PASSWORD environment variables. This is primarily provided for use during
# development when running a Dolt sql-server in a Docker container.
@test "sql-privs: root superuser honors DOLT_ROOT_HOST and DOLT_ROOT_PASSWORD" {
    PORT=$( definePORT )
    export DOLT_ROOT_HOST='%'
    export DOLT_ROOT_PASSWORD='Pass1'
    dolt sql-server --port $PORT &
    SERVER_PID=$!
    sleep 1

    # Assert that the root user can log in with the overridden password and run a query
    run dolt -u root -p Pass1 sql -q "select user, host from mysql.user where user='root';"
    [ $status -eq 0 ]
    ! [[ $output =~ "localhost" ]] || false
    [[ $output =~ "| root | % " ]] || false

    # Restart the SQL server. Changing DOLT_ROOT_HOST and DOLT_ROOT_PASSWORD here is a no-op,
    # since the root superuser was already initialized the previous time sql-server was started.
    stop_sql_server 1 && sleep 0.5
    dolt sql-server --port $PORT &
    SERVER_PID=$!
    export DOLT_ROOT_HOST='localhost'
    export DOLT_ROOT_PASSWORD='donotuse'
    sleep 1

    # Assert that root is still configured for any host
    run dolt -u root -p Pass1 sql -q "select user, host from mysql.user where user = 'root';"
    [ $status -eq 0 ]
    ! [[ $output =~ "localhost" ]] || false
    [[ $output =~ "| root | % " ]] || false
}

# Asserts that `dolt sql` can always be used to access the database as a superuser. For example, if the root
# user is assigned a password, `dolt sql` should still be able to log in as a superuser, since the user
# already has access to the host and data directory.
@test "sql-privs: superuser access is always available from dolt sql" {
    # Create a root@% user with a password set
    dolt sql -q "CREATE USER root@'%' identified by 'pass1'; grant all on *.* to root@'%' with grant option;"

    # Make sure dolt sql can still log in as root, even though root has a password set now
    run dolt sql -q "select user();"
    [ $status -eq 0 ]
    [[ $output =~ "root@localhost" ]] || false
}

# Asserts that creating users via 'dolt sql' before starting a sql-server causes the privileges.db to be
# initialized and prevents the root superuser from being created, since the customer has already started
# manually managing user accounts.
@test "sql-privs: implicit root superuser doesn't get created when users are created before the server starts" {
    dolt sql -q "CREATE USER user1@localhost; GRANT ALL PRIVILEGES on *.* to user1@localhost;"

    PORT=$( definePORT )
    dolt sql-server --port $PORT &
    SERVER_PID=$!
    sleep 1

    # Assert that the root superuser was not automatically created
    run dolt -u user1 sql -q "select user, host from mysql.user where user in ('root', 'user1');"
    echo "OUTPUT: $output"
    [ $status -eq 0 ]
    ! [[ $output =~ "root" ]] || false
    [[ $output =~ "| user1 | localhost |" ]] || false
}

# Asserts that the root@localhost superuser is not created when the --skip-default-root-user flag
# is specified when first running sql-server and initializing privileges.db.
@test "sql-privs: implicit root superuser doesn't get created when skipped" {
    PORT=$( definePORT )
    dolt sql-server --port $PORT --skip-root-user-initialization &
    SERVER_PID=$!
    sleep 1

    # Assert that the root user cannot log in
    run dolt -u root sql -q "select user, host from mysql.user where user='root';"
    [ $status -ne 0 ]

    # Assert that there is no root user
    run dolt sql -q "select user, host from mysql.user where user='root';"
    [ $status -eq 0 ]
    ! [[ $output =~ "root" ]] || false
}

@test "sql-privs: starting server with empty config works" {
    make_test_repo
    touch server.yaml

    start_sql_server_with_config test_db server.yaml

    run dolt sql -q "select user from mysql.user order by user"
    [ $status -eq 0 ]
    [[ $output =~ "root" ]] || false
    
    dolt sql -q "create user new_user"
    run dolt sql -q "select user from mysql.user order by user"
    [ $status -eq 0 ]
    [[ $output =~ "root" ]] || false
    [[ $output =~ "new_user" ]] || false
    
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

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ dolt ]] || false
    
    dolt sql -q "create user new_user"

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ dolt ]] || false
    [[ $output =~ new_user ]] || false

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

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ dolt ]] || false

    dolt sql -q "create user new_user"

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ dolt ]] || false
    [[ $output =~ new_user ]] || false
    
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

    SQL_USER=dolt
    start_sql_server_with_args --host 0.0.0.0 --privilege-file=privs.json

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    ! [[ $output =~ root ]] || false
    [[ $output =~ dolt ]] || false
    [[ $output =~ privs_user ]] || false

    dolt sql -q "create user new_user"

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    ! [[ $output =~ root ]] || false
    [[ $output =~ dolt ]] || false
    [[ $output =~ new_user ]] || false
    [[ $output =~ privs_user ]] || false

    # Test that privs.json file is not in json format
    run cat privs.json
    ! [[ "$output" =~ "\"User\":\"privs_user\"" ]] || false

    # Restart server
    stop_sql_server
    start_sql_server_with_args --host 0.0.0.0 --privilege-file=privs.json

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    ! [[ $output =~ root ]] || false
    [[ $output =~ dolt ]] || false
    [[ $output =~ new_user ]] || false
    [[ $output =~ privs_user ]] || false
    [[ $output =~ __dolt_local_user__ ]] || false
}

@test "sql-privs: errors instead of panic when reading badly formatted privilege file" {
    make_test_repo
    touch privs.db
    echo "garbage" > privs.db

    run start_sql_server_with_args --host 0.0.0.0 --privilege-file=privs.db
    [ "$status" -eq 1 ]
    [[ "$output" =~ "ill formatted privileges file" ]] || false
}

@test "sql-privs: default options" {
    make_test_repo

    start_sql_server test_db

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ dolt ]] || false

    dolt sql -q "create user new_user"

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ dolt ]] || false
    [[ $output =~ new_user ]] || false

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

    start_sql_server_with_args --host 0.0.0.0 --data-dir=db_dir

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls -a db_dir
    [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ db1 ]] || false
    [[ $output =~ db2 ]] || false
    [[ $output =~ db3 ]] || false
    [[ $output =~ information_schema ]] || false
    [[ $output =~ mysql ]] || false

    run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ dolt ]] || false

    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "create user new_user"

    run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ root ]] || false
    [[ $output =~ new_user ]] || false

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

    start_sql_server_with_args --host 0.0.0.0 --doltcfg-dir=doltcfgdir

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ root ]] || false

    dolt sql -q "create user new_user"

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ root ]] || false
    [[ $output =~ new_user ]] || false
    
    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    run ls doltcfgdir
    [[ "$output" =~ "privileges.db" ]] || false
}

@test "sql-privs: specify privilege file" {
    make_test_repo

    start_sql_server_with_args --host 0.0.0.0 --privilege-file=privs.db

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "privs.db" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls .doltcfg
    ! [[ "$output" =~ "privileges.db" ]] || false

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ root ]] || false

    dolt sql -q "create user new_user"

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ root ]] || false
    [[ $output =~ new_user ]] || false

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "privs.db" ]] || false
}

@test "sql-privs: specify data-dir and doltcfg-dir" {
    make_multi_test_repo

    start_sql_server_with_args --host 0.0.0.0 --data-dir=db_dir --doltcfg-dir=doltcfgdir

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ db1 ]] || false
    [[ $output =~ db2 ]] || false
    [[ $output =~ db3 ]] || false
    [[ $output =~ information_schema ]] || false
    [[ $output =~ mysql ]] || false

    run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ root ]] || false

    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "create user new_user"

    run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ root ]] || false
    [[ $output =~ new_user ]] || false

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

    start_sql_server_with_args --host 0.0.0.0 --data-dir=db_dir --privilege-file=privs.db

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "privs.db" ]] || false

    run ls -a db_dir
    [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ db1 ]] || false
    [[ $output =~ db2 ]] || false
    [[ $output =~ db3 ]] || false
    [[ $output =~ information_schema ]] || false
    [[ $output =~ mysql ]] || false

    run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ root ]] || false

    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "create user new_user"

    run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ root ]] || false
    [[ $output =~ new_user ]] || false

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

    start_sql_server_with_args --host 0.0.0.0 --doltcfg-dir=doltcfgdir --privilege-file=privs.db

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false
    [[ "$output" =~ "privs.db" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ root ]] || false

    dolt sql -q "create user new_user"

    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ dolt ]] || false
    [[ $output =~ new_user ]] || false

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

    start_sql_server_with_args --host 0.0.0.0 --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false
    [[ "$output" =~ "privs.db" ]] || false

    run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ db1 ]] || false
    [[ $output =~ db2 ]] || false
    [[ $output =~ db3 ]] || false
    [[ $output =~ information_schema ]] || false
    [[ $output =~ mysql ]] || false

    run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ root ]] || false

    dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "create user new_user"

    run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db db1 sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ root ]] || false
    [[ $output =~ new_user ]] || false

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
    start_sql_server_with_args --host 0.0.0.0

    dolt sql -q "create user new_user"
    stop_sql_server
    sleep 1
    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false
    run ls -a .doltcfg
    [[ "$output" =~ "privileges.db" ]] || false

    cd db_dir
    start_sql_server_with_args --host 0.0.0.0
    run dolt sql -q "select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ root ]] || false
    [[ $output =~ new_user ]] || false
}

@test "sql-privs: basic lack of privileges tests" {
     make_test_repo
     start_sql_server

     dolt sql -q "create table t1(c1 int)"
     dolt sql -q "create user test"
     dolt sql -q "grant select on test_db.* to test"

     # Should only see test_db database
     run dolt sql -q "show databases"
     [ $status -eq 0 ]
     [[ $output =~ test_db ]] || false
     
     run dolt sql -q "show tables"
     [ $status -eq 0 ]
     [[ $output =~ t1 ]] || false

     # check information_schema.SCHEMA_PRIVILEGES table
     run dolt sql -q "select * from information_schema.SCHEMA_PRIVILEGES;"
     [[ "$output" =~ "| 'test'@'%' | def           | test_db      | SELECT         | NO           |" ]] || false

     # Revoke works as expected
     dolt sql -q "revoke select on test_db.* from test"
     run dolt -u test sql -q "show tables"
     [ $status -ne 0 ]

     # Host in privileges is respected
     dolt sql -q "drop user test"
     dolt sql -q "create user test@'127.0.0.1'"
     dolt sql -q "grant select on test_db.* to test@'127.0.0.1'"
     run dolt sql -q "show tables"
     [ $status -eq 0 ]
     [[ $output =~ t1 ]] || false

     # check information_schema.SCHEMA_PRIVILEGES table
     run dolt sql -q "select * from information_schema.SCHEMA_PRIVILEGES;"
     [[ "$output" =~ "| 'test'@'127.0.0.1' | def           | test_db      | SELECT         | NO           |" ]] || false

     dolt sql -q "grant update on test_db.t1 to test@'127.0.0.1'"
     # check information_schema.TABLE_PRIVILEGES table
     run dolt sql -q "select * from information_schema.TABLE_PRIVILEGES;"
     [[ "$output" =~ "| 'test'@'127.0.0.1' | def           | test_db      | t1         | UPDATE         | NO           |" ]] || false

     dolt sql -q "grant insert on *.* to test@'127.0.0.1'"
     # check information_schema.USER_PRIVILEGES table
     run dolt sql -r csv -q "select * from information_schema.USER_PRIVILEGES;"
     [[ "$output" =~ "'test'@'127.0.0.1',def,INSERT,NO" ]] || false

     dolt sql -q "drop user test@'127.0.0.1'"
     dolt sql -q "create user test@'10.10.10.10'"
     dolt sql -q "grant select on test_db.* to test@'10.10.10.10'"
     # Assert that using the test account results in an authentication error, since only test@10.10.10.10 exists now
     run dolt -u test sql -q "show tables"
     [ $status -ne 0 ]
     [[ "$output" =~ "No authentication methods available for authentication" ]] || false
}

@test "sql-privs: creating user identified by password" {
     make_test_repo
     start_sql_server

     dolt sql -q "create user test identified by 'test'"
     dolt sql -q "grant select on mysql.user to test"

     # Should not be able to connect to test_db
     run dolt -u test -p test --use-db test_db sql -q "select user from mysql.user order by user"
     [ $status -ne 0 ]

     run dolt --port $PORT --host 0.0.0.0 --no-tls -u test -p test --use-db '' sql -q "select user from mysql.user"
     [ $status -eq 0 ]
     [[ $output =~ dolt ]] || false
     [[ $output =~ test ]] || false

     # Bad password can't connect
     run dolt -u test -p bad --use-db '' sql -q "select user from mysql.user order by user"
     [ $status -ne 0 ]
     
     # Should only see mysql database
     run dolt --port $PORT --host 0.0.0.0 --no-tls -u test -p test --use-db '' sql -q "show databases"
     [ $status -eq 0 ]	
     [[ $output =~ mysql ]] || false
     ! [[ $output =~ test_db ]] || false
}

@test "sql-privs: deleting user prevents access by that user" {
     make_test_repo
     start_sql_server

     dolt --port $PORT --host 0.0.0.0 --no-tls --use-db test_db sql -q "create table t1(c1 int)"
     dolt --port $PORT --host 0.0.0.0 --no-tls --use-db '' sql -q "create user test"
     dolt --port $PORT --host 0.0.0.0 --no-tls --use-db '' sql -q "grant select on test_db.* to test"
     run dolt --port $PORT --host 0.0.0.0 --no-tls --use-db test_db sql -q "show tables"
     [ $status -eq 0 ]
     [[ $output =~ t1 ]] || false

     dolt --port $PORT --host 0.0.0.0 --no-tls --use-db '' sql -q "drop user test"

     run dolt -u test --port $PORT --host 0.0.0.0 --no-tls --use-db test_db sql -q "show tables"
     [ $status -ne 0 ]
}

# Don't run this test with a server - we want to ensure that the privileges file
# is being serialized, persisted, and loaded
@test "sql-privs: revoking last privilege doesn't result in corrupted privileges file" {
     make_test_repo

     dolt sql -q "CREATE USER tester@localhost"
     dolt sql -q "GRANT SELECT ON test_db.* TO tester@localhost"
     dolt sql -q "REVOKE SELECT ON test_db.* FROM tester@localhost"

     run dolt sql -q "SHOW GRANTS FOR tester@localhost"
     [ $status -eq 0 ]
     [[ $output =~ "GRANT USAGE ON *.* TO \`tester\`@\`localhost\`" ]] || false
     ! [[ $output =~ "SELECT" ]] || false

     dolt sql -q "GRANT SELECT ON test_db.* TO tester@localhost"
     dolt sql -q "GRANT UPDATE ON test_db.* TO tester@localhost"
     run dolt sql -q "SHOW GRANTS FOR tester@localhost"
     [ $status -eq 0 ]
     [[ $output =~ "GRANT USAGE ON *.* TO \`tester\`@\`localhost\`" ]] || false
     [[ $output =~ "GRANT SELECT, UPDATE ON \`test_db\`.* TO \`tester\`@\`localhost\`" ]] || false

     dolt sql -q "REVOKE UPDATE ON test_db.* FROM tester@localhost"
     run dolt sql -q "SHOW GRANTS FOR tester@localhost"
     [ $status -eq 0 ]
     [[ $output =~ "GRANT USAGE ON *.* TO \`tester\`@\`localhost\`" ]] || false
     [[ $output =~ "GRANT SELECT ON \`test_db\`.* TO \`tester\`@\`localhost\`" ]] || false
     ! [[ $output =~ "UPDATE" ]] || false
}

@test "sql-privs: revoking all privileges doesn't result in a corrupted privileges file" {
     make_test_repo

     dolt sql -q "CREATE USER tester@localhost"
     dolt sql -q "GRANT SELECT ON test_db.* TO tester@localhost"
     dolt sql -q "REVOKE ALL ON test_db.* FROM tester@localhost"

     run dolt sql -q "SHOW GRANTS FOR tester@localhost"
     [ $status -eq 0 ]
     [[ $output =~ "GRANT USAGE ON *.* TO \`tester\`@\`localhost\`" ]] || false
     ! [[ $output =~ "SELECT" ]] || false
}

@test "sql-privs: wildcard user authentication works for IP patterns" {
     make_test_repo

     # Create users with specific IP and wildcard IP patterns (reproduces issue #9624 scenario)
     # Original customer had 'foo'@'10.0.0.1' and 'bar'@'10.0.0.%' 
     dolt sql -q "CREATE USER 'specific_user'@'127.0.0.1' IDENTIFIED BY 'password'"
     dolt sql -q "CREATE USER 'wildcard_user'@'127.0.0.%' IDENTIFIED BY 'password'"
     dolt sql -q "GRANT ALL PRIVILEGES ON test_db.* TO 'specific_user'@'127.0.0.1'"
     dolt sql -q "GRANT ALL PRIVILEGES ON test_db.* TO 'wildcard_user'@'127.0.0.%'"
     dolt sql -q "FLUSH PRIVILEGES"

     PORT=$( definePORT )
     dolt sql-server --host 0.0.0.0 --port=$PORT --socket "dolt.$PORT.sock" &
     SERVER_PID=$!
     sleep 1

     # Test specific IP user authentication (equivalent to customer's 'foo'@'10.0.0.1')
     run mysql --host 127.0.0.1 --port $PORT --user specific_user --password=password -e "SELECT USER(), CONNECTION_ID()"
     [ $status -eq 0 ]
     [[ $output =~ "specific_user@127.0.0.1" ]] || false

     # Test wildcard IP user authentication (equivalent to customer's 'bar'@'10.0.0.%')
     # This was broken before the fix - wildcard patterns failed with "No authentication methods available"
     run mysql --host 127.0.0.1 --port $PORT --user wildcard_user --password=password -e "SELECT USER(), CONNECTION_ID()"
     [ $status -eq 0 ]
     [[ $output =~ "wildcard_user@127.0.0.%" ]] || false

     # Test with dolt client - specific IP user authentication
     run dolt --host=127.0.0.1 --port=$PORT --user=specific_user --password=password --no-tls sql -q "SELECT USER(), CONNECTION_ID()"
     [ $status -eq 0 ]
     [[ $output =~ "specific_user@127.0.0.1" ]] || false

     # Test with dolt client - wildcard IP user authentication
     run dolt --host=127.0.0.1 --port=$PORT --user=wildcard_user --password=password --no-tls sql -q "SELECT USER(), CONNECTION_ID()"
     [ $status -eq 0 ]
     [[ $output =~ "wildcard_user@127.0.0.%" ]] || false

     # Verify authentication works consistently across both MySQL and Dolt clients
     run mysql --host 127.0.0.1 --port $PORT --user wildcard_user --password=password -e "SELECT 'mysql_client_success'"
     [ $status -eq 0 ]
     [[ $output =~ "mysql_client_success" ]] || false

     run dolt --host=127.0.0.1 --port=$PORT --user=wildcard_user --password=password --no-tls sql -q "SELECT 'dolt_client_success'"
     [ $status -eq 0 ]
     [[ $output =~ "dolt_client_success" ]] || false

     stop_sql_server 1
}
