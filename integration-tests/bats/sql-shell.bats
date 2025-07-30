#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
CREATE TABLE test (
  pk BIGINT NOT NULL COMMENT 'tag:0',
  c1 BIGINT COMMENT 'tag:1',
  c2 BIGINT COMMENT 'tag:2',
  c3 BIGINT COMMENT 'tag:3',
  c4 BIGINT COMMENT 'tag:4',
  c5 BIGINT COMMENT 'tag:5',
  PRIMARY KEY (pk)
);
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

# bats test_tags=no_lambda
@test "sql-shell: OkResult is printed in interactive shell" {
    skiponwindows "Need to install expect and make this script work on windows."
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
        skip "shell on server returns Empty Set instead of OkResult"
    fi
    run $BATS_TEST_DIRNAME/sql-shell-ok-result.expect
    [ "$status" -eq 0 ]
}

# bats test_tags=no_lambda
@test "sql-shell: database changed is printed in interactive shell" {
    skiponwindows "Need to install expect and make this script work on windows."
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
        skip "shell on server returns Empty Set instead of OkResult"
    fi
    run $BATS_TEST_DIRNAME/sql-shell-use.expect
    [ "$status" -eq 0 ]
}

# bats test_tags=no_lambda
@test "sql-shell: multi statement query returns accurate timing" {
    skiponwindows "Need to install expect and make this script work on windows."
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
        skip "shell on server returns Empty Set instead of OkResult"
    fi
    dolt sql -q "CREATE TABLE t(a int);"
    dolt sql -q "INSERT INTO t VALUES (1);"
    dolt sql -q "CREATE TABLE t1(b int);"
    run $BATS_TEST_DIRNAME/sql-shell-multi-stmt-timings.expect
    [ "$status" -eq 0 ]
}

# bats test_tags=no_lambda
@test "sql-shell: warnings are not suppressed" {
    skiponwindows "Need to install expect and make this script work on windows."
    run $BATS_TEST_DIRNAME/sql-shell-warnings.expect

    [[ "$output" =~ "Warning" ]] || false
    [[ "$output" =~ "1365" ]] || false
    [[ "$output" =~ "Division by 0" ]] || false
}

# bats test_tags=no_lambda
@test "sql-shell: can toggle warning details" {
    skiponwindows "Need to install expect and make this script work on windows."
    run $BATS_TEST_DIRNAME/sql-warning-summary.expect

    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "Warning (Code 1365): Division by 0\nWarning (Code 1365): Division by 0" ]] || false
}

# bats test_tags=no_lambda
@test "sql-shell: can toggle warning summary" {
   skiponwindows "Need to install expect and make this script work on windows."
   skip " set sql_warnings currently doesn't work --- needs more communication between server & shell"
   run $BATS_TEST_DIRNAME/sql-warning-detail.expect

   [ "$status" -eq 0 ]
   ! [[ "$output" =~ "1 row in set, 3 warnings" ]] || false
}

# bats test_tags=no_lambda
@test "sql-shell: show warnings hides warning summary, and removes whitespace" {
    skiponwindows "Need to install expect and make this script work on windows."
    run $BATS_TEST_DIRNAME/sql-show-warnings.expect

    [ "$status" -eq 0 ]
}

@test "sql-shell: use user without privileges, and no superuser created" {
    rm -rf .doltcfg

    # default user is root
    run dolt sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false

    # create user
    run dolt sql <<< "create user new_user@'localhost'"
    [ "$status" -eq 0 ]

    run dolt --user=new_user sql <<< "select user from mysql.user"
    [ "$status" -eq 1 ]
    # https://github.com/dolthub/dolt/issues/6307 
    [[ "$output" =~ "Access denied for user 'new_user'@'localhost'" ]] || false

    rm -rf .doltcfg
}

@test "sql-shell: pipe query text to sql shell" {
    skiponwindows "Works on Windows command prompt but not the WSL terminal used during bats"
    run bash -c "echo 'show tables' | dolt sql"
    [ $status -eq 0 ]
    [[ "$output" =~ "test" ]] || false
}

# bats test_tags=no_lambda
@test "sql-shell: sql shell writes to disk after every iteration (autocommit)" {
    skiponwindows "Need to install expect and make this script work on windows."
    run $BATS_TEST_DIRNAME/sql-shell.expect
    echo "$output"

    # 2 tables are created. 1 from above and 1 in the expect file.
    [[ "$output" =~ "+---------------------" ]] || false
    [[ "$output" =~ "| Tables_in_dolt-repo-" ]] || false
    [[ "$output" =~ "+---------------------" ]] || false
    [[ "$output" =~ "| test                " ]] || false
    [[ "$output" =~ "| test_expect         " ]] || false
    [[ "$output" =~ "+---------------------" ]] || false
}

# bats test_tags=no_lambda
@test "sql-shell: sql shell executes slash commands" {
    skiponwindows "Need to install expect and make this script work on windows."
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "Current test setup results in remote calls having a clean branch, where this expect script expects dirty."
    fi
    run $BATS_TEST_DIRNAME/sql-shell-slash-cmds.expect
    echo "$output"

    [ "$status" -eq 0 ]
}

# bats test_tags=no_lambda
@test "sql-shell: sql shell prompt updates" {
    skiponwindows "Need to install expect and make this script work on windows."
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "Presently sql command will not connect to remote server due to lack of lock file where there are not DBs."
    fi

    # start in an empty directory
    rm -rf .dolt
    mkdir sql_shell_test
    cd sql_shell_test

    $BATS_TEST_DIRNAME/sql-shell-prompt.expect
}

# bats test_tags=no_lambda
@test "sql-shell: shell works after failing query" {
    skiponwindows "Need to install expect and make this script work on windows."
    $BATS_TEST_DIRNAME/sql-works-after-failing-query.expect
}

# bats test_tags=no_lambda
@test "sql-shell: empty DB in prompt is OK" {
    skiponwindows "Need to install expect and make this script work on windows."
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "Presently sql command will not connect to remote server due to lack of lock file where there are not DBs."
    fi
    # ignore common setup. Use an empty db with no server.
    rm -rf .dolt
    mkdir emptyDb
    cd emptyDb
    $BATS_TEST_DIRNAME/sql-shell-empty-prompt.expect
}

@test "sql-shell: works with ANSI_QUOTES SQL mode" {
    if [ $SQL_ENGINE = "remote-engine" ]; then
      skip "Presently sql command will not connect to remote server due to lack of lock file where there are not DBs."
    fi

    mkdir doltsql
    cd doltsql
    dolt init

    dolt sql << SQL
SET @@SQL_MODE=ANSI_QUOTES;
CREATE TABLE "table1"("pk" int primary key, "col1" int DEFAULT ("pk"));
CREATE TRIGGER trigger1 BEFORE INSERT ON "table1" FOR EACH ROW SET NEW."pk" = NEW."pk" + 1;
INSERT INTO "table1" ("pk") VALUES (1);
CREATE VIEW "view1" AS select "pk", "col1" from "table1";
CREATE PROCEDURE procedure1() SELECT "pk", "col1" from "table1";
SQL

    # In a new session, with SQL_MODE set back to the default modes, assert that
    # we can still use the entities we created with ANSI_QUOTES.
    run dolt sql -q "INSERT INTO table1 (pk) VALUES (111);"
    [ $status -eq "0" ]

    run dolt sql -r csv -q "SELECT * from table1;"
    [ $status -eq "0" ]
    [[ $output =~ "2,1" ]] || false
    [[ $output =~ "112,111" ]] || false

    run dolt sql -q "show tables;"
    [ $status -eq "0" ]
    [[ $output =~ "table1" ]] || false
    [[ $output =~ "view1" ]] || false

    run dolt sql -r csv -q "SELECT * from view1;"
    [ $status -eq "0" ]
    [[ $output =~ "2,1" ]] || false
    [[ $output =~ "112,111" ]] || false

    run dolt sql -r csv -q "call procedure1;"
    [ $status -eq "0" ]
    [[ $output =~ "2,1" ]] || false
    [[ $output =~ "112,111" ]] || false
}

# bats test_tags=no_lambda
@test "sql-shell: delimiter" {
    skiponwindows "Need to install expect and make this script work on windows."
    mkdir doltsql
    cd doltsql
    dolt init

    run $BATS_TEST_DIRNAME/sql-delimiter.expect
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "Error" ]] || false
    [[ ! "$output" =~ "error" ]] || false

    run dolt sql -q "SELECT * FROM test ORDER BY 1" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "pk,v1" ]] || false
    [[ "$output" =~ "0,0" ]] || false
    [[ "$output" =~ "1,1" ]] || false
    [[ "${#lines[@]}" = "3" ]] || false

    run dolt sql -q "SHOW TRIGGERS"
    [ "$status" -eq "0" ]
    [[ "$output" =~ "SET NEW.v1 = NEW.v1 * 11" ]] || false

    cd ..
    rm -rf doltsql
}

# bats test_tags=no_lambda
@test "sql-shell: use databases" {
    skiponwindows "Need to install expect and make this script work on windows."
    mkdir doltsql
    cd doltsql
    dolt init
    dolt sql -q "create database db1"
    dolt sql -q "create database db2"

    dolt branch test

    run expect $BATS_TEST_DIRNAME/sql-use.expect
    echo $output
    
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "Error" ]] || false
    [[ ! "$output" =~ "error" ]] || false

    cd ..
    rm -rf doltsql
}

@test "sql-shell: default datadir, doltcfg, and privs" {
    # remove config files
    rm -rf .doltcfg

    run dolt sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    run ls .doltcfg
    ! [[ "$output" =~ "privileges.db" ]] || false

    run dolt sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    run dolt sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false

    run ls .doltcfg
    [[ "$output" =~ "privileges.db" ]] || false

    rm -rf .doltcfg
}

@test "sql-shell: specify data-dir" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "Remote behavior differs"
    fi

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir

    # create data dir
    mkdir db_dir
    cd db_dir

    # create databases
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

    # show databases, expect all
    run dolt --data-dir=db_dir sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt --data-dir=db_dir sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt --data-dir=db_dir sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --data-dir=db_dir sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls -a db_dir
    [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls db_dir/.doltcfg
    [[ "$output" =~ "privileges.db" ]] || false

    # test relative to $datadir
    cd db_dir

    # show databases, expect all
    run dolt sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # expect to find same users when in $datadir
    run dolt sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
}

@test "sql-shell: specify doltcfg directory" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "Remote behavior differs"
    fi
    # remove any previous config directories
    rm -rf .doltcfg
    rm -rf doltcfgdir

    # show users, expect just root user
    run dolt --doltcfg-dir=doltcfgdir sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false

    # create new_user
    run dolt --doltcfg-dir=doltcfgdir sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --doltcfg-dir=doltcfgdir sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    run ls doltcfgdir
    [[ "$output" =~ "privileges.db" ]] || false

    # remove files
    rm -rf .doltcfg
    rm -rf doltcfgdir
}

@test "sql-shell: specify privilege file" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "Remote behavior differs"
    fi
    # remove config files
    rm -rf .doltcfg
    rm -f privs.db

    # show users, expect just root user
    run dolt --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    # create new_user
    run dolt --privilege-file=privs.db sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "privs.db" ]] || false

    # expect to not see new_user when privs.db not specified
    run dolt sql <<< "select user from mysql.user"
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # remove config files
    rm -rf .doltcfg
    rm -f privs.db
}

@test "sql-shell: specify data-dir and doltcfg-dir" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "Remote behavior differs"
    fi

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf doltcfgdir

    # create data dir
    mkdir db_dir
    cd db_dir

    # create databases
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

    # show databases, expect all
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls db_dir
    ! [[ "$output" =~ "privileges.db" ]] || false

    run ls doltcfgdir
    [[ "$output" =~ "privileges.db" ]] || false

    # test relative to $datadir
    cd db_dir

    # show databases, expect all
    run dolt sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect root
    run dolt sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # show users, expect root and new_user
    run dolt --doltcfg-dir=../doltcfgdir sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf doltcfgdir
}

@test "sql-shell: specify data-dir and privilege-file" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "Remote behavior differs"
    fi

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf privs.db

    # create data dir
    mkdir db_dir
    cd db_dir

    # create databases
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

    # show databases, expect all
    run dolt --data-dir=db_dir --privilege-file=privs.db sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt --data-dir=db_dir --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    dolt --data-dir=db_dir --privilege-file=privs.db sql <<< "create user new_user"

    # show users, expect root user and new_user
    run dolt --data-dir=db_dir --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "privs.db" ]] || false

    run ls -a db_dir
    [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    run ls db_dir/.doltcfg
    ! [[ "$output" =~ "privs.db" ]] || false

    # test relative to $datadir
    cd db_dir

    # show databases, expect all
    run dolt sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect root
    run dolt sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # show users, expect root and new_user
    run dolt --privilege-file=../privs.db sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf privs.db
}

@test "sql-shell: specify doltcfg-dir and privilege-file" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "Remote behavior differs"
    fi
    # remove any previous config directories
    rm -rf .doltcfg
    rm -rf doltcfgdir
    rm -rf privs.db

    # show users, expect just root user
    run dolt --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false

    # create new_user
    run dolt --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false
    [[ "$output" =~ "privs.db" ]] || false

    run ls doltcfgdir
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    # remove config directory just in case
    rm -rf .doltcfg
    rm -rf doltcfgdir
    rm -rf privs.db
}

@test "sql-shell: specify data directory, cfg directory, and privilege file" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "Remote behavior differs"
    fi

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf doltcfgdir
    rm -rf privs.db

    # create data dir
    mkdir db_dir
    cd db_dir

    # create databases
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

    # show databases, expect all
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect just root user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ ".doltcfg" ]] || false
    ! [[ "$output" =~ "doltcfgdir" ]] || false

    run ls -a db_dir
    ! [[ "$output" =~ ".doltcfg" ]] || false

    # create new user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    # show users, expect root user and new_user
    run dolt --data-dir=db_dir --doltcfg-dir=doltcfgdir --privilege-file=privs.db sql <<< "select user from mysql.user;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    run ls -a
    ! [[ "$output" =~ "privileges.db" ]] || false
    [[ "$output" =~ "privs.db" ]] || false
    ! [[ "$output" =~ ".doltcfg" ]] || false
    [[ "$output" =~ "doltcfgdir" ]] || false

    run ls db_dir
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    run ls doltcfgdir
    ! [[ "$output" =~ "privileges.db" ]] || false
    ! [[ "$output" =~ "privs.db" ]] || false

    # test relative to $datadir
    cd db_dir

    # show databases, expect all
    run dolt sql <<< "show databases;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "db1" ]] || false
    [[ "$output" =~ "db2" ]] || false
    [[ "$output" =~ "db3" ]] || false

    # show users, expect root
    run dolt sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # show users, expect root and new_user
    run dolt --doltcfg-dir=../doltcfgdir sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    ! [[ "$output" =~ "new_user" ]] || false

    # show users, expect root and new_user
    run dolt --privilege-file=../privs.db sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    cd ..

    # remove config files
    rm -rf .doltcfg
    rm -rf db_dir
    rm -rf doltcfgdir
    rm -rf privs.db
}


@test "sql-shell: .doltcfg in parent directory errors" {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "Remote behavior differs"
    fi

    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db

    mkdir .doltcfg
    mkdir inner_db
    cd inner_db
    mkdir .doltcfg

    run dolt sql <<< "show databases;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "multiple .doltcfg directories detected" ]] || false

    # specifying datadir, resolves issue
    run dolt --data-dir=. sql <<< "show databases;"
    [ "$status" -eq 0 ]

    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db
}

@test "sql-shell: .doltcfg defaults to parent directory" {
    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db

    # create user in parent
    run dolt sql <<< "create user new_user"
    [ "$status" -eq 0 ]

    run dolt sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # check that .doltcfg and privileges.db was created
    run ls -a
    [[ "$output" =~ ".doltcfg" ]] || false
    run ls .doltcfg
    [[ "$output" =~ "privileges.db" ]] || false

    mkdir inner_db
    cd inner_db
    run dolt sql <<< "select user from mysql.user"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "root" ]] || false
    [[ "$output" =~ "new_user" ]] || false

    # remove existing directories
    rm -rf .doltcfg
    rm -rf inner_db
}

@test "sql-shell: specify data directory outside of dolt repo" {
    # remove files
    rm -rf datadir
    rm -rf .doltcfg
    rm -rf new_repo

    # initialize data directory and inner dbs
    mkdir datadir
    cd datadir

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

    # save data path
    DATADIR=$(pwd)

    cd ..

    # initialize new repo
    mkdir new_repo
    cd new_repo

    run dolt --data-dir=$DATADIR sql <<< "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "db1" ]] || false
    [[ $output =~ "db2" ]] || false
    [[ $output =~ "db3" ]] || false

    run dolt --data-dir=$DATADIR sql <<< "create user new_user"
    [ $status -eq 0 ]

    run dolt --data-dir=$DATADIR sql <<< "use db1; select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ "new_user" ]] || false

    run dolt --data-dir=$DATADIR sql <<< "use db2; select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ "new_user" ]] || false

    run dolt --data-dir=$DATADIR sql <<< "use db3; select user from mysql.user"
    [ $status -eq 0 ]
    [[ $output =~ "new_user" ]] || false

    # check that correct files exist
    cd ..

    run ls -a
    [[ $output =~ "datadir" ]] || false
    [[ $output =~ "new_repo" ]] || false
    ! [[ $output =~ ".doltcfg" ]] || false

    run ls -a datadir
    [[ $output =~ ".doltcfg" ]] || false

    run ls -a datadir/.doltcfg
    [[ $output =~ "privileges.db" ]] || false

    # remove files
    rm -rf new_repo
    rm -rf datadir
}

@test "sql-shell: bad sql in sql shell should error" {
    run dolt sql <<< "This is bad sql"
    [ $status -eq 1 ]
    run dolt sql <<< "select * from test; This is bad sql; insert into test (pk) values (666); select * from test;"
    [ $status -eq 1 ]
    [[ ! "$output" =~ "666" ]] || false
}

@test "sql-shell: inline query with missing -q flag should error" {
    run dolt sql "SELECT * FROM test;"
    [ $status -eq 1 ]
    [[ "$output" =~ "does not take positional arguments, but found 1" ]] || false
}

@test "sql-shell: validate string formatting" {
      dolt sql <<SQL
CREATE TABLE test2 (
  str varchar(256) NOT NULL,
  PRIMARY KEY (str)
);
SQL
  dolt add .
  dolt commit -m "created table"

  TESTSTR='0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ`~!@#$%^&*()){}[]/=?+|,.<>;:_-_%d%s%f'
  dolt sql -q "INSERT INTO test2 (str) VALUES ('$TESTSTR')"

  run dolt sql -q "SELECT * FROM test2"
  [ $status -eq 0 ]
  [[ "$output" =~ "$TESTSTR" ]] || false

  run dolt sql -q "SELECT * FROM test2" -r csv
  [ $status -eq 0 ]
  [[ "$output" =~ "$TESTSTR" ]] || false

  dolt sql -q "SELECT * FROM test2" -r json
  run dolt sql -q "SELECT * FROM test2" -r json
  [ $status -eq 0 ]
  [[ "$output" =~ "$TESTSTR" ]] || false

  dolt add .
  dolt commit -m "added data"

  run dolt diff HEAD^
  [ $status -eq 0 ]
  echo $output
  [[ "$output" =~ "$TESTSTR" ]] || false
}

@test "sql-shell: active branch after checkout" {
    run dolt sql <<< "select active_branch()"
    [ $status -eq 0 ]
    [[ "$output" =~ "active_branch()" ]] || false
    [[ "$output" =~ "main" ]] || false
    run dolt sql <<< "call dolt_checkout('-b', 'tmp_br'); select active_branch()"
    [ $status -eq 0 ]
    [[ "$output" =~ "active_branch()" ]] || false
    [[ "$output" =~ "tmp_br" ]] || false
}

# bats test_tags=no_lambda
@test "sql-shell: printed query time is accurate" {
    expect -c '
set timeout 2
spawn dolt sql
for {set i 0} {$i < 3} {incr i} {
  expect "> "
  send -- "select sleep(1);\r"
  expect {
    timeout {
      puts "test failure: expected to see a query result that took ~1 second, but did not"
      exit 1;
    }
    "1 row in set (1"
  }
}
expect "> "
send -- "quit;\r"
expect eof
'
}

@test "sql-shell: dolt_thread_dump" {
    run dolt sql <<< "call dolt_thread_dump();"
    [ $status -eq 0 ]
    [[ "$output" =~ "github.com/dolthub/dolt/go" ]] || false
    [[ "$output" =~ "github.com/dolthub/go-mysql-server" ]] || false
}

# bats test_tags=no_lambda
@test "sql-shell: commit time set correctly in shell" {
        skiponwindows "Need to install expect and make this script work on windows."

        run $BATS_TEST_DIRNAME/sql-shell-commit-time.expect
        [ "$status" -eq 0 ]
}

@test "sql-shell: -binary-as-hex, -skip-binary-as-hex flag is respected in server and local contexts" {
#    skiponwindows "Missing Dependencies"
    which expect > /dev/null || skip "expect is not installed"

    # Default behavior for interactive runs is to output binary as hex
    run expect "$BATS_TEST_DIRNAME"/sql-shell-binary-as-hex.expect
    [ "$status" -eq 0 ]

    run expect "$BATS_TEST_DIRNAME"/sql-shell-binary-as-hex.expect --binary-as-hex
    [ "$status" -eq 0 ]

    run expect "$BATS_TEST_DIRNAME"/sql-shell-binary-as-hex.expect --skip-binary-as-hex
    [ "$status" -eq 0 ]

    run expect "$BATS_TEST_DIRNAME"/sql-shell-binary-as-hex.expect --binary-as-hex --skip-binary-as-hex
    [ "$status" -eq 3 ]

    # Non-interactive runs should not output binary as hex by default
    run dolt sql -q "SELECT * FROM test_vbin"
    [ "$status" -eq 0 ]
    [[ ! $output =~ 0x[0-9A-F]+ ]]

    run dolt sql -q "SELECT * FROM test_bin"
    [ "$status" -eq 0 ]
    [[ ! $output =~ 0x[0-9A-F]+ ]]

    run dolt sql -q "SELECT * FROM test_vbin" --binary-as-hex
    [ "$status" -eq 0 ]
    [[ $output =~ 1.*0x616263 ]] || false
    [[ $output =~ 2.*0x0A000000001000112233 ]] || false
    [[ $output =~ 3.*0x ]] || false

    run dolt sql -q "SELECT * FROM test_bin" --binary-as-hex
    [ $status -eq 0 ]
    [[ $output =~ 1.*0x61626300000000000000 ]] || false
    [[ $output =~ 2.*0x0A000000001000112233 ]] || false
    [[ $output =~ 3.*0x00000000000000000000 ]] || false

    run dolt sql -q "SELECT * FROM test_vbin" --skip-binary-as-hex
    [ "$status" -eq 0 ]
    [[ ! $output =~ 0x[0-9A-F]+ ]]

    run dolt sql -q "SELECT * FROM test_bin" --skip-binary-as-hex
    [ "$status" -eq 0 ]
    [[ ! $output =~ 0x[0-9A-F]+ ]]

    run dolt sql -q "" --binary-as-hex --skip-binary-as-hex
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot use both --binary-as-hex and --skip-binary-as-hex" ]] || false

    # Check other formats output is correct
    run dolt sql -r csv -q "SELECT * FROM test_vbin WHERE id = 1;" --binary-as-hex
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,0x616263" ]] || false

    run dolt sql -r csv -q "SELECT * FROM test_vbin WHERE id = 1;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,abc" ]] || false

    run dolt sql -r csv -q "SELECT * FROM test_bin" --binary-as-hex
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1,0x61626300000000000000" ]] || false
    [[ "$output" =~ "2,0x0A000000001000112233" ]] || false
    [[ "$output" =~ "3,0x00000000000000000000" ]] || false
}