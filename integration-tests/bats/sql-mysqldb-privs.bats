#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

# This suite of tests is for testing the sql server's presentation of privileges, privilege persistence between
# CLI and server instances.
#
# Caring about privileges on the CLI isn't really the point, but working in both modes ensures that persistence
# is working correctly. You won't see mention of working with servers in these tests because it's handled by
# running tests in this file using helper/local-remote.bash

# working dir will be test_db
make_multi_test_repo() {
    rm -rf test_db
    mkdir test_db
    cd test_db

    mkdir db1
    cd db1
    dolt init
    cd ..

    mkdir db2
    cd db2
    dolt init
    cd ..

    ## All tests need a user, or two.
    dolt sql -q "CREATE USER tester1@localhost"
    dolt sql -q "CREATE USER tester2@localhost"
}

# working dir will be dolt_repo$$
delete_test_repo() {
    cd ..
    rm -rf test_db
}

setup() {
    setup_no_dolt_init
    make_multi_test_repo
}

teardown() {
    delete_test_repo
    teardown_common
}

@test "sql-mysqldb-privs: smoke test for db table" {
    dolt sql -q "GRANT SELECT ON db1.* TO tester1@localhost"
    run dolt sql -q "SELECT host,user,db,select_priv as s,insert_priv as i from mysql.db"

    [ $status -eq 0 ]
    [[ $output =~ "localhost | tester1 | db1 | Y | N" ]] || false

    run dolt sql -q "SHOW GRANTS FOR tester1@localhost"
    [ $status -eq 0 ]
    [[ $output =~ 'GRANT SELECT ON `db1`.* TO `tester1`@`localhost`' ]] || false

    dolt sql -q "GRANT INSERT ON db2.* TO tester2@localhost"

    run dolt sql -q "SELECT user FROM mysql.db"
    [ $status -eq 0 ]
    [[ $output =~ "tester1" ]] || false
    [[ $output =~ "tester2" ]] || false

    run dolt sql -q "SELECT db FROM mysql.db where user = 'tester2'"
    [ $status -eq 0 ]
    [[ $output =~ "db2" ]] || false
    ! [[ $output =~ "db1" ]] || false

    run dolt sql -q "SHOW GRANTS FOR tester2@localhost"
    [ $status -eq 0 ]
    [[ $output =~ 'GRANT INSERT ON `db2`.* TO `tester2`@`localhost`' ]] || false

    dolt sql -q "REVOKE SELECT ON db1.* FROM tester1@localhost"
    run dolt sql -q "SELECT user FROM mysql.db"
    [ $status -eq 0 ]
    ! [[ $output =~ "tester1" ]] || false
    [[ $output =~ "tester2" ]] || false
}

@test "sql-mysqldb-privs: smoke test for tables_priv table" {
    dolt sql -q "GRANT SELECT ON db1.tbl TO tester1@localhost"
    run dolt sql -q "SELECT host,user,db,table_name as t,table_priv FROM mysql.tables_priv"

    [ $status -eq 0 ]
    [[ $output =~ "localhost | tester1 | db1 | tbl | Select" ]] || false

    run dolt sql -q "SHOW GRANTS FOR tester1@localhost"
    [ $status -eq 0 ]
    [[ $output =~ 'GRANT SELECT ON `db1`.`tbl` TO `tester1`@`localhost`' ]] || false

    dolt sql -q "GRANT INSERT ON db1.tbl TO tester2@localhost"

    run dolt sql -q "SELECT user FROM mysql.tables_priv"
    [ $status -eq 0 ]
    [[ $output =~ "tester1" ]] || false
    [[ $output =~ "tester2" ]] || false

    run dolt sql -q "SELECT user,table_priv FROM mysql.tables_priv"
    [ $status -eq 0 ]
    [[ $output =~ "tester1 | Select" ]] || false
    [[ $output =~ "tester2 | Insert" ]] || false

    run dolt sql -q "SHOW GRANTS FOR tester2@localhost"
    [ $status -eq 0 ]
    [[ $output =~ 'GRANT INSERT ON `db1`.`tbl` TO `tester2`@`localhost`' ]] || false

    dolt sql -q "REVOKE SELECT ON db1.tbl FROM tester1@localhost"
    run dolt sql -q "SELECT user FROM mysql.tables_priv"
    [ $status -eq 0 ]
    ! [[ $output =~ "tester1" ]] || false
    [[ $output =~ "tester2" ]] || false
}

@test "sql-mysqldb-privs: smoke test for procs_priv table" {
    dolt sql -q "GRANT EXECUTE ON PROCEDURE db1.dolt_log TO tester1@localhost"
    run dolt sql -q "SELECT host,user,db,routine_name,routine_type,proc_priv FROM mysql.procs_priv"

    [ $status -eq 0 ]
    [[ $output =~ "localhost | tester1 | db1 | dolt_log     | PROCEDURE    | Execute" ]] || false

    run dolt sql -q "SHOW GRANTS FOR tester1@localhost"
    [ $status -eq 0 ]
    [[ $output =~ 'GRANT EXECUTE ON PROCEDURE `db1`.`dolt_log` TO `tester1`@`localhost`' ]] || false

    dolt sql -q "GRANT GRANT OPTION ON PROCEDURE db1.dolt_diff TO tester2@localhost"

    run dolt sql -q "SELECT user FROM mysql.procs_priv"
    [ $status -eq 0 ]
    [[ $output =~ "tester1" ]] || false
    [[ $output =~ "tester2" ]] || false

    run dolt sql -q "SELECT routine_name FROM mysql.procs_priv where user = 'tester2'"
    [ $status -eq 0 ]
    [[ $output =~ "dolt_diff" ]] || false
    ! [[ $output =~ "dolt_log" ]] || false

    run dolt sql -q "SHOW GRANTS FOR tester2@localhost"
    [ $status -eq 0 ]

    [[ $output =~ 'GRANT USAGE ON PROCEDURE `db1`.`dolt_diff` TO `tester2`@`localhost` WITH GRANT OPTION' ]] || false

    dolt sql -q "REVOKE EXECUTE ON PROCEDURE db1.dolt_log FROM tester1@localhost"
    run dolt sql -q "SELECT user FROM mysql.procs_priv"
    [ $status -eq 0 ]
    ! [[ $output =~ "tester1" ]] || false
    [[ $output =~ "tester2" ]] || false
}

@test "sql-mysqldb-privs: procs_priv table should differentiate between functions and procedures" {
    skip "Function Support is currently disabled"

    dolt sql -q "GRANT EXECUTE ON FUNCTION db1.dolt_log TO tester1@localhost"
    run dolt sql -q "SELECT host,user,db,routine_name as name,routine_type as type,proc_priv FROM mysql.procs_priv"
    [ $status -eq 0 ]
    [[ $output =~ "localhost | tester1 | db1 | dolt_log | FUNCTION | Execute" ]] || false

    # revoking a procedure by the same name should not revoke the function permission
    dolt sql -q "REVOKE EXECUTE ON PROCEDURE db1.dolt_log FROM tester1@localhost"
    run dolt sql -q "SELECT host,user,db,routine_name as name,routine_type as type,proc_priv FROM mysql.procs_priv"
    [ $status -eq 0 ]
    [[ $output =~ "localhost | tester1 | db1 | dolt_log | FUNCTION | Execute" ]] || false

    dolt sql -q "REVOKE EXECUTE ON FUNCTION db1.dolt_log FROM tester1@localhost"
    run dolt sql -q "SELECT host,user,db,routine_name as name,routine_type as type,proc_priv FROM mysql.procs_priv"
    [ $status -eq 0 ]
    ! [[ $output =~ "localhost | tester1 | db1 | dolt_log | FUNCTION | Execute" ]] || false
}

@test "sql-mysqldb-privs: revoke of non-existent permissions" {
    dolt sql -q "REVOKE INSERT ON db1.* FROM tester1@localhost"
    run dolt sql -q "SELECT user FROM mysql.db"
    [ $status -eq 0 ]
    ! [[ $output =~ "tester1" ]] || false

    dolt sql -q "REVOKE INSERT ON db1.tbl FROM tester1@localhost"
    run dolt sql -q "SELECT user FROM mysql.tables_priv"
    [ $status -eq 0 ]
    ! [[ $output =~ "tester1" ]] || false

    dolt sql -q "REVOKE EXECUTE ON PROCEDURE db1.dolt_log FROM tester1@localhost"
    run dolt sql -q "SELECT user FROM mysql.procs_priv"
    [ $status -eq 0 ]
    ! [[ $output =~ "tester1" ]] || false
}
