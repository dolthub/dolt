#! /usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir "$1"
  cd "$1"
  dolt init
  dolt sql -q "create table $1_tbl (id int)"
  dolt sql <<SQL
CREATE TABLE t (pk int PRIMARY KEY);
CREATE TABLE u (pk int PRIMARY KEY);
INSERT INTO dolt_ignore VALUES ('generated_*', 1);
SQL
  dolt add -A && dolt commit -m "tables t, u"
  dolt sql <<SQL
INSERT INTO  t VALUES (1),(2),(3);
INSERT INTO  u VALUES (1),(2),(3);
CREATE TABLE v (pk int PRIMARY KEY);
CREATE TABLE generated_foo (pk int PRIMARY KEY);
SQL
  dolt add t
  cd ..
}

setup() {
    setup_no_dolt_init
    make_repo defaultDB
    make_repo altDB
}

teardown() {
    stop_sql_server 1
    teardown_common
}

@test "sql-local-remote: test switch between server/no server" {
    start_sql_server defaultDB

    run dolt --verbose-engine-setup --user dolt sql -q "show databases" 
    [ "$status" -eq 0 ] || false
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "defaultDB" ]] || false
    [[ "$output" =~ "altDB" ]] || false

    stop_sql_server 1

    run dolt --verbose-engine-setup sql -q "show databases" 
    [ "$status" -eq 0 ] || false
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "defaultDB" ]] || false
    [[ "$output" =~ "altDB" ]] || false
}

@test "sql-local-remote: check --data-dir pointing to a server root can be used when in different directory." {
    start_sql_server altDb
    ROOT_DIR=$(pwd)

    mkdir someplace_else
    cd someplace_else

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt --use-db altDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "defaultDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    stop_sql_server 1

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt --use-db altDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "defaultDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false
}


@test "sql-local-remote: check --data-dir pointing to a database root can be used when in different directory." {
    start_sql_server altDb
    ROOT_DIR=$(pwd)

    mkdir -p someplace_new/fun
    cd someplace_new/fun

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR/altDB" --user dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR/altDB" --user dolt --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "defaultDB does not exist" ]] || false

    stop_sql_server 1

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR/altDB" --user dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR/altDB" --user dolt --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "defaultDB does not exist" ]] || false
}

@test "sql-local-remote: test 'status' and switch between server/no server" {
    start_sql_server defaultDB

    run dolt --user dolt status
    [ "$status" -eq 0 ] || false
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "  (use \"dolt reset <table>...\" to unstage)" ]] || false
    [[ "$output" =~ "	modified:         t" ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to update what will be committed)" ]] || false
    [[ "$output" =~ "  (use \"dolt checkout <table>\" to discard changes in working directory)" ]] || false
    [[ "$output" =~ "	modified:         u" ]] || false
    [[ "$output" =~ "Untracked tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        v" ]] || false
    ! [[ "$output" =~ "   new table:        generated_foo" ]] || false

    run dolt --user dolt status --ignored
    [ "$status" -eq 0 ] || false
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "  (use \"dolt reset <table>...\" to unstage)" ]] || false
    [[ "$output" =~ "	modified:         t" ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to update what will be committed)" ]] || false
    [[ "$output" =~ "  (use \"dolt checkout <table>\" to discard changes in working directory)" ]] || false
    [[ "$output" =~ "	modified:         u" ]] || false
    [[ "$output" =~ "Untracked tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        v" ]] || false
    [[ "$output" =~ "Ignored tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add -f <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        generated_foo" ]] || false

    stop_sql_server 1

    run dolt status
    [ "$status" -eq 0 ] || false
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "  (use \"dolt reset <table>...\" to unstage)" ]] || false
    [[ "$output" =~ "	modified:         t" ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to update what will be committed)" ]] || false
    [[ "$output" =~ "  (use \"dolt checkout <table>\" to discard changes in working directory)" ]] || false
    [[ "$output" =~ "	modified:         u" ]] || false
    [[ "$output" =~ "Untracked tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        v" ]] || false
    ! [[ "$output" =~ "   new table:        generated_foo" ]] || false

    run dolt --user dolt status --ignored
    [ "$status" -eq 0 ] || false
    [[ "$output" =~ "On branch main" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "  (use \"dolt reset <table>...\" to unstage)" ]] || false
    [[ "$output" =~ "	modified:         t" ]] || false
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to update what will be committed)" ]] || false
    [[ "$output" =~ "  (use \"dolt checkout <table>\" to discard changes in working directory)" ]] || false
    [[ "$output" =~ "	modified:         u" ]] || false
    [[ "$output" =~ "Untracked tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        v" ]] || false
    [[ "$output" =~ "Ignored tables:" ]] || false
    [[ "$output" =~ "  (use \"dolt add -f <table>\" to include in what will be committed)" ]] || false
    [[ "$output" =~ "	new table:        generated_foo" ]] || false
}
