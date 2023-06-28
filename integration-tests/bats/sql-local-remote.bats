#! /usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir "$1"
  cd "$1"
  dolt init
  dolt sql -q "create table $1_tbl (id int)"
  dolt sql <<SQL
CREATE TABLE table1 (pk int PRIMARY KEY);
CREATE TABLE table2 (pk int PRIMARY KEY);
INSERT INTO dolt_ignore VALUES ('generated_*', 1);
SQL
  dolt add -A && dolt commit -m "tables table1, table2"
  dolt sql <<SQL
INSERT INTO  table1 VALUES (1),(2),(3);
INSERT INTO  table2 VALUES (1),(2),(3);
CREATE TABLE table3 (pk int PRIMARY KEY);
CREATE TABLE generated_foo (pk int PRIMARY KEY);
SQL
  dolt add table1
  cd ..
}

setup() {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test tests remote connections directly, SQL_ENGINE is not needed."
    fi
    setup_no_dolt_init
    unset DOLT_CLI_PASSWORD
    unset DOLT_SILENCE_USER_REQ_FOR_TESTING
    make_repo defaultDB
    make_repo altDB
}

teardown() {
    stop_sql_server 1
    teardown_common
}

get_staged_tables() {
    dolt status | awk '
        match($0, /new table:\ */) { print substr($0, RSTART+RLENGTH) }
        /Untracked tables:/ { exit }
        /Tables with conflicting dolt_ignore patterns:/ { exit }
    '
}

basic_conflict() {
    dolt dolt sql -q "create table t (i int primary key, t text)"
    dolt dolt add .
    dolt dolt commit -am "init commit"
    dolt dolt checkout -b other
    dolt dolt sql -q "insert into t values (1,'other')"
    dolt dolt commit -am "other commit"
    dolt dolt checkout main
    dolt dolt sql -q "insert into t values (1,'main')"
    dolt dolt commit -am "main commit"
}

extract_value() {
    key="$1"
    input="$2"
    echo "$input" | awk "
        BEGIN { in_value = 0 }
        /$key: {/ { in_value = 1; next }
        match("'$0'", /$key: /) { print substr("'$0'", RSTART+RLENGTH) }
        /}/ { if (in_value) { in_value = 0 } }
        in_value { gsub(/^[ \t]+/, \"\"); print }
    "
}

assert_has_key() {
    key="$1"
    input="$2"
    extracted=$(extract_value "$key" "$input")
    if [[ -z $extracted ]]; then
        echo "Expected to find key $key"
        return 1
    else
        return 0
    fi
}

assert_has_key_value() {
    key="$1"
    value="$2"
    input="$3"
    extracted=$(extract_value "$key" "$input")
    if [[ "$extracted" != "$value" ]]; then
        echo "Expected key $key to have value $value, instead found $extracted"
        return 1
    else
        return 0
    fi
}

@test "sql-local-remote: test switch between server/no server" {
    start_sql_server defaultDB

    run dolt --verbose-engine-setup --user dolt --password "" sql -q "show databases"
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

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt --password "" --use-db altDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt --password "" --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "defaultDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt --password "" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    stop_sql_server 1

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt --password "" --use-db altDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt --password "" --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "defaultDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR" --user dolt --password "" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false
}


@test "sql-local-remote: check --data-dir pointing to a database root can be used when in different directory." {
    start_sql_server altDb
    ROOT_DIR=$(pwd)

    mkdir -p someplace_new/fun
    cd someplace_new/fun

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR/altDB" --user dolt --password "" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR/altDB" --user dolt --password "" --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "defaultDB does not exist" ]] || false

    stop_sql_server 1

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR/altDB" --user dolt --password "" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    run dolt --verbose-engine-setup --data-dir="$ROOT_DIR/altDB" --user dolt --password "" --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "defaultDB does not exist" ]] || false
}

@test "sql-local-remote: verify dolt blame behavior is identical in switch between server/no server" {
    cd altDB
    dolt sql -q "create table test (pk int primary key)"
    dolt sql -q "insert into test values (1)"
    dolt add test
    dolt commit -m "insert initial value into test"
    dolt sql -q "insert into test values (2), (3)"
    dolt add test
    dolt commit -m "insert more values into test"
    cd ..

    start_sql_server altDB
    run dolt --user dolt --password "" blame test
    [ "$status" -eq 0 ]
    export out="$output"
    stop_sql_server 1

    run dolt blame test
    [ "$status" -eq 0 ]
    [[ "$output" =  $out ]] || false
}

@test "sql-local-remote: verify simple dolt add behavior." {
    start_sql_server altDB
    cd altDB

    run dolt --verbose-engine-setup --user dolt --password "" sql -q "create table testtable (pk int PRIMARY KEY)"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false

    run dolt --verbose-engine-setup --user dolt --password "" add .
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false

    stop_sql_server 1

    staged=$(get_staged_tables)

    [[ ! -z $(echo "$staged" | grep "testtable") ]] || false
}

@test "sql-local-remote: test 'status' and switch between server/no server" {
  start_sql_server defaultDB

  run dolt --user dolt --password "" status
  [ "$status" -eq 0 ] || false
  [[ "$output" =~ "On branch main" ]] || false
  [[ "$output" =~ "Changes to be committed:" ]] || false
  [[ "$output" =~ "  (use \"dolt reset <table>...\" to unstage)" ]] || false
  [[ "$output" =~ "	modified:         table1" ]] || false
  [[ "$output" =~ "Changes not staged for commit:" ]] || false
  [[ "$output" =~ "  (use \"dolt add <table>\" to update what will be committed)" ]] || false
  [[ "$output" =~ "  (use \"dolt checkout <table>\" to discard changes in working directory)" ]] || false
  [[ "$output" =~ "	modified:         table2" ]] || false
  [[ "$output" =~ "Untracked tables:" ]] || false
  [[ "$output" =~ "  (use \"dolt add <table>\" to include in what will be committed)" ]] || false
  [[ "$output" =~ "	new table:        table3" ]] || false
  ! [[ "$output" =~ "   new table:        generated_foo" ]] || false
  remoteOutput=$output

  run dolt --user dolt --password "" status --ignored
  [ "$status" -eq 0 ] || false
  [[ "$output" =~ "On branch main" ]] || false
  [[ "$output" =~ "Changes to be committed:" ]] || false
  [[ "$output" =~ "  (use \"dolt reset <table>...\" to unstage)" ]] || false
  [[ "$output" =~ "	modified:         table1" ]] || false
  [[ "$output" =~ "Changes not staged for commit:" ]] || false
  [[ "$output" =~ "  (use \"dolt add <table>\" to update what will be committed)" ]] || false
  [[ "$output" =~ "  (use \"dolt checkout <table>\" to discard changes in working directory)" ]] || false
  [[ "$output" =~ "	modified:         table2" ]] || false
  [[ "$output" =~ "Untracked tables:" ]] || false
  [[ "$output" =~ "  (use \"dolt add <table>\" to include in what will be committed)" ]] || false
  [[ "$output" =~ "	new table:        table3" ]] || false
  [[ "$output" =~ "Ignored tables:" ]] || false
  [[ "$output" =~ "  (use \"dolt add -f <table>\" to include in what will be committed)" ]] || false
  [[ "$output" =~ "	new table:        generated_foo" ]] || false
  remoteIgnoredOutput=$output

  stop_sql_server 1

  run dolt status
  [ "$status" -eq 0 ] || false
  localOutput=$output

  run dolt --user dolt --password "" status --ignored
  [ "$status" -eq 0 ] || false
  localIgnoredOutput=$output

  [[ "$remoteOutput" == "$localOutput" ]] || false
  [[ "$remoteIgnoredOutput" == "$localIgnoredOutput" ]] || false
}

@test "sql-local-remote: verify dolt commit behavior is identical in switch between server/no server" {
    skip # TODO - Enable after log command is used for results in remote contexts in commit.go

    cd altDB
    dolt sql -q "create table test1 (pk int primary key)"
    dolt sql -q "create table test2 (pk int primary key)"
    dolt add test1
    cd ..

    start_sql_server altDB

    run dolt --verbose-engine-setup commit -m "committing remotely"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "committing remotely" ]] || false

    stop_sql_server 1

    cd altDB
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "committing remotely" ]] || false

    run dolt add test2
    [ "$status" -eq 0 ]
    cd ..

    run dolt --verbose-engine-setup commit -m "committing locally"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false

    cd altDB
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "committing locally" ]] || false
}

@test "sql-local-remote: verify simple dolt branch behavior." {
    start_sql_server altDB
    cd altDB

    run dolt --verbose-engine-setup --user dolt --password "" branch b1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false

    run dolt --verbose-engine-setup --user dolt --password "" branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "b1" ]] || false

    stop_sql_server 1

    run dolt --verbose-engine-setup --user dolt --password "" branch b2
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false

    run dolt --verbose-engine-setup --user dolt --password "" branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "b2" ]] || false
}

@test "sql-local-remote: verify dolt diff behavior with data and schema changes" {
  start_sql_server defaultDB
  cd defaultDB

  dolt sql <<SQL
create table test (pk int primary key, c1 int, c2 int);
insert into test values (1,2,3);
insert into test values (4,5,6);
SQL
  dolt add .
  dolt commit -am "First commit"

  dolt sql <<SQL
alter table test
drop column c2,
add column c3 varchar(10);
insert into test values (7,8,9);
delete from test where pk = 1;
update test set c1 = 100 where pk = 4;
SQL

    EXPECTED=$(cat <<'EOF'
 CREATE TABLE `test` (
   `pk` int NOT NULL,
   `c1` int,
-  `c2` int,
+  `c3` varchar(10),
   PRIMARY KEY (`pk`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
+---+----+-----+------+------+
|   | pk | c1  | c2   | c3   |
+---+----+-----+------+------+
| - | 1  | 2   | 3    | NULL |
| < | 4  | 5   | 6    | NULL |
| > | 4  | 100 | NULL | NULL |
| + | 7  | 8   | NULL | 9    |
+---+----+-----+------+------+
EOF
)

  dolt diff
  run dolt diff
  [ "$status" -eq 0 ] || false
  [[ "$output" =~ "$EXPECTED" ]] || false
  remoteOutput=$output

  stop_sql_server 1

  run dolt diff
  [ "$status" -eq 0 ] || false
  localOutput=$output

  [[ "$remoteOutput" == "$localOutput" ]] || false
}

@test "sql-local-remote: verify dolt show behavior" {
  cd defaultDB

  dolt commit --allow-empty -m "commit: initialize table1"

  run dolt show --no-pretty
  [ "$status" -eq 0 ] || false
  [[ "$output" =~ "SerialMessage" ]] || false
  assert_has_key "Name" "$output"
  assert_has_key_value "Name" "Bats Tests" "$output"
  assert_has_key_value "Desc" "commit: initialize table1" "$output"
  assert_has_key_value "Name" "Bats Tests" "$output"
  assert_has_key_value "Email" "bats@email.fake" "$output"
  assert_has_key "Time" "$output"
  assert_has_key_value "Height" "3" "$output"
  assert_has_key "RootValue" "$output"
  assert_has_key "Parents" "$output"
  assert_has_key "ParentClosure" "$output"

  parentHash=$(extract_value Parents "$output")
  parentClosureHash=$(extract_value ParentClosure "$output")
  rootValue=$(extract_value RootValue "$output")

  run dolt show "$parentHash"
  [ "$status" -eq 0 ] || false
  [[ "$output" =~ "tables table1, table2" ]] || false
  run dolt show "$rootValue"
  [ "$status" -eq 0 ] || false
  run dolt show "$parentClosureHash"
  [ "$status" -eq 0 ] || false

  start_sql_server defaultDB

  run dolt show --no-pretty
  [ $status -eq 1 ] || false
  [[ "$output" =~ "\`dolt show --no-pretty\` or \`dolt show NON_COMMIT_REF\` only supported in local mode." ]] || false

  run dolt show "$parentHash"
  [ $status -eq 0 ] || false
  [[ "$output" =~ "tables table1, table2" ]] || false
  run dolt show "$parentClosureHash"
  [ $status -eq 1 ] || false
  [[ "$output" =~ "\`dolt show --no-pretty\` or \`dolt show NON_COMMIT_REF\` only supported in local mode." ]] || false
  run dolt show "$rootValue"
  [ $status -eq 1 ] || false
  [[ "$output" =~ "\`dolt show --no-pretty\` or \`dolt show NON_COMMIT_REF\` only supported in local mode." ]] || false

  stop_sql_server 1
}

@test "sql-local-remote: check that the --password argument is used when talking to a server and ignored with local" {
    start_sql_server altDb

    dolt --user dolt --password "" sql -q "CREATE USER 'joe'@'%' IDENTIFIED BY 'joe123'; GRANT ALL PRIVILEGES ON defaultDb.* TO 'joe'@'%' WITH GRANT OPTION;";

    run dolt --verbose-engine-setup --user joe --password "badpwd" sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "Access denied for user 'joe'" ]] || false

    run dolt --user joe --password "joe123" sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Access denied for user 'joe'@'%' to database 'altDB'" ]] || false

    run dolt --verbose-engine-setup --user joe --password "joe123" --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "defaultDB_tbl" ]] || false

    # Empty Password should work since we started the server with the 'dolt' user with no pwd.
    run dolt --verbose-engine-setup --user dolt --password "" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    stop_sql_server 1

    run dolt --verbose-engine-setup --user joe --password failnow sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "starting local mode" ]] || false

    # altDB is not accessable to joe
    run dolt --verbose-engine-setup --user joe --password "joe123" sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Access denied for user 'joe'" ]] || false

    run dolt --verbose-engine-setup --user joe --password "joe123" --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "defaultDB_tbl" ]] || false

    # Get access denied for a failed login (bad pwd)
    run dolt --verbose-engine-setup --user joe --password failalways sql -q "SELECT user, host FROM mysql.user"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "Access denied for user 'joe'" ]] || false

    # Get an permission error when attempting to access forbidden info as an authenticated user.
    run dolt --verbose-engine-setup --user joe --password "joe123" sql -q "SELECT user, host FROM mysql.user"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "command denied to user 'joe'@'%'" ]] || false

    # Similar test to above, but will get different results because the dolt user doesn't exist (it was
    # used to start sql-server
    run dolt --user dolt --password "" sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Access denied for user 'dolt'" ]] || false
}

@test "sql-local-remote: check that the DOLT_CLI_PASSWORD argument is used when talking to a server and ignored with local" {
    start_sql_server altDb

    dolt --user dolt --password "" sql -q "CREATE USER 'joe'@'%' IDENTIFIED BY 'joe123'; GRANT ALL PRIVILEGES ON defaultDb.* TO 'joe'@'%' WITH GRANT OPTION;";

    export DOLT_CLI_PASSWORD="badpwd"
    run dolt --verbose-engine-setup --user joe sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "Access denied for user 'joe'" ]] || false

    export DOLT_CLI_PASSWORD="joe123"
    run dolt --user joe sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Access denied for user 'joe'@'%' to database 'altDB'" ]] || false

    export DOLT_CLI_PASSWORD="joe123"
    run dolt --verbose-engine-setup --user joe --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "defaultDB_tbl" ]] || false

    export DOLT_CLI_PASSWORD=""
    run dolt --verbose-engine-setup --user dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false

    stop_sql_server 1

    export DOLT_CLI_PASSWORD="badpwd"
    run dolt --verbose-engine-setup --user joe sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "Access denied for user 'joe'" ]] || false

    export DOLT_CLI_PASSWORD="joe123"
    run dolt --user joe --password "joe123" sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Access denied for user 'joe'" ]] || false

    run dolt --user joe --password "joe123" --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "defaultDB_tbl" ]] || false

    # Get access denied for a failed login (bad pwd)
    export DOLT_CLI_PASSWORD="badpwd"
    run dolt --user joe sql -q "SELECT user, host FROM mysql.user"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Access denied for user 'joe'" ]] || false

    export DOLT_CLI_PASSWORD="joe123"
    # Get an permission error when attempting to access forbidden info as an authenticated user.
    run dolt --user joe sql -q "SELECT user, host FROM mysql.user"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "command denied to user 'joe'@'%'" ]] || false

    export DOLT_CLI_PASSWORD="badpwd"
    run dolt --user rambo --use-db defaultDB sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Access denied for user 'rambo'" ]] || false

    export DOLT_CLI_PASSWORD=""
    run dolt --user dolt sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Access denied for user 'dolt'" ]] || false

    unset DOLT_CLI_PASSWORD
}

@test "sql-local-remote: ensure passing only a password results in an error" {
    export SQL_USER="root"
    start_sql_server altDb

    run dolt --password "anything" sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "When a password is provided, a user must also be provided" ]] || false

    export DOLT_CLI_PASSWORD="anything"
    run dolt sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "When a password is provided, a user must also be provided" ]] || false

    stop_sql_server 1 

    run dolt --password "anything" sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "When a password is provided, a user must also be provided" ]] || false

    export DOLT_CLI_PASSWORD="anything"
    run dolt sql -q "show tables"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "When a password is provided, a user must also be provided" ]] || false
}

@test "sql-local-remote: verify dolt conflicts resolve behavior" {
  skip "This test relies on dolt commit being migrated"

  start_sql_server defaultDB
  cd defaultDB

  basic_conflict
  dolt dolt checkout main
  run dolt dolt sql -q "select * from t"
  [ $status -eq 0 ]
  [[ $output =~ "main" ]] || false

  run dolt dolt merge other
  [ $status -eq 0 ]
  [[ $output =~ "Automatic merge failed" ]] || false

  run dolt dolt conflicts resolve --ours .
  [ $status -eq 0 ]
  remoteOutput=$output
  run dolt dolt sql -q "select * from t"
  [ $status -eq 0 ]
  [[ $output =~ "main" ]] || false

  stop_sql_server 1

  basic_conflict
  dolt dolt checkout main
  run dolt dolt sql -q "select * from t"
  [ $status -eq 0 ]
  [[ $output =~ "main" ]] || false

  run dolt dolt merge other
  [ $status -eq 0 ]
  [[ $output =~ "Automatic merge failed" ]] || false

  run dolt dolt conflicts resolve --ours .
  [ $status -eq 0 ]
  localOutput=$output
  run dolt dolt sql -q "select * from t"
  [ $status -eq 0 ]
  [[ $output =~ "main" ]] || false

  [[ "$remoteOutput" == "$localOutput" ]] || false
}