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
  # Note that we leave the table in a dirty state, which is useful to several tests, and harmless to others. For
  # some, you need to ensure the repo is clean, and you should run `dolt reset --hard` at the beginning of the test.
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
    dolt sql -q "create table t (i int primary key, t text)"
    dolt add .
    dolt commit -am "init commit"
    dolt checkout -b other
    dolt sql -q "insert into t values (1,'other')"
    dolt commit -am "other commit"
    dolt checkout main
    dolt sql -q "insert into t values (1,'main')"
    dolt commit -am "main commit"
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

get_commit_hash_at() {
    local ref="$1"
    dolt log "$ref" --oneline | head -n 1 | cut -d ' ' -f 1 | sed 's/\x1b\[[0-9;]*m//g' | tr -d ' '
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

@test "sql-local-remote: verify simple dolt checkout behavior." {
    skip # currently checkout with a server is not supported
    start_sql_server altDB
    cd altDB

    run dolt --verbose-engine-setup --user dolt --password "" checkout -b other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false

    run dolt --verbose-engine-setup --user dolt --password "" branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "other" ]] || false

    # Due to a current limitation, subsequent commands won't use the new branch until the server is stopped
    # See https://github.com/dolthub/dolt/issues/6315 for more information.
    stop_sql_server 1

    run dolt branch --show-current
    [ "$status" -eq 0 ]
    [[ "$output" =~ "other" ]] || false

    start_sql_server altDB

    run dolt --verbose-engine-setup --user dolt --password "" checkout main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false

    stop_sql_server 1

    run dolt branch --show-current
    [ "$status" -eq 0 ]
    [[ "$output" =~ "main" ]] || false
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

@test "sql-local-remote: verify dolt commit print" {
    run dolt --use-db altDB commit -A -m "Wonderful Commit"
    [[ "${lines[0]}" =~ "commit " ]] || false
    [[ "${lines[1]}" =~ "Author: Bats Tests <bats@email.fake>" ]] || false
    [[ "${lines[2]}" =~ "Date: " ]] || false
    [[ "${lines[3]}" =~ "	Wonderful Commit" ]] || false
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
  assert_has_key "Timestamp" "$output"
  assert_has_key "UserTimestamp" "$output"
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
  [[ "$output" =~ '`dolt show --no-pretty` or `dolt show (BRANCHNAME)` only supported in local mode.' ]] || false

  run dolt show "$parentHash"
  [ $status -eq 0 ] || false
  [[ "$output" =~ "tables table1, table2" ]] || false
  run dolt show "$parentClosureHash"
  [ $status -eq 1 ] || false
  [[ "$output" =~ '`dolt show (NON_COMMIT_HASH)` only supported in local mode.' ]] || false
  run dolt show "$rootValue"
  [ $status -eq 1 ] || false
  [[ "$output" =~ '`dolt show (NON_COMMIT_HASH)` only supported in local mode.' ]] || false

  stop_sql_server 1
}

@test "sql-local-remote: verify dolt conflicts cat behavior" {
  cd defaultDB

  dolt sql << SQL
CREATE TABLE people (
  id INT NOT NULL,
  last_name VARCHAR(120),
  first_name VARCHAR(120),
  birthday DATETIME(6),
  age INT DEFAULT '0',
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
SQL
  dolt add .
  dolt commit -am "base"

  dolt checkout -b right
  dolt sql <<SQL
ALTER TABLE people
MODIFY COLUMN age FLOAT;
SQL
  dolt commit -am "right"

  dolt checkout main
  dolt sql <<SQL
ALTER TABLE people
MODIFY COLUMN age BIGINT;
SQL
  dolt commit -am "left"

  run dolt merge right -m "merge right"
  [ "$status" -eq 1 ]
  [[ "$output" =~ "CONFLICT (schema)" ]] || false

  run dolt conflicts cat .
  [ "$status" -eq 0 ]
  [[ "$output" =~ "| our_schema" ]] || false
  [[ "$output" =~ "| their_schema" ]] || false
  [[ "$output" =~ "| base_schema" ]] || false
  [[ "$output" =~ "| description" ]] || false
  [[ "$output" =~ "different column definitions for our column age and their column age" ]] || false
  [[ "$output" =~ "\`age\` bigint," ]] || false
  [[ "$output" =~ "\`age\` float," ]] || false
  [[ "$output" =~ "\`age\` int DEFAULT '0'," ]] || false
  localOutput=$output

  start_sql_server defaultDB

  run dolt conflicts cat .
  [ "$status" -eq 0 ]
  remoteOutput=$output

  [[ "$remoteOutput" == "$localOutput" ]] || false
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

@test "sql-local-remote: verify simple dolt reset behavior" {
    start_sql_server altDB
    dolt sql -q "create table test1 (pk int primary key)"
    dolt add test1
    dolt commit -m "create table test1"

    dolt sql -q "insert into test1 values (1)"
    dolt add test1
    run dolt --verbose-engine-setup reset
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false

    stop_sql_server 1

    dolt add test1
    run dolt --verbose-engine-setup reset
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*test) ]] || false
}

@test "sql-local-remote: verify dolt conflicts resolve behavior" {
  cd altDB
  dolt tag v0

  # setup
  basic_conflict
  dolt checkout main
  run dolt sql -q "select * from t"
  [ $status -eq 0 ]
  [[ $output =~ "main" ]] || false
  run dolt merge other
  [ $status -eq 1 ]
  [[ $output =~ "Automatic merge failed" ]] || false

  # start server
  start_sql_server altDB

  # test remote
  run dolt conflicts resolve --ours .
  [ $status -eq 0 ]
  remoteOutput=$output
  run dolt sql -q "select * from t"
  [ $status -eq 0 ]
  [[ $output =~ "main" ]] || false

  # stop server
  stop_sql_server 1

  # reset
  dolt reset --hard v0
  dolt branch -D other

  # test local
  basic_conflict
  dolt checkout main
  run dolt sql -q "select * from t"
  [ $status -eq 0 ]
  [[ $output =~ "main" ]] || false

  run dolt merge other
  [ $status -eq 1 ]
  [[ $output =~ "Automatic merge failed" ]] || false

  run dolt conflicts resolve --ours .
  [ $status -eq 0 ]
  localOutput=$output
  run dolt sql -q "select * from t"
  [ $status -eq 0 ]
  [[ $output =~ "main" ]] || false

  [[ "$remoteOutput" == "$localOutput" ]] || false
}

@test "sql-local-remote: ensure revert produces similar output for each mode" {
    dolt --use-db altDB commit -A -m "Commit ABCDEF"

    start_sql_server altDb

    run dolt --use-db altDB revert HEAD
    [ $status -eq 0 ]
    [[ "$output" =~ 'Revert "Commit ABCDEF"' ]] || false

    dolt reset --hard HEAD~1

    stop_sql_server 1

    run dolt revert HEAD
    [ $status -eq 0 ]
    [[ $output =~ 'Revert "Commit ABCDEF"' ]] || false
}

@test "sql-local-remote: Ensure that dolt clean works for each mode" {
    dolt reset --hard
    dolt sql -q "create table tbl (pk int primary key)"

    start_sql_server altDB

    run dolt --verbose-engine-setup clean --dry-run
    [ $status -eq 0 ]
    [[ $output =~ "starting remote mode" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ $output =~ "Untracked tables" ]] || false

    dolt clean

    run dolt status
    [ $status -eq 0 ]
    [[ $output =~ "nothing to commit, working tree clean" ]] || false

    stop_sql_server 1

    dolt sql -q "create table tbl (pk int primary key)"
    run dolt --verbose-engine-setup clean --dry-run
    [ $status -eq 0 ]
    [[ $output =~ "starting local mode" ]] || false

    run dolt status
    [ $status -eq 0 ]
    [[ $output =~ "Untracked tables" ]] || false

    dolt clean

    run dolt status
    [ $status -eq 0 ]
    [[ $output =~ "nothing to commit, working tree clean" ]] || false
}

@test "sql-local-remote: verify dolt tag behavior" {
  cd altDB

  # get commit hashes
  headCommit=$(get_commit_hash_at HEAD)
  secondCommit=$(get_commit_hash_at HEAD~1)

  # show tags
  run dolt --verbose-engine-setup tag
  [ $status -eq 0 ]
  [[ $output =~ "verbose: starting local mode" ]] || false

  # add tag without message
  run dolt --verbose-engine-setup tag v1_tag
  [ $status -eq 0 ]
  [[ $output =~ "verbose: starting local mode" ]] || false

  # list tags and check new tag is present
  run dolt tag
  [ $status -eq 0 ]
  [[ $output =~ "v1_tag" ]] || false

  # list tags with verbose flag and check new tag is present
  run dolt --verbose-engine-setup tag --verbose
  [ $status -eq 0 ]
  [[ $output =~ "v1_tag"$'\t'"$headCommit" ]] || false
  [[ $output =~ "Tagger: Bats Tests <bats@email.fake>" ]] || false
  [[ $output =~ "verbose: starting local mode" ]] || false

  # add tag with commit
  run dolt tag v2_tag $secondCommit
  [ $status -eq 0 ]

  # list tags and check new tag is present
  run dolt tag --verbose
  [ $status -eq 0 ]
  [[ $output =~ "v1_tag"$'\t'"$headCommit" ]] || false
  [[ $output =~ "v2_tag"$'\t'"$secondCommit" ]] || false

  # add tag with message
  run dolt tag v3_tag -m "tag message"
  [ $status -eq 0 ]

  # list tags and check new tag is present
  run dolt tag --verbose
  [ $status -eq 0 ]
  [[ $output =~ "v3_tag"$'\t'"$headCommit" ]] || false
  [[ $output =~ "tag message" ]] || false

  # add tag with message and commit
  run dolt tag v4_tag $secondCommit -m "second message"
  [ $status -eq 0 ]

  # list tags and check new tag is present
  run dolt tag --verbose
  [ $status -eq 0 ]
  [[ $output =~ "v4_tag"$'\t'"$secondCommit" ]] || false
  [[ $output =~ "second message" ]] || false

  # add tag with author
  run dolt tag v5_tag --author "John Doe <john@doe.com>"
  [ $status -eq 0 ]

  # list tags and check new tag is present
  run dolt tag --verbose
  [ $status -eq 0 ]
  [[ $output =~ "v5_tag"$'\t'"$headCommit" ]] || false
  [[ $output =~ "Tagger: John Doe <john@doe.com>" ]] || false

  # delete tag
  run dolt tag -d v2_tag
  [ $status -eq 0 ]

  # list tags and check deleted tag is not present
  run dolt tag --verbose
  [ $status -eq 0 ]
  [[ $output =~ "v1_tag"$'\t'"$headCommit" ]] || false
  [[ ! $output =~ "v2_tag" ]] || false
  [[ $output =~ "v3_tag"$'\t'"$headCommit" ]] || false
  [[ $output =~ "v4_tag"$'\t'"$secondCommit" ]] || false
  [[ $output =~ "tag message" ]] || false
  [[ $output =~ "second message" ]] || false
  [[ $output =~ "Tagger: John Doe <john@doe.com>" ]] || false

  cd ../defaultDB
  start_sql_server defaultDB

  # get commit hashes
  headCommit=$(get_commit_hash_at HEAD)
  secondCommit=$(get_commit_hash_at HEAD~1)

  # show tags
  run dolt --verbose-engine-setup tag
  [ $status -eq 0 ]
  [[ $output =~ "verbose: starting remote mode" ]] || false

  # add tag without message
  run dolt --verbose-engine-setup tag v1_tag
  [ $status -eq 0 ]
  [[ $output =~ "verbose: starting remote mode" ]] || false

  # list tags and check new tag is present
  run dolt tag
  [ $status -eq 0 ]
  [[ $output =~ "v1_tag" ]] || false

  # list tags with verbose flag and check new tag is present
  run dolt --verbose-engine-setup tag --verbose
  [ $status -eq 0 ]
  [[ $output =~ "v1_tag"$'\t'"$headCommit" ]] || false
  [[ $output =~ "Tagger: Bats Tests <bats@email.fake>" ]] || false
  [[ $output =~ "verbose: starting remote mode" ]] || false

  # add tag with commit
  run dolt tag v2_tag $secondCommit
  [ $status -eq 0 ]

  # list tags and check new tag is present
  run dolt tag --verbose
  [ $status -eq 0 ]
  [[ $output =~ "v1_tag"$'\t'"$headCommit" ]] || false
  [[ $output =~ "v2_tag"$'\t'"$secondCommit" ]] || false

  # add tag with message
  run dolt tag v3_tag -m "tag message"
  [ $status -eq 0 ]

  # list tags and check new tag is present
  run dolt tag --verbose
  [ $status -eq 0 ]
  [[ $output =~ "v3_tag"$'\t'"$headCommit" ]] || false
  [[ $output =~ "tag message" ]] || false

  # add tag with message and commit
  run dolt tag v4_tag $secondCommit -m "second message"
  [ $status -eq 0 ]

  # list tags and check new tag is present
  run dolt tag --verbose
  [ $status -eq 0 ]
  [[ $output =~ "v4_tag"$'\t'"$secondCommit" ]] || false
  [[ $output =~ "second message" ]] || false

  # add tag with author
  run dolt tag v5_tag --author "John Doe <john@doe.com>"
  [ $status -eq 0 ]

  # list tags and check new tag is present
  run dolt tag --verbose
  [ $status -eq 0 ]
  [[ $output =~ "v5_tag"$'\t'"$headCommit" ]] || false
  [[ $output =~ "Tagger: John Doe <john@doe.com>" ]] || false

  # delete tag
  run dolt tag -d v2_tag
  [ $status -eq 0 ]

  # list tags and check deleted tag is not present
  run dolt tag --verbose
  [ $status -eq 0 ]
  [[ $output =~ "v1_tag"$'\t'"$headCommit" ]] || false
  [[ ! $output =~ "v2_tag" ]] || false
  [[ $output =~ "v3_tag"$'\t'"$headCommit" ]] || false
  [[ $output =~ "v4_tag"$'\t'"$secondCommit" ]] || false
  [[ $output =~ "tag message" ]] || false
  [[ $output =~ "second message" ]] || false
  [[ $output =~ "Tagger: John Doe <john@doe.com>" ]] || false
}

@test "sql-local-remote: verify dolt cherry-pick behavior" {
  cd altDB

  # setup for cherry-pick.bats
  dolt clean
  dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v varchar(10), index(v))"
  dolt add .
  dolt commit -am "Created table"
  dolt checkout -b branch1
  dolt sql -q "INSERT INTO test VALUES (1, 'a')"
  dolt commit -am "Inserted 1"
  dolt sql -q "INSERT INTO test VALUES (2, 'b')"
  dolt commit -am "Inserted 2"
  dolt sql -q "INSERT INTO test VALUES (3, 'c')"
  dolt commit -am "Inserted 3"
  run dolt sql -q "SELECT * FROM test" -r csv
  [[ "$output" =~ "1,a" ]] || false
  [[ "$output" =~ "2,b" ]] || false
  [[ "$output" =~ "3,c" ]] || false

  # setup for "cherry-pick: schema change, with data conflict" test
  dolt checkout main
  dolt sql -q "CREATE TABLE other (pk int primary key, c1 int, c2 int)"
  dolt sql -q "INSERT INTO other VALUES (1, 2, 3)"
  dolt commit -Am "add other table (on main)"
  # Create two commits on branch2: one to assert does NOT get included, and one to cherry pick
  dolt checkout -b branch2
  dolt sql -q "INSERT INTO other VALUES (100, 200, 300);"
  dolt commit -am "add row 100 to other (on branch2)"
  # This ALTER TABLE statement modifies other rows that aren't included in the cherry-picked
  # commit – row (100, 200, 300) is modified to (100, 400). This shows up as a conflict
  # in the cherry-pick (modified row on one side, row doesn't exist on the other side).
  dolt sql -q "ALTER TABLE other DROP COLUMN c1;"
  dolt sql -q "UPDATE other SET c2 = 400 WHERE pk = 100"
  dolt sql -q "INSERT INTO other VALUES (10, 30);"
  dolt sql -q "INSERT INTO test VALUES (100, 'q');"
  dolt commit -am "alter table, add row 10 to other, add row 100 to test (on branch2)"

  # actual cherry-pick test
  dolt checkout main
  run dolt cherry-pick branch2
  [ $status -eq 1 ]
  [[ $output =~ "Unable to apply commit cleanly due to conflicts or constraint violations" ]] || false
  localCherryPickOutput=$output

  # Assert that table 'test' is staged, but table 'other' is not staged, since it had conflicts
  run dolt sql -q "SELECT table_name, case when staged = 0 then 'staged' else 'working' end as location, status from dolt_status;"
  [ $status -eq 0 ]
  [[ $output =~ "| test       | working  | modified |" ]] || false
  [[ $output =~ "| other      | staged   | modified |" ]] || false

  # setup for remote test
  dolt checkout main
  dolt reset --hard main

  # start server
  start_sql_server altDB

  run dolt cherry-pick branch2
  [ $status -eq 1 ]
  [[ $output =~ "Unable to apply commit cleanly due to conflicts or constraint violations" ]] || false
  remoteCherryPickOutput=$output
  # Assert that table 'test' is staged, but table 'other' is not staged, since it had conflicts
  run dolt sql -q "SELECT table_name, case when staged = 0 then 'staged' else 'working' end as location, status from dolt_status;"
  [ $status -eq 0 ]
  [[ $output =~ "| test       | working  | modified |" ]] || false
  [[ $output =~ "| other      | staged   | modified |" ]] || false

  [[ "$localCherryPickOutput" == "$remoteCherryPickOutput" ]] || false
}

@test "sql-local-remote: verify checkout will fail early when a server is running" {
  cd altDB
  dolt reset --hard # Ensure database is clean to start.
  start_sql_server altDB

  dolt branch br

  run dolt checkout br
  [ $status -eq 1 ]

  [[ $output =~ "dolt checkout can not currently be used when there is a local server running. Please stop your dolt sql-server and try again." ]] || false
}

@test "sql-local-remote: verify unmigrated command will fail with warning" {
    cd altDB
    start_sql_server altDB
    run dolt --user dolt profile
    [ $status -eq 1 ]
    [[ "$output" =~ "Global arguments are not supported for this command" ]] || false
}

@test "sql-local-remote: verify commands without global arg support will fail with warning" {
    cd altDB
    start_sql_server altDB
    run dolt --user dolt version
    [ $status -eq 1 ]
    [[ "$output" =~ "This command does not support global arguments." ]] || false
}

@test "sql-local-remote: verify dolt log behavior" {
    cd altDB

    run dolt --verbose-engine-setup log
    [ $status -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "tables table1, table2" ]] || false

    run dolt log
    [ $status -eq 0 ]
    localOutput=$output

    start_sql_server altDB
    run dolt --verbose-engine-setup log
    [ $status -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "tables table1, table2" ]] || false

    run dolt log
    [ $status -eq 0 ]
    remoteOutput=$output

    [[ "$localOutput" == "$remoteOutput" ]] || false
}

@test "sql-local-remote: verify dolt fetch behavior" {
    mkdir remote
    cd altDB
    dolt remote add origin file://../remote
    dolt commit --allow-empty -m "cm1"
    dolt push origin main

    cd ../defaultDB
    dolt remote add origin file://../remote

    dolt fetch
    run dolt log origin/main
    [ $status -eq 0 ]
    [[ "$output" =~ "cm1" ]] || false

    cd ../altDB
    dolt commit --allow-empty -m "cm2"
    dolt push origin main
    cd ../defaultDB

    start_sql_server defaultDB
    dolt fetch
    run dolt log origin/main
    [ $status -eq 0 ]
    [[ "$output" =~ "cm2" ]] || false
}

@test "sql-local-remote: verify dolt push behavior" {
    mkdir remote
    cd altDB
    dolt remote add origin file://../remote
    dolt commit --allow-empty -m "cm1"
    dolt push origin main

    cd ..
    dolt clone file://./remote repo
    cd repo
    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "cm1" ]] || false

    cd ../altDB
    start_sql_server altDB
    dolt commit --allow-empty -m "cm2"
    dolt push origin main
    cd ../repo

    dolt pull
    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "cm2" ]] || false
}

@test "sql-local-remote: verify dolt pull behavior" {
    mkdir remote
    cd altDB
    dolt remote add origin file://../remote
    dolt commit --allow-empty -m "cm1"
    dolt push origin main

    cd ..
    dolt clone file://./remote repo
    cd altDB
    dolt commit --allow-empty -m "cm2"
    dolt push origin main

    cd ../repo
    dolt pull
    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "cm2" ]] || false

    cd ../altDB
    dolt commit --allow-empty -m "cm3"
    dolt push origin main
    cd ../repo

    start_sql_server repo
    dolt pull origin main
    run dolt log
    [ $status -eq 0 ]
    [[ "$output" =~ "cm3" ]] || false
}

@test "sql-local-remote: verify dolt ls behavior" {
    cd altDB

    run dolt --verbose-engine-setup ls
    [ $status -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false
    [[ "$output" =~ "generated_foo" ]] || false
    [[ "$output" =~ "table1" ]] || false
    [[ "$output" =~ "table2" ]] || false
    [[ "$output" =~ "table3" ]] || false

    run dolt ls
    [ $status -eq 0 ]
    localOutput=$output

    start_sql_server altDB
    run dolt --verbose-engine-setup ls
    [ $status -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "altDB_tbl" ]] || false
    [[ "$output" =~ "generated_foo" ]] || false
    [[ "$output" =~ "table1" ]] || false
    [[ "$output" =~ "table2" ]] || false
    [[ "$output" =~ "table3" ]] || false

    run dolt ls
    [ $status -eq 0 ]
    remoteOutput=$output

    [[ "$localOutput" == "$remoteOutput" ]] || false
}

@test "sql-local-remote: verify dolt merge-base behavior" {
    cd altDB
    dolt checkout -b feature
    dolt sql -q "create table table4 (pk int PRIMARY KEY)"
    dolt add .
    dolt commit -m "created table3"

    run dolt --verbose-engine-setup merge-base main feature
    [ $status -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    localOutput="${lines[1]}"

    start_sql_server altDB
    run dolt --verbose-engine-setup merge-base main feature
    [ $status -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    remoteOutput="${lines[1]}"

    [[ "$localOutput" == "$remoteOutput" ]] || false
}

@test "sql-local-remote: verify dolt reflog behavior" {
    cd altDB
    dolt sql -q "create table t (i int primary key, j int);"
    dolt sql -q "insert into t values (1, 1), (2, 2), (3, 3)";
    dolt commit -Am "initial commit"

    run dolt --verbose-engine-setup reflog
    [ $status -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "initial commit" ]] || false
    run dolt reflog
    localOutput=$output

    start_sql_server altDB
    run dolt --verbose-engine-setup reflog
    [ $status -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "initial commit" ]] || false
    run dolt reflog
    remoteOutput=$output

    [[ "$localOutput" == "$remoteOutput" ]] || false
}

@test "sql-local-remote: verify dolt gc behavior" {
    cd altDB
    dolt sql <<SQL
CREATE TABLE test (pk int PRIMARY KEY);
INSERT INTO test VALUES
    (1),(2),(3),(4),(5);
SQL
    run dolt sql -q 'select count(*) from test' -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "5" ]] || false

    run dolt --verbose-engine-setup gc
    [ $status -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false

    run dolt sql -q 'select count(*) from test' -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "5" ]] || false

    start_sql_server altDB
    dolt sql <<SQL
CREATE TABLE test2 (pk int PRIMARY KEY);
INSERT INTO test2 VALUES
    (1),(2),(3),(4),(5);
SQL
    run dolt sql -q 'select count(*) from test' -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "5" ]] || false
    run dolt sql -q 'select count(*) from test2' -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "5" ]] || false

    run dolt --verbose-engine-setup gc
    [ $status -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false

    run dolt sql -q 'select count(*) from test' -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "5" ]] || false
    run dolt sql -q 'select count(*) from test2' -r csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "5" ]] || false
}

@test "sql-local-remote: verify dolt rebase behavior" {
    cd altDB

    dolt sql -q "drop table dolt_ignore;"
    dolt add .

    dolt branch b1
    dolt commit -m "main commit 2"
    dolt checkout b1
    dolt sql -q "create table t2 (pk int primary key)"
    dolt add .
    dolt commit -m "b1 commit 1"

    touch rebaseScript.sh
    echo "#!/bin/bash" >> rebaseScript.sh
    chmod +x rebaseScript.sh
    export EDITOR=$PWD/rebaseScript.sh
    export DOLT_TEST_FORCE_OPEN_EDITOR="1"

    run dolt --verbose-engine-setup rebase -i main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting local mode" ]] || false
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b1" ]] || false

    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "main commit 2" ]] || false
    [[ "$output" =~ "b1 commit 1" ]] || false

    dolt checkout main
    dolt sql -q "create table t3 (pk int primary key)"
    dolt add .
    dolt commit -m "main commit 3"
    dolt checkout b1

    start_sql_server altDB
    run dolt --verbose-engine-setup rebase -i main
    [ "$status" -eq 0 ]
    [[ "$output" =~ "starting remote mode" ]] || false
    [[ "$output" =~ "Successfully rebased and updated refs/heads/b1" ]] || false

    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "main commit 3" ]] || false
    [[ "$output" =~ "main commit 2" ]] || false
    [[ "$output" =~ "b1 commit 1" ]] || false
}
