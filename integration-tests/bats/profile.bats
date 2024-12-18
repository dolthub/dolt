#!/usr/bin/env bats
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
SQL
  dolt add -A && dolt commit -m "tables table1, table2"
  cd -
}

setup() {
    setup_no_dolt_init
    make_repo defaultDB
    make_repo altDB

    unset DOLT_CLI_PASSWORD
    unset DOLT_SILENCE_USER_REQ_FOR_TESTING
}

teardown() {
    stop_sql_server 1
    teardown_common
}

@test "profile: --profile exists and isn't empty" {
    cd defaultDB
    dolt sql -q "create table test (pk int primary key)"
    dolt sql -q "insert into test values (999)"
    dolt add test
    dolt commit -m "insert initial value into test"
    cd -

    dolt profile add --use-db defaultDB defaultTest
    run dolt --profile defaultTest sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "999" ]] || false
}

@test "profile: --profile doesn't exist" {
    dolt profile add --use-db defaultDB defaultTest

    run dolt --profile nonExistentProfile sql -q "select * from altDB_tbl"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Failed to inject profile arguments: profile nonExistentProfile not found" ]] || false
}

@test "profile: additional flag gets used" {
    cd altDB
    dolt sql -q "create table test (pk int primary key)"
    dolt sql -q "insert into test values (999)"
    dolt add test
    dolt commit -m "insert initial value into test"
    cd -

    dolt profile add --user dolt --password "" userProfile
    run dolt --profile userProfile --use-db altDB sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "999" ]] || false
}

@test "profile: duplicate flag overrides correctly" {
    cd altDB
    dolt sql -q "create table test (pk int primary key)"
    dolt sql -q "insert into test values (999)"
    dolt add test
    dolt commit -m "insert initial value into test"
    cd -

    dolt profile add --use-db defaultDB defaultTest
    run dolt --profile defaultTest --use-db altDB sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "999" ]] || false
}

@test "profile: duplicate flag with non-duplicate flags in profile overrides correctly" {
    cd altDB
    dolt sql -q "create table test (pk int primary key)"
    dolt sql -q "insert into test values (999)"
    dolt add test
    dolt commit -m "insert initial value into test"
    cd -

    start_sql_server altDb
    dolt --user dolt --password "" sql -q "CREATE USER 'steph' IDENTIFIED BY 'pass'; GRANT ALL PRIVILEGES ON altDB.* TO 'steph' WITH GRANT OPTION;";
    dolt profile add --user "not-steph" --password "pass" --use-db altDB userWithDBProfile

    run dolt --profile userWithDBProfile --user steph sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "999" ]] || false
}

@test "profile: duplicate flag with non-duplicate flags overrides correctly" {
    cd altDB
    dolt sql -q "create table test (pk int primary key)"
    dolt sql -q "insert into test values (999)"
    dolt add test
    dolt commit -m "insert initial value into test"
    cd -

    start_sql_server altDb
    dolt --user dolt --password "" sql -q "CREATE USER 'steph' IDENTIFIED BY 'pass'; GRANT ALL PRIVILEGES ON altDB.* TO 'steph' WITH GRANT OPTION;";
    dolt profile add --user "not-steph" --password "pass" userProfile

    run dolt --profile userProfile --user steph --use-db altDB sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "999" ]] || false
}

@test "profile: dolt profile add adds a profile" {
    run dolt profile add --use-db altDB altTest
    [ "$status" -eq 0 ]

    run dolt profile
    [ "$status" -eq 0 ]
    [[ "$output" =~ "altTest" ]] || false
}

@test "profile: dolt profile add does not overwrite an existing profile" {
    run dolt profile add --user "steph" --password "password123" userProfile
    [ "$status" -eq 0 ]

    run dolt profile -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "userProfile" ]] || false
    [[ "$output" =~ "steph" ]] || false
    [[ "$output" =~ "password123" ]] || false

    run dolt profile add --user "joe" --password "password123" userProfile
    [ "$status" -eq 1 ]
    [[ "$output" =~ "profile userProfile already exists, please delete this profile and re-add it if you want to edit any values" ]] || false

    run dolt profile -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "userProfile" ]] || false
    [[ "$output" =~ "steph" ]] || false
    [[ ! "$output" =~ "joe" ]] || false
    [[ "$output" =~ "password123" ]] || false
}

@test "profile: dolt profile add adds a profile with existing profiles" {
    dolt profile add --use-db altDB altTest
    dolt profile add --use-db defaultDB defaultTest

    run dolt profile
    [ "$status" -eq 0 ]
    [[ "$output" =~ "altTest" ]] || false
    [[ "$output" =~ "defaultTest" ]] || false
}

@test "profile: dolt profile add with multiple names errors" {
    run dolt profile add --use-db altDB altTest altTest2
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Only one profile name can be specified" ]] || false
}

@test "profile: adding default profile prints warning message" {
    run dolt profile add --use-db defaultDB default
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Default profile has been added. All dolt commands taking global arguments will use this default profile until it is removed." ]] || false
    [[ "$output" =~ "WARNING: This will alter the behavior of commands which specify no \`--profile\`." ]] || false
    [[ "$output" =~ "If you are using dolt in contexts where you expect a \`.dolt\` directory to be accessed, the default profile will be used instead." ]] || false
}

@test "profile: dolt profile add encodes profiles in config" {
    run dolt profile add --use-db altDB altTest
    [ "$status" -eq 0 ]

    run cat "$BATS_TMPDIR/config-$$/.dolt/config_global.json"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "altTest" ]] || false
}

@test "profile: dolt profile add locks global config with 0600" {
    run dolt profile add --use-db altDB altTest
    [ "$status" -eq 0 ]

    run stat "$BATS_TMPDIR/config-$$/.dolt/config_global.json"
    [[ "$output" =~ "-rw-------" ]] || false
}

@test "profile: dolt profile remove removes a profile" {
    dolt profile add --use-db altDB altTest
    run dolt profile
    [ "$status" -eq 0 ]
    [[ "$output" =~ "altTest" ]] || false

    run dolt profile remove altTest
    [ "$status" -eq 0 ]

    run dolt profile
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "altTest:" ]] || false
}

@test "profile: dolt profile remove leaves existing profiles" {
    dolt profile add --use-db altDB altTest
    dolt profile add --use-db defaultDB defaultTest
    run dolt profile
    [ "$status" -eq 0 ]
    [[ "$output" =~ "altTest" ]] || false
    [[ "$output" =~ "defaultTest" ]] || false

    run dolt profile remove altTest
    [ "$status" -eq 0 ]

    run dolt profile
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "altTest:" ]] || false
    [[ "$output" =~ "defaultTest" ]] || false
}

@test "profile: dolt profile remove last profile also removes profile param from global config" {
    dolt profile add --use-db altDB altTest
    run dolt profile
    [ "$status" -eq 0 ]
    [[ "$output" =~ "altTest" ]] || false

    run dolt profile remove altTest
    [ "$status" -eq 0 ]

    run dolt profile
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "altTest" ]] || false

    run dolt config --list --global
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "profile" ]] || false
}

@test "profile: dolt profile remove with no existing profiles errors" {
    run dolt profile
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false

    run dolt profile remove altTest
    [ "$status" -eq 1 ]
    [[ "$output" =~ "no existing profiles" ]] || false
}

@test "profile: dolt profile remove with non-existent profile errors" {
    dolt profile add --use-db altDB altTest
    run dolt profile
    [ "$status" -eq 0 ]
    [[ "$output" =~ "altTest" ]] || false

    run dolt profile remove defaultTest
    [ "$status" -eq 1 ]
    [[ "$output" =~ "profile defaultTest does not exist" ]] || false
}

@test "profile: dolt profile remove with multiple names errors" {
    dolt profile add --use-db altDB altTest
    run dolt profile remove altTest altTest2
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Only one profile name can be specified" ]] || false
}

@test "profile: dolt profile remove locks global config with 0600" {
    dolt profile add --use-db altDB altTest
    run dolt profile remove altTest
    [ "$status" -eq 0 ]

    run stat "$BATS_TMPDIR/config-$$/.dolt/config_global.json"
    [[ "$output" =~ "-rw-------" ]] || false
}

@test "profile: dolt profile lists all profiles" {
    dolt profile add --use-db altDB altTest
    dolt profile add --use-db defaultDB -u "steph" --password "pass" defaultTest

    run dolt profile
    [ "$status" -eq 0 ]
    [[ "$output" =~ "altTest" ]] || false
    [[ ! "$output" =~ "use-db: altDB" ]] || false
    [[ "$output" =~ "defaultTest" ]] || false
    [[ ! "$output" =~ "user: steph" ]] || false
    [[ ! "$output" =~ "password: pass" ]] || false
    [[ ! "$output" =~ "use-db: defaultDB" ]] || false
}

@test "profile: dolt profile --verbose lists all profiles and all details" {
    dolt profile add --use-db altDB altTest
    dolt profile add --use-db defaultDB -u "steph" --password "pass" defaultTest

    run dolt profile -v
    [ "$status" -eq 0 ]
    [[ "$output" =~ "altTest" ]] || false
    [[ "$output" =~ "use-db: altDB" ]] || false
    [[ "$output" =~ "defaultTest" ]] || false
    [[ "$output" =~ "user: steph" ]] || false
    [[ "$output" =~ "password: pass" ]] || false
    [[ "$output" =~ "use-db: defaultDB" ]] || false
}

@test "profile: default profile used when none specified" {
    cd defaultDB
    dolt sql -q "create table test (pk int primary key)"
    dolt sql -q "insert into test values (999)"
    dolt add test
    dolt commit -m "insert initial value into test"
    cd -

    dolt profile add --use-db defaultDB default
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "999" ]] || false
}

@test "profile: no profile used when none specified and no default set" {
    cd defaultDB
    dolt sql -q "insert into table1 values (999)"
    dolt add table1
    dolt commit -m "insert initial value into table1"
    cd -

    dolt profile add --use-db defaultDB defaultTest
    run dolt sql -q "select * from table1"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "999" ]] || false
}

@test "profile: correct default profile used when none specified" {
    cd defaultDB
    dolt sql -q "create table test (pk int primary key)"
    dolt sql -q "insert into test values (999)"
    dolt add test
    dolt commit -m "insert initial value into test"
    cd -

    dolt profile add --use-db defaultDB default
    dolt profile add --use-db altDB altTest
    run dolt sql -q "select * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "999" ]] || false
}

@test "profile: commands that don't support global args work with a default profile set" {
    cd altDB
    dolt profile add --use-db defaultDB default

    run dolt config --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "user.email = bats@email.fake" ]] || false
    [[ "$output" =~ "user.name = Bats Tests" ]] || false
}

@test "profile: profile with user but not password waits for password prompt" {
    dolt profile add --use-db defaultDB -u "steph" defaultTest
    run dolt --profile defaultTest sql -q "select * from table1" <<< ""
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Enter password:" ]] || false
}

@test "profile: profile with user and empty password doesn't wait for password prompt" {
    dolt profile add --use-db defaultDB -u "steph" -p "" defaultTest
    run dolt --profile defaultTest sql -q "show tables"
    [ "$status" -eq 0 ]
}

@test "profile: multiple profiles work" {
    cd defaultDB
    dolt sql -q "create table defaultTable (pk int primary key)"
    dolt commit -Am "create defaultTable"
    cd -

    cd altDB
    dolt sql -q "create table altTable (pk int primary key)"
    dolt commit -Am "create altTable"
    cd -

    dolt profile add --use-db defaultDB defaultTest
    dolt profile add --use-db altDB altTest

    run dolt --profile defaultTest sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "defaultTable" ]] || false
    [[ ! "$output" =~ "altTable" ]] || false

    run dolt --profile altTest sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "defaultTable" ]] || false
    [[ "$output" =~ "altTable" ]] || false
}

@test "profile: profile doesn't need write permission in current dir" {
    chmod 111 .
    dolt profile
    chmod 755 .
}
