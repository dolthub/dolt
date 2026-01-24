#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    setup_no_dolt_init
    mkdir $BATS_TMPDIR/config-test$$
    nativevar DOLT_ROOT_PATH $BATS_TMPDIR/config-test$$ /p
    cd $BATS_TMPDIR/dolt-repo-$$
}

teardown() {
    teardown_common
    rm -rf "$BATS_TMPDIR/config-test$$"
    stop_sql_server
}

function no_stderr {
    "$@" 2>/dev/null
}

function no_stdout {
    "$@" 1>/dev/null
}

@test "config: make sure no dolt configuration for simulated fresh user" {
    run dolt config --list
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "config: try to initialize a repository with no configuration with correct hint" {
    run dolt init
    [ "$status" -eq 1 ]
    name='dolt config --global --add user.email "you@example.com"'
    email='dolt config --global --add user.name "Your Name"'
    [[ "$output" =~ "Please tell me who you are" ]] || false
    [[ "$output" =~ "$name" ]] || false
    [[ "$output" =~ "$email" ]] || false
}

@test "config: cannot set nonsense variables" {
    run dolt config --add foo bar
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: invalid config option" ]] || false

    run dolt config --set foo bar
    [ "$status" -eq 1 ]
    [[ "$output" =~ "error: invalid config option" ]] || false
}

@test "config: can unset nonsense variables" {
    dolt config --global --add user.name steph  # need to create config_global.json first
    echo '{"foo":"bar"}' > $DOLT_ROOT_PATH/.dolt/config_global.json
    run dolt config --global --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "foo = bar" ]] || false

    run dolt config --global --unset foo
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Config successfully updated" ]] || false
}

@test "config: warning on cli commands if config has nonsense variables" {
    dolt config --global --add user.name steph  # need to create config_global.json first
    echo '{"global":"foo"}' > $DOLT_ROOT_PATH/.dolt/config_global.json
    run no_stdout dolt version
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Warning: Unknown global config option 'global'. Use \`dolt config --global --unset global\` to remove." ]] || false

    # warning prints to stderr
    run no_stderr dolt version
    [ "$status" -eq 0 ]
    ! [[ "$output" =~ "Warning: Unknown global config option 'global'. Use \`dolt config --global --unset global\` to remove." ]] || false

    dolt config --global --add user.email "you@example.com"
    dolt config --global --add user.name "Your Name"
    dolt init

    dolt config --local --add user.name steph  # need to create config.json first
    echo '{"local":"bar"}' > .dolt/config.json
    run no_stdout dolt config --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Warning: Unknown global config option 'global'. Use \`dolt config --global --unset global\` to remove." ]] || false
    [[ "$output" =~ "Warning: Unknown local config option 'local'. Use \`dolt config --local --unset local\` to remove." ]] || false
}

@test "config: set a global config variable" {
    run dolt config --global --add user.name steph
    [ "$status" -eq 0 ]
    # Need to make this a regex because of the coloring
    [[ "$output" =~ "Config successfully updated" ]] || false
    run dolt config --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "user.name = steph" ]] || false
    run dolt config --get user.name
    [ "$status" -eq 0 ]
    [ "$output" = "steph" ]
    run dolt config --global --add user.email
    [ "$status" -eq 1 ]
    [[ "$output" =~ "wrong number of arguments" ]] || false
    run dolt config --global --add
    [ "$status" -eq 1 ]
    [[ "$output" =~ "wrong number of arguments" ]] || false
}

@test "config: delete a config variable" {
    dolt config --global --add user.name steph
    run dolt config --global --unset user.name
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Config successfully updated" ]] || false
    run dolt config --list
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "steph" ]] || false
    run dolt config --get user.name
    [ "$status" -eq 1 ]
    [ "$output" = "" ]
}

@test "config: set and delete multiple config variables" {
    dolt config --global --add user.name steph
    dolt config --global --add user.email steph@dolthub.com
    dolt config --global --add metrics.disabled true
    run dolt config --list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    run dolt config --global --unset user.name user.email metrics.disabled
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Config successfully updated" ]] || false
    run dolt config --list
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "config: set a user and email and init a repo" {
    dolt config --global --add user.name "bats tester"
    run dolt init
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Please tell me who you are" ]] || false
    dolt config --global --add user.email "bats-tester@liquidata.co"
    run dolt init
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully initialized dolt data repository." ]] || false
}

@test "config: set a local config variable with --set" {
    dolt config --global --set user.name "tester"
    dolt config --global --set user.email "tester@liquidata.co"
    dolt init
    run dolt config --local --set metrics.disabled true
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Config successfully updated" ]] || false
    [ -f .dolt/config.json ]
    run dolt config --list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "metrics.disabled = true" ]] || false
    run dolt config --get metrics.disabled
    [ "$status" -eq 0 ]
    [ "$output" = "true" ]
}

@test "config: set a local config variable" {
    dolt config --global --add user.name "bats tester"
    dolt config --global --add user.email "bats-tester@liquidata.co"
    dolt init
    run dolt config --local --add metrics.disabled true
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Config successfully updated" ]] || false
    [ -f .dolt/config.json ]
    run dolt config --list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "metrics.disabled = true" ]] || false
    run dolt config --get metrics.disabled
    [ "$status" -eq 0 ]
    [ "$output" = "true" ]
}

@test "config: override a global config variable with a local config variable" {
    dolt config --global --add user.name "bats tester"
    dolt config --global --add user.email "bats-tester@liquidata.co"
    dolt init
    dolt config --global --add core.editor globalEditor
    dolt config --local --add core.editor localEditor
    run dolt config --local --get core.editor
    [ "$status" -eq 0 ]
    [ "$output" = "localEditor" ]
    # will list both global and local values in list output
    run dolt config --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "core.editor = localEditor" ]] || false
    [[ "$output" =~ "core.editor = globalEditor" ]] || false
    # will get the local value explicitly
    run dolt config --get --local core.editor
    [ "$status" -eq 0 ]
    [[ "$output" =~ "localEditor" ]] || false
    [[ ! "$output" =~ "globalEditor" ]] || false
    # will get the global value explicitly
    run dolt config --get --global core.editor
    [ "$status" -eq 0 ]
    [[ "$output" =~ "globalEditor" ]] || false
    [[ ! "$output" =~ "localEditor" ]] || false
    # will get the local value implicitly
    run dolt config --get core.editor
    [ "$status" -eq 0 ]
    [[ "$output" =~ "localEditor" ]] || false
    [[ ! "$output" =~ "globalEditor" ]] || false
}

@test "config: Commit to repo w/ ---author and without config vars sets" {
    dolt config --global --add user.name "bats tester"
    dolt config --global --add user.email "joshn@doe.com"

    dolt init
    dolt sql -q  "
    CREATE TABLE test (
      pk BIGINT NOT NULL COMMENT 'tag:0',
      c1 BIGINT COMMENT 'tag:1',
      c2 BIGINT COMMENT 'tag:2',
      c3 BIGINT COMMENT 'tag:3',
      c4 BIGINT COMMENT 'tag:4',
      c5 BIGINT COMMENT 'tag:5',
      PRIMARY KEY (pk)
    );"

    dolt config --global --unset user.name
    dolt config --global --unset user.email

    run dolt config --list
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
    [[ $output = "" ]] || false

    dolt add .
    run dolt commit --author="John Doe <john@doe.com>" -m="Commit1"
    [ "$status" -eq 0 ]

    run dolt log
    [ "$status" -eq 0 ]
    regex='John Doe <john@doe.com>'
    [[ "$output" =~ "$regex" ]] || false
}

@test "config: SQL can create databases with no user and email set" {
    dolt sql -q  "
    CREATE DATABASE testdb;
    use testdb;
    CREATE TABLE test (pk int primary key, c1 varchar(1));"

    [ -d "testdb" ]
    cd testdb
    run dolt log
    [ "$status" -eq 0 ]
    regex='Dolt System Account <doltuser@dolthub.com>'
    [[ "$output" =~ "$regex" ]] || false
}

@test "config: sql server can create databases with no user and email set" {
    skiponwindows "This test has dependencies missing on windows installations"
    
    start_sql_server

    dolt sql -q "create database testdb"
    run dolt sql --result-format csv -q "show databases"
    [ $status -eq 0 ]
    [[ "$output" =~ "testdb" ]] || false
    dolt sql -q "create table a(x int)"
    dolt sql -q "insert into a values (1), (2)"

    [ -d "testdb" ]
    dolt --use-db testdb sql -q "select * from dolt_log"
    run dolt --use-db testdb sql -q "select * from dolt_log"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Dolt System Account" ]] || false
    [[ "$output" =~ "doltuser@dolthub.com" ]] || false
}

@test "config: SQL COMMIT uses default values when user.name or user.email is unset." {
    dolt config --global --add user.name "bats tester"
    dolt config --global --add user.email "joshn@doe.com"

    dolt init
    dolt sql -q "CREATE TABLE test (pk int primary key)"
    dolt add .

    dolt config --global --unset user.name
    dolt config --global --unset user.email

    dolt sql -q "CALL DOLT_COMMIT('-a', '-m', 'updated stuff')"

    dolt config --global --add user.name "bats tester"
    dolt sql -q "INSERT INTO test VALUES (1);"
    dolt sql -q "CALL DOLT_COMMIT('-a', '-m', 'updated stuff')"
}

@test "config: Set default init branch" {
    dolt config --global --add user.name "bats tester"
    dolt config --global --add user.email "joshn@doe.com"

    dolt config --global --add init.defaultBranch "master"
    dolt config --list
    run dolt config --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "init.defaultbranch = master" ]] || false

    dolt init
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* master" ]] || false

    # cleanup
    dolt config --global --unset init.defaultBranch
}

@test "config: default init branch is not master" {
    dolt config --global --add user.name "bats tester"
    dolt config --global --add user.email "joshn@doe.com"

    dolt init
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch main" ]] || false
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* main" ]] || false
}

@test "config: init accepts branch flag" {
    dolt config --global --add user.name "bats tester"
    dolt config --global --add user.email "joshn@doe.com"

    dolt init -b=vegan-btw
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch vegan-btw" ]] || false
    run dolt branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "* vegan-btw" ]] || false
}

@test "config: config doesn't need write permission in current dir" {
    chmod 555 .
    dolt config --list
    chmod 755 .
}
