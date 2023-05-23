#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helpers.bash

setup() {
  setup_dolt_repo
}

teardown() {
  cd ..
    teardown_dolt_repo

    # Check if postgresql is still running. If so stop it
    active=$(service postgresql status)
    if echo "$active" | grep "online"; then
        service postgresql stop
    fi
}

@test "rust mysql.connector client" {
    cd $BATS_TEST_DIRNAME/rust
    cargo run --bin mysql_connector_test $USER $PORT $REPO_NAME
}

