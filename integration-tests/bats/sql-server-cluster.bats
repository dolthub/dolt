#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir "$1"
  cd "$1"
  dolt init
  cd ..
}

setup() {
    skiponwindows "tests are flaky on Windows"
    setup_no_dolt_init
    make_repo repo1
    make_repo repo2
}

teardown() {
    stop_sql_server "1"
    teardown_common
}

@test "sql-server-cluster: persisted role and epoch take precedence over bootstrap values" {
    echo "
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: 3309
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:50052/{database}
  bootstrap_role: standby
  bootstrap_epoch: 10
  remotesapi:
    port: 50051" > server.yaml

    (cd repo1 && dolt remote add standby http://localhost:50052/repo1)
    (cd repo2 && dolt remote add standby http://localhost:50052/repo2)
    dolt sql-server --config server.yaml &
    SERVER_PID=$!
    wait_for_connection 3309 5000

    server_query_with_port 3309 repo1 1 dolt "" "select @@GLOBAL.dolt_cluster_role, @@GLOBAL.dolt_cluster_role_epoch" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nstandby,10"

    kill $SERVER_PID
    wait $SERVER_PID
    SERVER_PID=

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: 3309
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:50052/{database}
  bootstrap_role: primary
  bootstrap_epoch: 0
  remotesapi:
    port: 50051" > server.yaml

    dolt sql-server --config server.yaml &
    SERVER_PID=$!
    wait_for_connection 3309 5000

    server_query_with_port 3309 repo1 1 dolt "" "select @@GLOBAL.dolt_cluster_role, @@GLOBAL.dolt_cluster_role_epoch" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nstandby,10"
}

@test "sql-server-cluster: dolt_assume_cluster_role" {
    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: 3309
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:50052/{database}
  bootstrap_role: standby
  bootstrap_epoch: 10
  remotesapi:
    port: 50051" > server.yaml

    (cd repo1 && dolt remote add standby http://localhost:50052/repo1)
    (cd repo2 && dolt remote add standby http://localhost:50052/repo2)
    dolt sql-server --config server.yaml &
    SERVER_PID=$!
    wait_for_connection 3309 5000

    # stale epoch
    run server_query_with_port 3309 repo1 1 dolt "" "call dolt_assume_cluster_role('standby', '9');" "" 1
    [[ "$output" =~ "error assuming role" ]] || false
    # wrong role at current epoch
    run server_query_with_port 3309 repo1 1 dolt "" "call dolt_assume_cluster_role('primary', '10');" "" 1
    [[ "$output" =~ "error assuming role" ]] || false
    # wrong role name
    run server_query_with_port 3309 repo1 1 dolt "" "call dolt_assume_cluster_role('backup', '11');" "" 1
    [[ "$output" =~ "error assuming role" ]] || false

    # successes

    # same role, same epoch
    server_query_with_port 3309 repo1 1 dolt "" "call dolt_assume_cluster_role('standby', '10');" "status\n0"
    # same role, new epoch
    server_query_with_port 3309 repo1 1 dolt "" "call dolt_assume_cluster_role('standby', '12'); select @@GLOBAL.dolt_cluster_role, @@GLOBAL.dolt_cluster_role_epoch;" "status\n0;@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nstandby,12"
    # new role, new epoch
    server_query_with_port 3309 repo1 1 dolt "" "call dolt_assume_cluster_role('primary', '13'); select @@GLOBAL.dolt_cluster_role, @@GLOBAL.dolt_cluster_role_epoch;" "status\n0;@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nprimary,13"

    # Server comes back up with latest assumed role.
    kill $SERVER_PID
    wait $SERVER_PID
    SERVER_PID=
    dolt sql-server --config server.yaml &
    SERVER_PID=$!
    wait_for_connection 3309 5000

    server_query_with_port 3309 repo1 1 dolt "" "select @@GLOBAL.dolt_cluster_role, @@GLOBAL.dolt_cluster_role_epoch;" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nprimary,13"
}

@test "sql-server-cluster: create database makes a new remote" {
    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: 3309
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:50052/{database}
  bootstrap_role: primary
  bootstrap_epoch: 10
  remotesapi:
    port: 50051" > server.yaml

    (cd repo1 && dolt remote add standby http://localhost:50052/repo1)
    (cd repo2 && dolt remote add standby http://localhost:50052/repo2)
    dolt sql-server --config server.yaml &
    SERVER_PID=$!
    wait_for_connection 3309 5000

    server_query_with_port 3309 repo1 1 dolt "" "create database a_new_database;use a_new_database;select name, url from dolt_remotes" ";;name,url\nstandby,http://localhost:50052/a_new_database"
}

@test "sql-server-cluster: sql-server fails to start if a configured remote is missing" {
    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: 3309
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:50052/{database}
  bootstrap_role: primary
  bootstrap_epoch: 10
  remotesapi:
    port: 50051" > server.yaml

    (cd repo1 && dolt remote add standby http://localhost:50052/repo1)
    run dolt sql-server --config server.yaml
    [ "$status" -ne 0 ]
    [[ "$output" =~ "destination remote standby does not exist" ]] || false
}
