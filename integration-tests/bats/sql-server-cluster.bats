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
    stop_sql_server
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
  - name: doltdb-1
    remote_url_template: http://doltdb-1.doltdb:50051/{database}
  bootstrap_role: standby
  bootstrap_epoch: 10
  remotesapi:
    port: 50051" > server.yaml

    dolt sql-server --config server.yaml &
    SERVER_PID=$!
    wait_for_connection 3309 5000

    server_query_with_port 3309 repo1 1 dolt "" "select @@GLOBAL.dolt_cluster_role, @@GLOBAL.dolt_cluster_role_epoch" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nstandby,10"

    kill $SERVER_PID
    wait $SERVER_PID
    SERVER_PID=

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
  - name: doltdb-1
    remote_url_template: http://doltdb-1.doltdb:50051/{database}
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
  - name: doltdb-1
    remote_url_template: http://doltdb-1.doltdb:50051/{database}
  bootstrap_role: standby
  bootstrap_epoch: 10
  remotesapi:
    port: 50051" > server.yaml

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
