#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir -p "$1"
  (cd "$1"; dolt init)
}

SERVERONE_MYSQL_PORT=3309
SERVERONE_GRPC_PORT=50051

SERVERTWO_MYSQL_PORT=3310
SERVERTWO_GRPC_PORT=50052

setup() {
    skiponwindows "tests are flaky on Windows"
    setup_no_dolt_init

    make_repo serverone/repo1
    make_repo serverone/repo2

    make_repo servertwo/repo1
    make_repo servertwo/repo2
}

teardown() {
    stop_sql_server "1"
    teardown_common
}

@test "sql-server-cluster: an older primary comes up, becomes a standby and does not overwrite newer primary" {
    cd serverone

    cd repo1
    dolt sql -q 'create table vals (i int primary key)'
    dolt sql -q 'insert into vals values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)'
    cd ../

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 15
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    serverone_pid=$!

    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    cd ../servertwo

    cd repo1
    dolt sql -q 'create table vals (i int primary key)'
    dolt sql -q 'insert into vals values (1),(2),(3),(4),(5)'
    cd ../

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERTWO_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERONE_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERTWO_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    servertwo_pid=$!

    wait_for_connection "${SERVERTWO_MYSQL_PORT}" 5000

    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "SELECT count(*) FROM vals" "count(*)\n10"
    server_query_with_port "${SERVERTWO_MYSQL_PORT}" repo1 1 dolt "" "SELECT @@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nstandby,15"

    kill $servertwo_pid
    wait $servertwo_pid

    kill $serverone_pid
    wait $serverone_pid
}

@test "sql-server-cluster: a new primary comes up, old primary becomes a standby and has its state overwritten" {
    cd serverone

    cd repo1
    dolt sql -q 'create table vals (i int primary key)'
    dolt sql -q 'insert into vals values (1),(2),(3),(4),(5)'
    cd ../

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    serverone_pid=$!

    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    cd ../servertwo

    cd repo1
    dolt sql -q 'create table vals (i int primary key)'
    dolt sql -q 'insert into vals values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)'
    cd ../

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERTWO_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERONE_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 15
  remotesapi:
    port: ${SERVERTWO_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    servertwo_pid=$!

    wait_for_connection "${SERVERTWO_MYSQL_PORT}" 5000

    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "SELECT @@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nstandby,15"
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "SELECT count(*) FROM vals" "count(*)\n10"
    server_query_with_port "${SERVERTWO_MYSQL_PORT}" repo1 1 dolt "" "SELECT @@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch" "@@GLOBAL.dolt_cluster_role,@@GLOBAL.dolt_cluster_role_epoch\nprimary,15"

    kill $servertwo_pid
    wait $servertwo_pid

    kill $serverone_pid
    wait $serverone_pid
}

@test "sql-server-cluster: primary -> standby without the standby up fails" {
    cd serverone

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 10
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    (cd repo1 && dolt remote add standby http://localhost:"${SERVERTWO_GRPC_PORT}"/repo1)
    (cd repo2 && dolt remote add standby http://localhost:"${SERVERTWO_GRPC_PORT}"/repo2)
    dolt sql-server --config server.yaml &
    SERVER_PID=$!
    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    # when we do this, we become read only for a little bit...
    run server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "call dolt_assume_cluster_role('standby', '11');" "" 1
    [[ "$output" =~ "failed to transition from primary to standby gracefully" ]] || false

    # after that fails, we should still be primary and we should accept writes.
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "create table vals (i int primary key);insert into vals values (0);select i from vals" ";;i\n0"
}

@test "sql-server-cluster: primary replicates to standby, fails over, new primary replicates to standby, fails over, new primary has all writes" {
    cd serverone

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERONE_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERTWO_GRPC_PORT}/{database}
  bootstrap_role: standby
  bootstrap_epoch: 1
  remotesapi:
    port: ${SERVERONE_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERTWO_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    serverone_pid=$!

    wait_for_connection "${SERVERONE_MYSQL_PORT}" 5000

    cd ../servertwo

    echo "
log_level: trace
user:
  name: dolt
listener:
  host: 0.0.0.0
  port: ${SERVERTWO_MYSQL_PORT}
behavior:
  read_only: false
  autocommit: true
cluster:
  standby_remotes:
  - name: standby
    remote_url_template: http://localhost:${SERVERONE_GRPC_PORT}/{database}
  bootstrap_role: primary
  bootstrap_epoch: 1
  remotesapi:
    port: ${SERVERTWO_GRPC_PORT}" > server.yaml

    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.email bats@email.fake
    DOLT_ROOT_PATH=`pwd` dolt config --global --add user.name "Bats Tests"
    DOLT_ROOT_PATH=`pwd` dolt config --global --add metrics.disabled true

    (cd repo1 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo1)
    (cd repo2 && dolt remote add standby http://localhost:${SERVERONE_GRPC_PORT}/repo2)
    DOLT_ROOT_PATH=`pwd` dolt sql-server --config server.yaml &
    servertwo_pid=$!

    wait_for_connection "${SERVERTWO_MYSQL_PORT}" 5000
    server_query_with_port "${SERVERTWO_MYSQL_PORT}" repo1 1 dolt "" "create table vals (i int primary key);insert into vals values (0),(1),(2),(3),(4);call dolt_assume_cluster_role('standby', 2);" ";;status\n0"
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "select count(*) from vals;call dolt_assume_cluster_role('primary', 2)" "count(*)\n5;status\n0"
    server_query_with_port "${SERVERONE_MYSQL_PORT}" repo1 1 dolt "" "insert into vals values (5),(6),(7),(8),(9);call dolt_assume_cluster_role('standby', 3)" ";status\n0"
    server_query_with_port "${SERVERTWO_MYSQL_PORT}" repo1 1 dolt "" "select count(*) from vals;call dolt_assume_cluster_role('primary', 3)" "count(*)\n10;status\n0"

    kill $servertwo_pid
    wait $servertwo_pid

    kill $serverone_pid
    wait $serverone_pid
}
