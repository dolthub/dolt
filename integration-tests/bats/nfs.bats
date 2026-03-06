#!/usr/bin/env bats

load "$BATS_TEST_DIRNAME"/helper/common.bash
load "$BATS_TEST_DIRNAME"/helper/query-server-common.bash

TEST_NAME="dolt-nfs"
TEST_IMAGE="$TEST_NAME:bookworm-slim"
TEST_CONTAINER=""
DOLT_REPOSITORY="/var/lib/dolt/nfs"
PORT=""

setup_file() {
    WORKSPACE_ROOT=$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)
    export WORKSPACE_ROOT

    docker build -f "$BATS_TEST_DIRNAME/nfsDockerfile" -t "$TEST_IMAGE" "$WORKSPACE_ROOT"
}

csh() {
    docker exec -i "$TEST_CONTAINER" sh -lc "$@"
}

cdolt() {
    docker exec -i -w "$DOLT_REPOSITORY" "$TEST_CONTAINER" dolt "$@"
}

start_container_sql_server() {
    csh "set -eu
cd '$DOLT_REPOSITORY'
export DOLT_ROOT_HOST='%'
dolt sql-server --host 0.0.0.0 --port 3306 --socket /tmp/dolt.sock >/tmp/dolt-server.log 2>&1 &
echo \$! > /tmp/dolt-server.pid"
    run wait_for_connection "$PORT" 8500
    [ "$status" -eq 0 ]
}

setup() {
    TEST_CONTAINER="${TEST_NAME}-$$"
    PORT=$( definePORT )

    docker run -d --name "$TEST_CONTAINER" --privileged -p "$PORT:3306" "$TEST_IMAGE" sh -lc 'sleep infinity' >/dev/null

    csh "set -eu
mkdir -p '$DOLT_REPOSITORY'
cd '$DOLT_REPOSITORY'
dolt config --global --add user.email 'bats@email.fake'
dolt config --global --add user.name 'Bats Tests'
dolt init"

    csh "set -eu
mkdir -p /nfs/export /nfs/mount /run/rpcbind /var/lib/nfs/rpc_pipefs
dd if=/dev/zero of=/nfs/export.img bs=1M count=64
mkfs.ext4 -F /nfs/export.img
mount -o loop /nfs/export.img /nfs/export
echo '/nfs/export 127.0.0.1(rw,sync,no_subtree_check,no_root_squash,insecure,fsid=0)' > /etc/exports
rpcbind -w >/tmp/rpcbind.log 2>&1
rpc.nfsd 8
rpc.mountd >/tmp/rpc.mountd.log 2>&1 &
exportfs -rav
mount -t nfs -o nolock 127.0.0.1:/nfs/export /nfs/mount"
}

teardown() {
    csh "kill \$(cat /tmp/dolt-server.pid) 2>/dev/null || true
exportfs -u 127.0.0.1:/nfs/export 2>/dev/null || true
rpc.nfsd 0 2>/dev/null || true
pkill rpc.mountd 2>/dev/null || true
pkill rpcbind 2>/dev/null || true
umount -f /nfs/mount 2>/dev/null || true
umount -f /nfs/export 2>/dev/null || true" || true
    docker rm -f "$TEST_CONTAINER" >/dev/null 2>&1
}

# bats test_tags=no_lambda
@test "nfs: backup directory is deletable after dolt_backup sync and remove (issue #10588)" {
    backup_dir="/nfs/mount/backup"
    backup_url="file://$backup_dir"

    cdolt sql -q "create table t (pk int primary key); insert into t values (1); call dolt_commit('-Am', 'init');"

    start_container_sql_server

    cdolt sql -q "call dolt_add('-A');"
    cdolt sql -q "call dolt_backup('add', 'local_backup', '$backup_url');"
    cdolt sql -q "call dolt_backup('sync', 'local_backup');"
    cdolt sql -q "call dolt_backup('remove', 'local_backup');"

    run csh "lsof +D \"$backup_dir\""
    [ "$status" -ne 0 ]

    csh "rm -rf \"$backup_dir\"
[ ! -e \"$backup_dir\" ]"
}
