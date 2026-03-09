#!/usr/bin/env bats

load "$BATS_TEST_DIRNAME"/helper/common.bash
load "$BATS_TEST_DIRNAME"/helper/query-server-common.bash

TEST_NAME="dolt-nfs"
TEST_IMAGE="$TEST_NAME:bookworm-slim"
DOLT_REPOSITORY="/var/lib/dolt/nfs"

setup_file() {
    WORKSPACE_ROOT=$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)
    export WORKSPACE_ROOT

    docker build -f "$BATS_TEST_DIRNAME/nfsDockerfile" -t "$TEST_IMAGE" "$WORKSPACE_ROOT"
}

setup() {
    TEST_CONTAINER="${TEST_NAME}-$$"
    PORT=$( definePORT )

    docker run -d --name "$TEST_CONTAINER" --privileged -p "$PORT:3306" "$TEST_IMAGE" sh -lc 'sleep infinity' >/dev/null

    dsh "set -eu
mkdir -p '$DOLT_REPOSITORY'
cd '$DOLT_REPOSITORY'
dolt init"

    dsh "set -eu
mkdir -p /nfs/export /nfs/mount /run/rpcbind
dd if=/dev/zero of=/nfs/export.img bs=1M count=64
mkfs.ext4 -F /nfs/export.img
mount -o loop /nfs/export.img /nfs/export
echo '/nfs/export 127.0.0.1(rw,sync,no_subtree_check,no_root_squash,insecure,fsid=0)' > /etc/exports
rpcbind
rpc.statd
modprobe -q nfsd || true
mountpoint -q /proc/fs/nfsd || mount -t nfsd nfsd /proc/fs/nfsd
exportfs -r
rpc.nfsd 8
rpc.mountd
modprobe -q nfs || true
mount -t nfs -o vers=3,nolock 127.0.0.1:/nfs/export /nfs/mount"
}

teardown() {
    dsh "set -e
exportfs -u 127.0.0.1:/nfs/export
rpc.nfsd 0
pkill rpc.mountd
pkill rpc.statd
pkill rpcbind
umount -f /nfs/mount
umount -f /nfs/export
umount -f /proc/fs/nfsd"
    docker rm -f "$TEST_CONTAINER" >/dev/null 2>&1
}


dsh() {
    docker exec -i "$TEST_CONTAINER" sh -lc "$@"
}

ddolt() {
    docker exec -i -w "$DOLT_REPOSITORY" "$TEST_CONTAINER" dolt "$@"
}

start_docker_sql_server() {
    dsh "set -eu
cd '$DOLT_REPOSITORY'
export DOLT_ROOT_HOST='%'
dolt sql-server --host 0.0.0.0 --port 3306 --socket /tmp/dolt.sock >/tmp/dolt-server.log 2>&1 &
echo \$! > /tmp/dolt-server.pid"
    run wait_for_connection "$PORT" 8500
    [ "$status" -eq 0 ]
}

stop_docker_sql_server() {
    dsh "kill \$(cat /tmp/dolt-server.pid)"
}

# bats test_tags=no_lambda
@test "nfs: backup directory is deletable after dolt_backup sync and remove" {
    backup_dir="/nfs/mount/backup"
    backup_url="file://$backup_dir"

    ddolt sql -q "create table t (pk int primary key); insert into t values (1); call dolt_commit('-Am', 'init');"

    start_docker_sql_server

    ddolt sql -q "call dolt_add('-A');"
    ddolt sql -q "call dolt_backup('add', 'local_backup', '$backup_url');"
    ddolt sql -q "call dolt_backup('sync', 'local_backup');"
    ddolt sql -q "call dolt_backup('remove', 'local_backup');"

    run dsh "lsof +D \"$backup_dir\""
    [ "$status" -ne 0 ]

    dsh "rm -rf \"$backup_dir\"
[ ! -e \"$backup_dir\" ]"

    stop_docker_sql_server
}
