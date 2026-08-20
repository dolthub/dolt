load "$BATS_TEST_DIRNAME/helper/query-server-common.bash"

# Helpers for testing dolt against a local S3-compatible object store.
# setup_minio      starts a MinIO server on a loopback port with a bucket ready
# teardown_minio   stops it and removes its data
# Call teardown_minio from the bats teardown() hook.
#
# MinIO ships as a single binary, so this needs no container runtime. CI
# installs it into .ci_bin, which is already on PATH.

# setup_minio:
MINIO_DIR=""              # working dir (data, log)
MINIO_PID=""              # pid of the running server
MINIO_PORT=""             # loopback TCP port the server listens on
MINIO_ENDPOINT=""         # http://127.0.0.1:$MINIO_PORT
MINIO_BUCKET="dolt-bats"  # bucket, created before the server starts

# setup_minio starts a MinIO server on a random loopback port and points the
# AWS credential chain at it. Skips the test if minio is not on PATH.
setup_minio() {
    local minio_bin
    minio_bin="$(command -v minio 2>/dev/null)" || skip "minio not found on PATH"

    MINIO_DIR="$(mktemp -d "$BATS_TEST_TMPDIR/minio.XXXXXX")"
    # A top level directory in the data dir is a bucket, so this is all the
    # bucket creation needed. No client involved.
    mkdir -p "$MINIO_DIR/data/$MINIO_BUCKET"

    MINIO_PORT="$(definePORT)"
    MINIO_ENDPOINT="http://127.0.0.1:$MINIO_PORT"

    # MinIO is not AWS. It rejects whatever real AWS credentials the
    # environment carries for the aws:// tests, and rejects the session token
    # that comes with a role-assumed identity along with them. Each bats test
    # runs in its own subshell, so this does not leak to other tests.
    export AWS_ACCESS_KEY_ID="minioadmin"
    export AWS_SECRET_ACCESS_KEY="minioadmin"
    export AWS_REGION="us-east-1"
    unset AWS_SESSION_TOKEN

    # Redirect stdio to /dev/null so minio does not inherit bats' open pipe
    # file descriptors and prevent bats from reaching EOF after all tests
    # complete, the same reason setup_git_sshd does it.
    MINIO_ROOT_USER="$AWS_ACCESS_KEY_ID" MINIO_ROOT_PASSWORD="$AWS_SECRET_ACCESS_KEY" \
        "$minio_bin" server "$MINIO_DIR/data" --address "127.0.0.1:$MINIO_PORT" \
        </dev/null >>"$MINIO_DIR/minio.log" 2>&1 &
    MINIO_PID=$!

    local i
    for (( i = 0; i < 100; i++ )); do
        # /dev/tcp is a bash built-in that avoids a dependency on netcat.
        (: >/dev/tcp/127.0.0.1/"$MINIO_PORT") 2>/dev/null && return 0
        sleep 0.1
    done

    echo "minio failed to start on port $MINIO_PORT" >&2
    cat "$MINIO_DIR/minio.log" >&2
    return 1
}

# teardown_minio stops the server from setup_minio and removes its data.
teardown_minio() {
    if [[ -n "${MINIO_PID:-}" ]]; then
        kill "$MINIO_PID" 2>/dev/null || true
        wait "$MINIO_PID" 2>/dev/null || true
        MINIO_PID=""
    fi
    [[ -n "${MINIO_DIR:-}" ]] && rm -rf "$MINIO_DIR"
    MINIO_DIR=""
}

# minio_url returns an s3:// url for database $1 on the running MinIO.
# path-style is required: MinIO has no wildcard DNS for virtual-hosted buckets.
minio_url() {
    echo "s3://$MINIO_BUCKET/$1?endpoint=$MINIO_ENDPOINT&region=$AWS_REGION&path-style=true"
}
