load $BATS_TEST_DIRNAME/helper/query-server-common.bash

# Track the remotesrv process ID.
remotesrv_pid=""
# Track the port number that remotesrv is running on.
REMOTESRV_PORT=""

# Starts a remotesrv server on the specified port (or a random port if not specified).
# Usage: start_remotesrv [port]
start_remotesrv() {
    local port=${1:-$(definePORT)}
    mkdir -p $BATS_TMPDIR/remotes-$$
    remotesrv --http-port $port --grpc-port $port --dir $BATS_TMPDIR/remotes-$$ &> $BATS_TMPDIR/remotes-$$/remotesrv.log 3>&- &
    remotesrv_pid=$!
    REMOTESRV_PORT=$port

    if ! wait_for_port $port; then
        stop_remotesrv
        return 1
    fi
}

# Waits for a port to become available.
# Usage: wait_for_port <port> [timeout_ms]
wait_for_port() {
    local port=$1
    local timeout_ms=${2:-5000}
    local end_time=$((SECONDS+($timeout_ms/1000)))

    while [ $SECONDS -lt $end_time ]; do
        nc -z localhost "$port" >/dev/null 2>&1 && return 0
        sleep 0.1
    done

    echo "Failed to connect on port $port within $timeout_ms ms."
    return 1
}

# Stops the remotesrv server if it's running.
# Usage: stop_remotesrv
stop_remotesrv() {
    if [ ! -z "$remotesrv_pid" ]; then
        kill $remotesrv_pid 2>/dev/null || true
        wait $remotesrv_pid 2>/dev/null || true
        remotesrv_pid=""
    fi
    rm -rf $BATS_TMPDIR/remotes-$$
}

