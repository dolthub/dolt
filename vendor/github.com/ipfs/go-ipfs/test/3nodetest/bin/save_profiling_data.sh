#!/bin/sh

for container in 3nodetest_bootstrap_1 3nodetest_client_1 3nodetest_server_1; do
    # ipfs binary is required by `go tool pprof`
    docker cp $container:/go/bin/ipfs build/profiling_data_$container
done

# since the nodes are executed with the --debug flag, profiling data is written
# to the the working dir. by default, the working dir is /go.

for container in 3nodetest_bootstrap_1 3nodetest_client_1 3nodetest_server_1; do
    docker cp $container:/go/ipfs.cpuprof build/profiling_data_$container
done

# TODO get memprof from client (client daemon isn't terminated, so memprof isn't retrieved)
for container in 3nodetest_bootstrap_1 3nodetest_server_1; do
    docker cp $container:/go/ipfs.memprof build/profiling_data_$container
done
