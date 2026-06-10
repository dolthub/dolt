#!/usr/bin/env bats
load helpers.bash

# MySQL client tests are set up to test Dolt as a MySQL server and
# standard MySQL Clients in a wide array of languages. I used BATS because
# it was easy to set up the Dolt piece using the command line.
#
# We create a Dolt database and start a server in the setup(). Then, each test
# will attempt to access the server through a client. We'll do some basic
# reads and writes to make sure the client works. As we discover any
# gotchas, we can add tests for that specific language.

setup() {
    setup_dolt_repo '1.2.3-MariaDB'

    export MYSQL_TCP_PORT=$PORT
    export MYSQL_HOST=127.0.0.1
}

teardown() {
    # Kill and wait the server before cd/rm so the process exits
    # before its data directory is removed.
    kill $SERVER_PID || :
    wait $SERVER_PID || :
    SERVER_PID=
    cd ..
    rm -rf $REPO_NAME
}

@test "python pymysql client" {
    /build/bin/python/custom-version-string-test $USER $PORT $REPO_NAME '1.2.3-MariaDB'
}
