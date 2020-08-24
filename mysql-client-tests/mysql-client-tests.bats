# MySQL client tests are set up to test Dolt as a MySQL server and
# standard MySQL Clients in a wide array of languages. I used BATS because
# it was easy to set up the Dolt piece using the command line.
#
# We create a Dolt database and start a server in the setup(). Then, each test
# will attempt to access the server through a client. We'll do some basic
# reads and writes to make sure the client works. As we discover any
# gotchas, we can add tests for that specific language.

setup() {
    REPO_NAME="dolt_repo_$$"
    mkdir $REPO_NAME	
    cd $REPO_NAME

    dolt init
    let PORT="$$ % (65536-1024) + 1024"
    USER="dolt"
    dolt sql-server --host 0.0.0.0 --port=$PORT --user $USER &
    SERVER_PID=$!
}

teardown() {
    cd ..
    rm -rf $REPO_NAME
    kill -HUP $SERVER_PID
}

@test "python mysql client" {
    python3 $BATS_TEST_DIRNAME/python/mysql-client.py $USER $PORT $REPO_NAME
}
