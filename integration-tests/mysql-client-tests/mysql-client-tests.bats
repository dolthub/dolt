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
    dolt sql-server --host 0.0.0.0 --port=$PORT --user=$USER --loglevel=trace &
    SERVER_PID=$!
    # Give the server a chance to start
    sleep 1
}

teardown() {
    cd ..
    kill $SERVER_PID
    rm -rf $REPO_NAME    
}

@test "go go-sql-drive/mysql test" {
    go run $BATS_TEST_DIRNAME/go/go-sql-driver-mysql-test.go $USER $PORT $REPO_NAME
}

@test "python mysql.connector client" {
    python3 $BATS_TEST_DIRNAME/python/mysql.connector-test.py $USER $PORT $REPO_NAME
}

@test "python pymysql client" {
    python3 $BATS_TEST_DIRNAME/python/pymysql-test.py $USER $PORT $REPO_NAME
}

@test "python sqlachemy client" {
    python3 $BATS_TEST_DIRNAME/python/sqlalchemy-test.py $USER $PORT $REPO_NAME
}

@test "mysql-connector-java client" {
    javac $BATS_TEST_DIRNAME/java/MySQLConnectorTest.java
    java -cp $BATS_TEST_DIRNAME/java:$BATS_TEST_DIRNAME/java/mysql-connector-java-8.0.21.jar MySQLConnectorTest $USER $PORT $REPO_NAME
}

@test "node mysql client" {
    node $BATS_TEST_DIRNAME/node/index.js $USER $PORT $REPO_NAME
}

@test "c mysql connector" {
    (cd $BATS_TEST_DIRNAME/c; make clean; make)
    $BATS_TEST_DIRNAME/c/mysql-connector-c-test $USER $PORT $REPO_NAME
}

@test "cpp mysql connector" {
    if [ -d $BATS_TEST_DIRNAME/cpp/_build ]
    then
	rm -rf $BATS_TEST_DIRNAME/cpp/_build/*
    else
	mkdir $BATS_TEST_DIRNAME/cpp/_build
    fi
    cd $BATS_TEST_DIRNAME/cpp/_build
    if [[ `uname` = "Darwin" ]]; then
	PATH=/usr/local/Cellar/mysql-client/8.0.21/bin/:"$PATH" cmake .. -DWITH_SSL=/usr/local/Cellar/openssl@1.1/1.1.1g/ -DWITH_JDBC=yes;
    else
        cmake ..
    fi
cmake ..
    make -j 10
    $BATS_TEST_DIRNAME/cpp/_build/test_mysql_connector_cxx $USER $PORT $REPO_NAME
    cd -
}

@test "dotnet mysql connector" {
    cd $BATS_TEST_DIRNAME/dotnet/MySqlConnector
    # dotnet run uses output channel 3 which conflicts with bats so we pipe it to null
    dotnet run -- $USER $PORT $REPO_NAME 3>&-
}

@test "dotnet mysql client" {
    cd $BATS_TEST_DIRNAME/dotnet/MySqlClient
    # dotnet run uses output channel 3 which conflicts with bats so we pipe it to null
    dotnet run -- $USER $PORT $REPO_NAME 3>&-
}

@test "perl DBD:mysql client" {
    perl $BATS_TEST_DIRNAME/perl/dbd-mysql-test.pl $USER $PORT $REPO_NAME
}

@test "ruby ruby/mysql test" {
    ruby $BATS_TEST_DIRNAME/ruby/ruby-mysql-test.rb $USER $PORT $REPO_NAME
}
