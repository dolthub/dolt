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

    dolt sql -q "CREATE TABLE mysqldump_table(pk int)"
    dolt sql -q "INSERT INTO mysqldump_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext)"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"

    let PORT="$$ % (65536-1024) + 1024"
    USER="dolt"
    dolt sql-server --host 0.0.0.0 --port=$PORT --user=$USER --loglevel=trace &
    SERVER_PID=$!
    # Give the server a chance to start
    sleep 1

    export MYSQL_PWD=""
}

teardown() {
    cd ..
    kill $SERVER_PID
    rm -rf $REPO_NAME

    # Check if postgresql is still running. If so stop it
    active=$(service postgresql status)
    if echo "$active" | grep "online"; then
        service postgresql stop
    fi
}

@test "go go-sql-drive/mysql test" {
    (cd $BATS_TEST_DIRNAME/go; go build .)
    $BATS_TEST_DIRNAME/go/go $USER $PORT $REPO_NAME
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
    node $BATS_TEST_DIRNAME/node/knex.js $USER $PORT $REPO_NAME
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

@test "elixir myxql test" {
    cd $BATS_TEST_DIRNAME/elixir/
    # install some mix dependencies
    mix local.hex --force
    mix local.rebar --force
    mix deps.get

    # run the test
    mix run -e "IO.puts(SmokeTest.run())" $USER $PORT $REPO_NAME
}

@test "mysqldump works" {
    mysqldump $REPO_NAME -P $PORT -h 0.0.0.0 -u $USER
}

@test "mysql_fdw read path" {
    service postgresql start
    run su -c "psql -U postgres <<EOF
\x
CREATE EXTENSION mysql_fdw;

-- create server object
CREATE SERVER mysql_server
        FOREIGN DATA WRAPPER mysql_fdw
        OPTIONS (host '0.0.0.0', port '$PORT');

-- create user mapping
CREATE USER MAPPING FOR postgres
        SERVER mysql_server
        OPTIONS (username '$USER', password '');

-- create foreign table
CREATE FOREIGN TABLE warehouse
        (
                warehouse_id int,
                warehouse_name text
        )
        SERVER mysql_server
        OPTIONS (dbname '$REPO_NAME', table_name 'warehouse');

SELECT * FROM warehouse;
EOF" -m "postgres"
    [[ "$output" =~ "UPS" ]] || false
    [[ "$output" =~ "TV" ]] || false
    [[ "$output" =~ "Table" ]] || false
    service postgresql stop
}

@test "R RMySQL client" {
    Rscript $BATS_TEST_DIRNAME/r/rmysql-test.r $USER $PORT $REPO_NAME
}

@test "R RMariaDB client" {
    Rscript $BATS_TEST_DIRNAME/r/rmariadb-test.r $USER $PORT $REPO_NAME
}
