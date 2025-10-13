#!/usr/bin/env bats
load /build/bin/helpers.bash

# MySQL client tests are set up to test Dolt as a MySQL server and
# standard MySQL Clients in a wide array of languages. I used BATS because
# it was easy to set up the Dolt piece using the command line.
#
# We create a Dolt database and start a server in the setup(). Then, each test
# will attempt to access the server through a client. We'll do some basic
# reads and writes to make sure the client works. As we discover any
# gotchas, we can add tests for that specific language.

setup() {
    setup_dolt_repo
}

teardown() {
    cd ..
    teardown_dolt_repo

    # Check if postgresql is still running. If so stop it
    active=$(service postgresql status)
    if echo "$active" | grep "online"; then
        service postgresql stop
    fi
}

@test "go go-sql-driver/mysql" {
    /build/bin/go/sql-driver-mysql-test $USER $PORT $REPO_NAME
}

@test "go go-mysql" {
    /build/bin/go/mysql-client-test $USER $PORT $REPO_NAME
}

@test "python mysql.connector" {
    /build/bin/python/mysql-connector-test $USER $PORT $REPO_NAME
}

@test "python mariadb connector" {
  /build/bin/python/mariadb-connector-test $USER $PORT $REPO_NAME
}

@test "python pymysql client" {
    /build/bin/python/pymysql-test $USER $PORT $REPO_NAME
}

@test "python sqlachemy client" {
    /build/bin/python/sqlalchemy-test $USER $PORT $REPO_NAME
}

@test "java mysql-connector-j" {
    java -jar /build/bin/java/mysql-connector-test.jar $USER $PORT $REPO_NAME
}

@test "java mysql-connector-j collation" {
    java -jar /build/bin/java/mysql-connector-test-collation.jar $USER $PORT $REPO_NAME
}

@test "java mariadb-java-client" {
    java -jar /build/bin/java/mariadb-connector-test.jar $USER $PORT $REPO_NAME
}

@test "java r2dbc-mariadb connector" {
    java -jar /build/bin/java/mariadb-R2DBC-test.jar $USER $PORT $REPO_NAME
}

@test "node mysql client" {
    node /build/bin/node/index.js $USER $PORT $REPO_NAME
    node /build/bin/node/knex.js $USER $PORT $REPO_NAME
}

@test "node mariadb connector" {
    node /build/bin/node/mariadb-connector.js $USER $PORT $REPO_NAME
}

@test "node mysql client, hosted workbench stability" {
    node /build/bin/node/workbench.js $USER $PORT $REPO_NAME /build/bin/node/testdata
}

@test "c mysql client" {
    /build/bin/c/mysql-client-test $USER $PORT $REPO_NAME
}

@test "c mariadb client" {
    /build/bin/c/mariadb-client-test $USER $PORT $REPO_NAME
}

@test "cpp mysql connector" {
    /build/bin/cpp/mysql-connector-test $USER $PORT $REPO_NAME
}

@test "cpp mariadb connector" {
    /build/bin/cpp/mariadb-connector-test $USER $PORT $REPO_NAME
}

@test "dotnet mysql connector" {
    /build/bin/dotnet/mysql-connector-test $USER $PORT $REPO_NAME
}

@test "dotnet mysql client" {
    /build/bin/dotnet/mysql-client-test $USER $PORT $REPO_NAME
}

@test "perl DBD:mysql client" {
    perl /build/bin/perl/dbd-mysql-test.pl $USER $PORT $REPO_NAME
}

@test "perl DBD:MariaDB client" {
    perl /build/bin/perl/dbd-mariadb-test.pl $USER $PORT $REPO_NAME
}

@test "ruby ruby/mysql client" {
    ruby /build/bin/ruby/mysql-client-test.rb $USER $PORT $REPO_NAME
}

@test "ruby mysql2" {
    ruby /build/bin/ruby/mysql2-test.rb $USER $PORT $REPO_NAME
}

@test "elixir myxql" {
    /build/bin/elixir/myxql-driver-test $USER $PORT $REPO_NAME
}

@test "elixir mysql-otp" {
    /build/bin/elixir/mysql-otp-test $USER $PORT $REPO_NAME
}

@test "mysqldump" {
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
    Rscript /build/bin/r/rmysql-test.r $USER $PORT $REPO_NAME
}

@test "R RMariaDB client" {
    Rscript /build/bin/r/rmariadb-test.r $USER $PORT $REPO_NAME
}

@test "rust mysql client" {
    /build/bin/rust/mysql-client-test $USER $PORT $REPO_NAME
}

@test "php mysqli mysql client" {
    php /build/bin/php/mysqli_connector_test.php $USER $PORT $REPO_NAME
}

@test "php pdo mysql client" {
    php /build/bin/php/pdo_connector_test.php $USER $PORT $REPO_NAME
}

