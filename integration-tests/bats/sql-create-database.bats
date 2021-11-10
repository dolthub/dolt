load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

# These tests use batch mode since the in memory db spawned by CREATE DATABASE expire after each session.
@test "sql-create-database: create new database" {
    run dolt sql << SQL
CREATE DATABASE mydb;
SHOW DATABASES;
USE mydb;
CREATE TABLE test (
    pk int primary key
);
INSERT INTO test VALUES (222);
SELECT COUNT(*) FROM test WHERE pk=222;
DROP DATABASE mydb;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt_repo_$$" ]] || false
    [[ "$output" =~ "information_schema" ]] || false
    [[ "$output" =~ "mydb" ]] || false
    # From COUNT
    [[ "$output" =~ "1" ]] || false

    run dolt sql -q "SHOW DATABASES"
    [[ "$output" =~ "mydb" ]] || false
}

@test "sql-create-database: drop database" {
    dolt sql <<SQL
create database mydb;
use mydb;
create table test(a int primary key);
select dolt_commit("-am", "first commit");
SQL

    [ -d mydb ]
    cd mydb

    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ "first commit" ]] || false

    cd ..

    dolt sql -q "drop database mydb"

    [ ! -d mydb ]
}

@test "sql-create-database: create database that already exists throws an error" {
    run dolt sql << SQL
CREATE DATABASE mydb;
CREATE DATABASE mydb;
SQL

    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't create database mydb; database exists" ]] || false
}

@test "sql-create-database: create database IF NOT EXISTS on database that already exists doesn't throw an error" {
    run dolt sql << SQL
CREATE DATABASE mydb;
CREATE DATABASE IF NOT EXISTS mydb;
SQL

    [ "$status" -eq 0 ]
}

@test "sql-create-database: create and drop new database" {
    run dolt sql << SQL
CREATE DATABASE mydb;
DROP DATABASE mydb;
SQL
    [ "$status" -eq 0 ]
    
    [ ! -d mydb ]

    run dolt sql -q "use mydb"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "database not found: mydb" ]] || false
}

@test "sql-create-database: create new database IF EXISTS works" {
    # Test bad syntax.
    run dolt sql -q "CREATE DATABASE IF EXISTS test;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error parsing SQL" ]] || false

    dolt sql -q "CREATE DATABASE IF NOT EXISTS test;"
    run dolt sql -q "SHOW DATABASES;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt_repo_$$" ]] || false
    [[ "$output" =~ "information_schema" ]] || false
    [[ "$output" =~ "test" ]] || false

    run dolt sql <<SQL
USE test;
CREATE TABLE test (
    pk int primary key
);
INSERT INTO test VALUES (222);
SQL
    [ "$status" -eq 0 ]

    run dolt sql <<SQL
USE test;
SELECT COUNT(*) FROM test WHERE pk=222;
SQL
    [[ "$output" =~ "1" ]] || false

    dolt sql -q "drop database test"
    
    run dolt sql << SQL
CREATE DATABASE IF NOT EXISTS test;
USE test;
DROP DATABASE IF EXISTS test;
DROP DATABASE IF EXISTS test;
SQL
    # The second drop database should just return a warning resulting in a status of 0.
    [ "$status" -eq 0 ]

    run dolt sql << SQL
CREATE DATABASE IF NOT EXISTS test;
SHOW DATABASES;
USE test;
DROP DATABASE IF NOT EXISTS test;
SQL
    # IF NOT should not work with drop.
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error parsing SQL" ]] || false
}

@test "sql-create-database: drop database throws error for database that doesn't exist" {
    run dolt sql -q "DROP DATABASE mydb;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "database not found" ]] || false
}

@test "sql-create-database: sql drop database errors for info schema" {
    run dolt sql -q "DROP DATABASE information_schema"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "DROP DATABASE isn't supported for database information_schema" ]] || false
}

@test "sql-create-database: create new database via SCHEMA alias" {
    run dolt sql << SQL
CREATE SCHEMA mydb;
SHOW DATABASES;
USE mydb;
CREATE TABLE test (
    pk int primary key
);
INSERT INTO test VALUES (222);
SELECT COUNT(*) FROM test WHERE pk=222;
DROP SCHEMA mydb;
SQL
    [[ "$output" =~ "dolt_repo_$$" ]] || false
    [[ "$output" =~ "information_schema" ]] || false
    [[ "$output" =~ "mydb" ]] || false
    [[ "$output" =~ "1" ]] || false

    run dolt sql -q "SHOW DATABASES"
    [[ "$output" =~ "mydb" ]] || false
}

@test "sql-create-database: use for non existing database throws an error" {
    run dolt sql -q "USE test"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "database not found: test" ]] || false
}

@test "sql-create-database: SHOW DATABASES works after CREATE and DROP" {
    run dolt sql -q "SHOW DATABASES"
    before=$output

    run dolt sql << SQL
CREATE DATABASE hi;
DROP DATABASE hi;
SHOW DATABASES;
SQL

    [ "$status" -eq 0 ]
    [[ "$output" =~ "$before" ]] || false
}
