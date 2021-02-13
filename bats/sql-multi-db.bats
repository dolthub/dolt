load $BATS_TEST_DIRNAME/helper/common.bash

make_repo() {
    mkdir $1
    cd $1
    echo $PWD
    dolt init
    cd ..
}

setup() {
    setup_no_dolt_init
    make_repo repo1
    make_repo repo2
}

teardown() {
    teardown_common
}

seed_repos_with_tables_with_use_statements() {
    dolt sql -r csv --multi-db-dir ./ -b -q "
            USE repo1;
            CREATE TABLE r1_t1 (pk BIGINT, PRIMARY KEY(pk));
            INSERT INTO r1_t1 (pk) values (0),(1),(2);
            USE repo2;
            CREATE TABLE r2_t1 (pk BIGINT, c1 BIGINT, PRIMARY KEY(pk));
            INSERT INTO r2_t1 (pk, c1) values (2,200),(3,300),(4,400);"
}

@test "sql multi-db test show databases" {
    EXPECTED=$(echo -e "Database\ninformation_schema\nrepo1\nrepo2")
    run dolt sql -r csv --multi-db-dir ./ -q "SHOW DATABASES"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "sql use statement and table accessibility" {
    seed_repos_with_tables_with_use_statements

    EXPECTED_R1T1=$(echo -e "pk\n0\n1\n2")
    run dolt sql -r csv --multi-db-dir ./ -b -q "USE repo1; SELECT * FROM r1_t1;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED_R1T1" ]] || false

    EXPECTED_R2T1=$(echo -e "pk,c1\n2,200\n3,300\n4,400")
    run dolt sql -r csv --multi-db-dir ./ -b -q "USE repo2; SELECT * FROM r2_t1;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED_R2T1" ]] || false

    # test tables of other database inaccessible without database qualifier
    run dolt sql -r csv --multi-db-dir ./ -b -q "USE repo1; SELECT * FROM r2_t1;"
    [ ! "$status" -eq 0 ]
    run dolt sql -r csv --multi-db-dir ./ -b -q "USE repo2; SELECT * FROM r1_t1;"
    [ ! "$status" -eq 0 ]

    # test tables in other databases accessible when qualified
    run dolt sql -r csv --multi-db-dir ./ -b -q "USE repo1; SELECT * FROM repo2.r2_t1;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED_R2T1" ]] || false
    run dolt sql -r csv --multi-db-dir ./ -b -q "USE repo2; SELECT * FROM repo1.r1_t1;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED_R1T1" ]] || false
}

@test "sql test use invalid db name" {
    seed_repos_with_tables_with_use_statements

    run dolt sql -r csv --multi-db-dir ./ -q "USE invalid_db_name;"
    [ ! "$status" -eq 0 ]
    echo $output
    [[ "$output" =~ "database not found: invalid_db_name" ]] || false
}

@test "sql join tables in different databases" {
    seed_repos_with_tables_with_use_statements

    EXPECTED=$(echo -e "pk,c1\n2,200")
    run dolt sql -r csv --multi-db-dir ./ -b -q "
        USE repo1;
        SELECT r1_t1.pk, repo2.r2_t1.c1 FROM r1_t1 JOIN repo2.r2_t1 ON r1_t1.pk=repo2.r2_t1.pk;"
    echo \"\"\"$output\"\"\"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "sql create new database" {
    dolt init

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
    [[ ! "$output" =~ "mydb" ]] || false
}

@test "sql create and dropnew database" {
    dolt init

    run dolt sql << SQL
CREATE DATABASE mydb;
DROP DATABASE mydb;
USE mydb;
SQL
    [ "$status" -eq 1 ]
    [[ "$output" =~ "database not found: mydb" ]] || false
}

@test "sql create new database IF EXISTS works" {
    dolt init

    # Test bad syntax.
    run dolt sql -q "CREATE DATABASE IF EXISTS test;"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error parsing SQL" ]] || false

    run dolt sql << SQL
CREATE DATABASE IF NOT EXISTS test;
SHOW DATABASES;
USE test;
CREATE TABLE test (
    pk int primary key
);
INSERT INTO test VALUES (222);
SELECT COUNT(*) FROM test WHERE pk=222;
DROP DATABASE test;
SQL
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt_repo_$$" ]] || false
    [[ "$output" =~ "information_schema" ]] || false
    [[ "$output" =~ "test" ]] || false
    # From COUNT
    [[ "$output" =~ "1" ]] || false

    run dolt sql << SQL
CREATE DATABASE IF NOT EXISTS test;
SHOW DATABASES;
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

@test "sql drop database errors for non memory databases" {
    dolt init

    run dolt sql -q "DROP DATABASE dolt_repo_$$"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "DROP DATABASE isn't supported for database dolt_repo_$$" ]] || false

    run dolt sql -q "DROP DATABASE information_schema"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "DROP DATABASE isn't supported for database information_schema" ]] || false
}

@test "sql create new database via SCHEMA alias" {
    dolt init

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
    [[ ! "$output" =~ "mydb" ]] || false
}
