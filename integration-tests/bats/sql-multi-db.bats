#!/usr/bin/env bats
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

@test "sql-multi-db: sql multi-db test show databases" {
    EXPECTED=$(echo -e "Database\ninformation_schema\nrepo1\nrepo2")
    run dolt sql -r csv --multi-db-dir ./ -q "SHOW DATABASES"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "sql-multi-db: sql use statement and table accessibility" {
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

@test "sql-multi-db: sql test use invalid db name" {
    seed_repos_with_tables_with_use_statements

    run dolt sql -r csv --multi-db-dir ./ -q "USE invalid_db_name;"
    [ ! "$status" -eq 0 ]
    echo $output
    [[ "$output" =~ "database not found: invalid_db_name" ]] || false
}

@test "sql-multi-db: sql join tables in different databases" {
    seed_repos_with_tables_with_use_statements

    EXPECTED=$(echo -e "pk,c1\n2,200")
    run dolt sql -r csv --multi-db-dir ./ -b -q "
        USE repo1;
        SELECT r1_t1.pk as pk, repo2.r2_t1.c1 as c1 FROM r1_t1 JOIN repo2.r2_t1 ON r1_t1.pk=repo2.r2_t1.pk;"
    echo \"\"\"$output\"\"\"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "$EXPECTED" ]] || false
}

@test "sql-multi-db: join on multiple databases with same name" {
    seed_repos_with_tables_with_use_statements
    dolt sql --multi-db-dir ./ -b -q "
            USE repo1;
            CREATE TABLE r2_t1 (pk BIGINT, c1 BIGINT, PRIMARY KEY(pk));
            INSERT INTO r2_t1 (pk, c1) values (2,200),(3,300),(4,400);"
    run dolt sql --multi-db-dir ./ -q "select * from repo1.r2_t1 join repo2.r2_t1 on repo1.r2_t1.pk=repo2.r2_t1.pk"
    skip "Fails on Not unique table/alias"
    [ "$status" -eq 0 ]
    [[ ! $output =~ "Not unique table/alias" ]] || false
}

@test "sql-multi-db: fetch multiple databases with appropriate tempdir" {
    skip_nbf_dolt_1
    seed_repos_with_tables_with_use_statements
    mkdir remote1
    mkdir -p subremotes/repo1
    cd subremotes/repo1
    dolt init
    dolt remote add origin file://../../remote1
    dolt push origin main
    cd ..
    dolt clone file://../remote1 repo2

    cd ..
    run dolt sql --multi-db-dir ./subremotes -b -q "
        USE repo2;
        select dolt_fetch() as f;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "f" ]] || false
    [[ "${lines[2]}" =~ "1" ]] || false
}
