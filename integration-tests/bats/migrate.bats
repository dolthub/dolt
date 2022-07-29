#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    skip_nbf_dolt_1
    skip_nbf_dolt_dev

    TARGET_NBF="__DOLT_1__"
    setup_common
}

teardown() {
    teardown_common
}

function checksum_table {
    QUERY="SELECT GROUP_CONCAT(column_name) FROM information_schema.columns WHERE table_name = '$1'"
    COLUMNS=$( dolt sql -q "$QUERY" -r csv | tail -n1 | sed 's/"//g' )
    dolt sql -q "SELECT CAST(SUM(CRC32(CONCAT($COLUMNS))) AS UNSIGNED) FROM $1 AS OF '$2';" -r csv | tail -n1
}

@test "migrate: smoke test" {
    dolt sql <<SQL
CREATE TABLE test (pk int primary key, c0 int, c1 int);
INSERT INTO test VALUES (0,0,0);
CALL dadd('-A');
CALL dcommit('-am', 'added table test');
SQL
    CHECKSUM=$(checksum_table test head)

    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "__LD_1__" ]] || false
    [[ ! $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false

    dolt migrate

    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false
    [[ ! $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "__LD_1__" ]] || false

    run checksum_table test head
    [[ "$output" =~ "$CHECKSUM" ]] || false

    run dolt sql -q "SELECT count(*) FROM dolt_commits" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "2" ]] || false
}

@test "migrate: functional transform" {
    dolt sql <<SQL
CREATE TABLE test (pk int primary key, c0 int, c1 int);
INSERT INTO test VALUES (0,0,0);
CALL dadd('-A');
CALL dcommit('-am', 'added table test');
SQL

    mkdir db_one db_two
    mv .dolt db_one
    pushd db_two/
    mkdir .dolt
    cp -r ../db_one/.dolt/* .dolt
    popd

    pushd db_one
    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false
    ONE=$(dolt branch -av)
    popd

    pushd db_two
    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false
    TWO=$(dolt branch -av)
    popd

    [[ "$ONE" = "$TWO" ]] || false
}

@test "migrate: manifest backup" {
    dolt sql <<SQL
CREATE TABLE test (pk int primary key, c0 int, c1 int);
INSERT INTO test VALUES (0,0,0);
CALL dadd('-A');
CALL dcommit('-am', 'added table test');
SQL

    dolt migrate
    [[ $(cat ./.dolt/noms/manifest.bak | cut -f 2 -d :) = "__LD_1__" ]] || false
    [[ ! $(cat ./.dolt/noms/manifest.bak | cut -f 2 -d :) = "$TARGET_NBF" ]] || false
}

@test "migrate: multiple branches" {
    dolt sql <<SQL
CREATE TABLE test (pk int primary key, c0 int, c1 int);
INSERT INTO test VALUES (0,0,0);
CALL dadd('-A');
CALL dcommit('-am', 'added table test');
SQL
    dolt branch one
    dolt branch two

    dolt sql <<SQL
CALL dcheckout('one');
INSERT INTO test VALUES (1,1,1);
CALL dcommit('-am', 'row (1,1,1)');
CALL dcheckout('two');
INSERT INTO test VALUES (2,2,2);
CALL dcommit('-am', 'row (2,2,2)');
CALL dmerge('one');
SQL

    MAIN=$(checksum_table test main)
    ONE=$(checksum_table test one)
    TWO=$(checksum_table test two)

    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false

    run checksum_table test main
    [[ "$output" =~ "$MAIN" ]] || false
    run checksum_table test one
    [[ "$output" =~ "$ONE" ]] || false
    run checksum_table test two
    [[ "$output" =~ "$TWO" ]] || false

    run dolt sql -q "SELECT count(*) FROM dolt_commits" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "4" ]] || false
}

@test "migrate: tag and working set" {
    dolt sql <<SQL
CREATE TABLE test (pk int primary key, c0 int, c1 int);
INSERT INTO test VALUES (0,0,0);
CALL dadd('-A');
CALL dcommit('-am', 'added table test');
CALL dtag('tag1', 'head');
INSERT INTO test VALUES (1,1,1);
CALL dcommit('-am', 'added rows');
INSERT INTO test VALUES (2,2,2);
SQL

    HEAD=$(checksum_table test head)
    PREV=$(checksum_table test head~1)
    TAG=$(checksum_table test tag1)
    [ $TAG -eq $PREV ]

    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false

    run checksum_table test head
    [[ "$output" =~ "$HEAD" ]] || false
    run checksum_table test head~1
    [[ "$output" =~ "$PREV" ]] || false
    run checksum_table test tag1
    [[ "$output" =~ "$TAG" ]] || false
}

@test "migrate: views and docs" {
    cat <<TXT > README.md
# Dolt is Git for Data!
Dolt is a SQL database that you can fork, clone, branch, merge, push
TXT

    dolt sql <<SQL
CREATE TABLE test (pk int primary key, c0 int, c1 int);
CREATE VIEW test2 AS SELECT c0, c1 FROM test;
INSERT INTO test VALUES (0,0,0);
CALL dadd('-A');
CALL dcommit('-am', 'added table test');
SQL
    dolt docs read README.md README.md
    dolt commit -am "added a README"

    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false

    run dolt sql -q "SELECT * FROM test2" -r csv
    [[ "$output" =~ "c0,c1" ]] || false
    [[ "$output" =~ "0,0" ]] || false

    run dolt docs write README.md
    [[ "$output" = $(cat README.md) ]] || false
}

@test "migrate: keyless tables" {
    dolt sql <<SQL
CREATE TABLE keyless (c0 int, c1 int);
INSERT INTO keyless VALUES (0,0),(1,1);
CALL dadd('-A');
CALL dcommit('-am', 'added keyless table');
INSERT INTO keyless VALUES (2,2),(3,3);
CALL dcommit('-am', 'added more rows');
SQL

    HEAD=$(checksum_table keyless head)
    PREV=$(checksum_table keyless head~1)

    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false

    run checksum_table keyless head
    [[ "$output" =~ "$HEAD" ]] || false
    run checksum_table keyless head~1
    [[ "$output" =~ "$PREV" ]] || false
}
