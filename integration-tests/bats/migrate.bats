#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    TARGET_NBF="__DOLT__"
    setup_no_dolt_init
    dolt init --old-format
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
    [[ "$output" =~ "3" ]] || false

    dolt checkout dolt_migrated_commits
    run dolt sql -q "SELECT count(*) FROM dolt_commit_mapping" -r csv
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
    dolt branch -D dolt_migrated_commits
    ONE=$(dolt branch -av)
    popd

    pushd db_two
    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false
    dolt branch -D dolt_migrated_commits
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
    [[ "$output" =~ "6" ]] || false
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
    dolt docs upload README.md README.md
    dolt add .
    dolt commit -am "added a README"

    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false

    run dolt sql -q "SELECT * FROM test2" -r csv
    [[ "$output" =~ "c0,c1" ]] || false
    [[ "$output" =~ "0,0" ]] || false

    run dolt docs print README.md
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

@test "migrate: fixup key collations to match index order" {
    dolt sql <<SQL
CREATE TABLE t (
    a varchar(20) primary key COLLATE utf8mb4_0900_ai_ci NOT NULL,
    b varchar(20) COLLATE utf8mb4_0900_ai_ci NOT NULL,
    INDEX(b));
INSERT INTO t VALUES ('h/a', 'a'), ('h&a', 'B');
SQL

    run dolt schema show t
    [[ "$output" =~ "utf8mb4_0900_ai_ci" ]] || false
    [[ ! "$output" =~ "utf8mb4_0900_bin," ]] || false

    dolt migrate
    [[ $(cat ./.dolt/noms/manifest | cut -f 2 -d :) = "$TARGET_NBF" ]] || false

    # utf8mb4_0900_ai_ci is converted to utf8mb4_0900_bin to match index order
    dolt schema show t
    run dolt schema show t
    [[ "$output" =~ "utf8mb4_0900_bin" ]] || false
    [[ ! "$output" =~ "utf8mb4_0900_ai_ci" ]] || false

    run dolt sql -q "SELECT * FROM t ORDER BY a LIMIT 1" -r csv
    [[ "$output" =~ "h&a,B" ]] || false
}

@test "migrate: database with inverted primary key order" {
    dolt sql <<SQL
CREATE TABLE t (
    pk2 varchar(20) NOT NULL,
    pk1 varchar(20) NOT NULL,
    PRIMARY KEY (pk1, pk2));
INSERT INTO t (pk2, pk1) VALUES ("z","a"),("y","b"),("x","c");
SQL
    dolt commit -Am "added table t"

    run dolt schema show t
    [[ "$output" =~ "PRIMARY KEY (\`pk1\`,\`pk2\`)" ]] || false

    dolt migrate

    run dolt schema show t
    [[ "$output" =~ "PRIMARY KEY (\`pk1\`,\`pk2\`)" ]] || false
}

@test "migrate: removed tables stay removed" {
    dolt sql -q "create table alpha (pk int primary key);"
    dolt sql -q "create table beta (pk int primary key);"
    dolt commit -Am "create tables"

    dolt sql -q "alter table alpha rename to zulu;"
    dolt sql -q "drop table beta"
    dolt commit -Am "rename table alpha to zeta, drop table beta"

    dolt migrate

    run dolt ls
    [ $status -eq 0 ]
    [[ "$output" =~ "zulu" ]] || false
    [[ ! "$output" =~ "alpha" ]] || false
    [[ ! "$output" =~ "beta" ]] || false
}

@test "migrate: --drop-conflicts drops conflicts on migrate" {
    dolt sql <<SQL
CREATE TABLE test (pk int primary key, c0 int, c1 int);
INSERT INTO test VALUES (0,0,0);
CALL dcommit('-Am', 'added table test');
CALL dcheckout('-b', 'other');
CALL dbranch('third');
INSERT INTO test VALUES (1, 2, 3);
CALL dcommit('-am', 'added row on branch other');
CALL dcheckout('main');
INSERT INTO test VALUES (1, -2, -3);
CALL dcommit('-am', 'added row on branch main');
SET @@dolt_allow_commit_conflicts = 1;
CALL dmerge('other');
INSERT INTO test VALUES (9,9,9);
SET @@dolt_allow_commit_conflicts = 1;
SET @@dolt_force_transaction_commit = 1;
CALL dcommit( '--force', '-am', 'commit conflicts');
CALL dcheckout('third');
SQL
    dolt migrate --drop-conflicts
}

@test "migrate: no panic for migration on migrated database" {
    dolt sql <<SQL
CREATE TABLE test (pk int primary key, c0 int, c1 int);
INSERT INTO test VALUES (0,0,0);
CALL dadd('-A');
CALL dcommit('-am', 'added table test');
SQL
    dolt migrate
    run dolt migrate
    [ $status -eq 0 ]
    [[ "$output" =~ "already migrated" ]] || false
}

@test "migrate: changing primary key ordinals should migrate" {
    dolt sql -q "create table t (col1 int, col2 int, col3 enum('a', 'b'), primary key (col1, col2, col3))"
    dolt sql -q "insert into t values (1, 2, 'a'), (2, 3, 'b'), (3, 4, 'a');"
    dolt commit -Am "initial"

    dolt sql -q "alter table t drop primary key;"
    dolt sql -q "alter table t add primary key (col3, col2, col1);"
    dolt commit -am "change primary key order"

    dolt sql -q "insert into t values (5, 6, 'b');"
    dolt commit -am "add new row"

    dolt migrate
    run dolt sql -r csv -q "select * from t order by col1 asc;"
    [ $status -eq 0 ]
    [[ $output =~ "col1,col2,col3" ]]
    [[ $output =~ "1,2,a" ]]
    [[ $output =~ "2,3,b" ]]
    [[ $output =~ "3,4,a" ]]
    [[ $output =~ "5,6,b" ]]
}

@test "migrate: indexes, collation, and checks should be preserved" {
   dolt sql -q "create table t (i int primary key, j int, v varchar(100), index (j), constraint j_chk check (j = 0)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
   run dolt sql -q "show create table t"
   [ $status -eq 0 ]
   [[ $output =~ "KEY \`j\` (\`j\`)" ]] || false
   [[ $output =~ "CONSTRAINT \`j_chk\` CHECK ((\`j\` = 0))" ]] || false
   [[ $output =~ ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci" ]] || false

   dolt commit -Am "create table"

   dolt migrate
   run dolt sql -q "show create table t"
   [ $status -eq 0 ]
   [[ $output =~ "KEY \`j\` (\`j\`)" ]] || false
   [[ $output =~ "CONSTRAINT \`j_chk\` CHECK ((\`j\` = 0))" ]] || false
   [[ $output =~ ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci" ]] || false
}
