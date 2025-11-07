#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "sql-charsets-collations: show character set" {
    run dolt sql -q "show character set";
    [ $status -eq 0 ]
    [[ $output =~ "utf8mb4 " ]] || false
    [[ $output =~ "ascii " ]] || false
    [[ $output =~ "utf8mb4_0900_ai_ci" ]] || false
    [[ $output =~ "ascii_general_ci" ]] || false
}

@test "sql-charsets-collations: define charset and collation on a column" {
    dolt sql -q "create table german1 (c char(10) CHARACTER SET latin1 COLLATE latin1_german1_ci)"
    run dolt sql -q "show create table german1";
    [ $status -eq 0 ]
    [[ $output =~ "CHARACTER SET latin1" ]] || false
    [[ $output =~ "COLLATE latin1_german1_ci" ]] || false

    # check information_schema.COLUMNS table
    run dolt sql -q "select table_name, column_name, character_set_name, collation_name from information_schema.COLUMNS;" -r csv
    [[ "$output" =~ "german1,c,latin1,latin1_german1_ci" ]] || false
}

@test "sql-charsets-collations: define charset and collation on a table" {
    dolt sql -q "create table german1 (c char(10)) CHARACTER SET latin1 COLLATE latin1_german1_ci"
    run dolt sql -q "show create table german1";
    [ $status -eq 0 ]
    [[ $output =~ "latin1_german1_ci" ]] || false
    [[ ! $output =~ "utf8" ]] || false

    # check information_schema.TABLES table
    run dolt sql -q "select table_name, table_type, table_collation from information_schema.TABLES where table_name = 'german1';" -r csv
    [[ "$output" =~ "german1,BASE TABLE,latin1_german1_ci" ]] || false
}

@test "sql-charsets-collations: define charset and collation on a database" {
    start_sql_server
    dolt sql -q "CREATE DATABASE test CHARACTER SET latin1 COLLATE latin1_swedish_ci;"
    dolt sql -q "SELECT @@character_set_database; set @@SESSION.character_set_database=latin1"
    dolt sql -q "SELECT @@character_set_database; set @@SESSION.collation_database=latin1_swedish_ci"
    stop_sql_server
    sleep 0.5
}

@test "sql-charsets-collations: define and use a collation and charset" {
    dolt sql -q "create table german1 (c char(10) CHARACTER SET latin1 COLLATE latin1_german1_ci)"
    dolt sql -q "insert into german1 values ('Bar'), ('Bär')"
    run dolt sql -q "SELECT * FROM german1 WHERE c = 'Bär'"
    [ $status -eq 0 ]
    [[ $output =~ 'Bar' ]] || false
    [[ $output =~ 'Bär' ]] || false
    [ ${#lines[@]} -eq 6 ]

    dolt sql -q	"create table german2 (c char(10) CHARACTER SET latin1 COLLATE latin1_german2_ci)"
    dolt sql -q	"insert into german2 values ('Bar'), ('Bär')"
    run dolt sql -q "SELECT * FROM german2 WHERE c = 'Bär'"
    [ $status -eq 0 ]
    [[ $output =~ 'Bär' ]] || false
    [[ ! $output =~ 'Bar' ]] || false
    [ ${#lines[@]} -eq 5 ]
}

@test "sql-charsets-collations: modify a charset on a column" {
    dolt sql -q "create table german1 (c char(10) CHARACTER SET latin1 COLLATE latin1_german1_ci)"
    dolt sql -q	"insert into german1 values ('Bar'), ('Bär')"
    dolt sql -q "alter table german1 modify column c char(10) CHARACTER SET utf8mb4"
    run dolt sql -q "show create table german1"
    [ $status -eq 0 ]
    [[ $output =~ 'utf8mb4' ]] || false
    [[ $output =~ 'utf8mb4_0900_ai_ci' ]] || false
    [[ ! $output =~ 'latin1_german1_ci' ]] || false
    [[ ! $output =~ 'latin1' ]] || false
}

@test "sql-charsets-collations: modify a collation on a column" {
    dolt sql -q "create table german1 (c char(10) CHARACTER SET latin1 COLLATE latin1_german1_ci)"
    dolt sql -q	"insert into german1 values ('Bar'), ('Bär')"
    dolt sql -q "alter table german1 modify column c char(10) COLLATE latin1_german2_ci"
    run dolt sql -q "show create table german1"
    [ $status -eq 0 ]
    [[ $output =~ 'latin1_german2_ci' ]] || false
    [[ ! $output =~ 'latin1_german1_ci' ]] || false

    run dolt sql -q "SELECT * FROM german1 WHERE c = 'Bär'"
    [ $status -eq 0 ]
    [[ ! $output =~ 'Bar' ]] || false
    [[ $output =~ 'Bär' ]] || false
    [ ${#lines[@]} -eq 5 ]
}

@test "sql-charsets-collations: collations respected in regexes" {
    # Simple case insensitive matches in utf8mb4 do the right thing
    dolt sql -q "create table t (c char(10) character set utf8mb4 collate utf8mb4_0900_ai_ci)"
    dolt sql -q "insert into t values ('A'), ('a')"
    run dolt sql -q "select c from t where c like '%A%'"
    [ $status -eq 0 ]
    [[ $output =~ "a" ]] || false
    [[ $output =~ "A" ]] || false

    dolt sql -q "alter table t modify column c char(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin"
    run	dolt sql -q "select c from t where c like '%A%'"
    [[ ! $output =~ "a" ]] || false
    [[ $output =~ "A" ]] || false
    
    dolt sql -q "drop table t"
    
    # Outside of ascii, no such luck
    dolt sql -q "create table t (c varchar(100) character set utf8mb4 collate utf8mb4_unicode_ci)"
    dolt sql -q "insert into t values ('schön'), ('schon')"
    run	dolt sql -q "select c from t where c like '%o%'"
    [ $status -eq 0 ]
    [[ $output =~ "schon" ]] || false
    [[ $output =~ "schön" ]] || false
}

@test "sql-charsets-collations: collations respected wildcard select and count" {
    start_sql_server

    dolt sql << 'SQL'
CREATE TABLE DATABASECHANGELOG (
    ID varchar(255) NOT NULL,
    AUTHOR varchar(255) NOT NULL,
    FILENAME varchar(255) NOT NULL,
    DATEEXECUTED datetime NOT NULL,
    ORDEREXECUTED int NOT NULL,
    EXECTYPE varchar(10) NOT NULL,
    MD5SUM varchar(35),
    DESCRIPTION varchar(255),
    COMMENTS varchar(255),
    TAG varchar(255),
    LIQUIBASE varchar(20),
    CONTEXTS varchar(255),
    LABELS varchar(255),
    DEPLOYMENT_ID varchar(10),
    UNIQUE KEY idx_databasechangelog_id_author_filename (ID,AUTHOR,FILENAME)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO DATABASECHANGELOG (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, EXECTYPE, MD5SUM, DESCRIPTION, COMMENTS, TAG, LIQUIBASE, CONTEXTS, LABELS, DEPLOYMENT_ID)
VALUES('v50.2024-01-04T13:52:51', 'noahmoss', 'migrations/001_update_migrations.yaml', '2024-11-04 18:39:46', 334, 'EXECUTED', '9:19f8b6614c4fe95ff71b42830785df04', 'createTable tableName=data_permissions', 'Data permissions table', NULL, '4.26.0', NULL, NULL, '2252723448');
SQL

    run dolt sql -q "select count(1) from DATABASECHANGELOG where ID like 'V50.2024-01-04%';"
    [ $status -eq 0 ]
    [[ $output == *"1"* ]] || false

    run dolt sql -q "select * from DATABASECHANGELOG where ID like 'V50.2024-01-04%';"
    [ $status -eq 0 ]
    [[ $output == *"v50.2024-01-04T13:52:51"* ]] || false
    [[ $output == *"noahmoss"* ]] || false
    [[ $output == *"migrations/001_update_migrations.yaml"* ]] || false
    [[ $output == *"2024-11-04"* ]] || false
    [[ $output == *"334"* ]] || false
    [[ $output == *"EXECUTED"* ]] || false
    [[ $output == *"9:19f8b6614c4fe95ff71b42830785df04"* ]] || false
    [[ $output == *"createTable tableName=data_permissions"* ]] || false
    [[ $output == *"Data permissions table"* ]] || false
    [[ $output == *"4.26.0"* ]] || false
    [[ $output == *"2252723448"* ]] || false

    run dolt sql -q "select ID, AUTHOR, FILENAME from DATABASECHANGELOG where ID like 'V50.2024-01-04%';"
    [ $status -eq 0 ]
    [[ $output == *"v50.2024-01-04T13:52:51"* ]] || false
    [[ $output == *"noahmoss"* ]] || false
    [[ $output == *"migrations/001_update_migrations.yaml"* ]] || false

    dolt sql -q "alter table DATABASECHANGELOG modify ID varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;"
    run dolt sql -q "select count(1) from DATABASECHANGELOG where ID like 'V50.2024-01-04%';"
    [ $status -eq 0 ]
    [[ $output == *"0"* ]] || false

    stop_sql_server
}
