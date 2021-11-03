#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "dump: dolt dump SQL export with multiple tables" {
    dolt sql -q "CREATE TABLE mysqldump_table(pk int);"
    dolt sql -q "INSERT INTO mysqldump_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"
    dolt sql -q "create table enums (a varchar(10) primary key, b enum('one','two','three'))"
    dolt sql -q "insert into enums values ('abc', 'one'), ('def', 'two')"

    run dolt dump
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f doltdump.sql ]

    run grep INSERT doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 6 ]

    run grep CREATE doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]

    run dolt dump
    [ "$status" -ne 0 ]
    [[ "$output" =~ "doltdump.sql already exists" ]] || false

    run dolt dump -f
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false

    run dolt sql < doltdump.sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows inserted: 6 Rows updated: 0 Rows deleted: 0" ]] || false

}

@test "dump: dolt dump and mysqldump compatibility" {
    dolt sql -q "CREATE TABLE mysqldump_table(pk int);"
    dolt sql -q "INSERT INTO mysqldump_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"

    let PORT="$$ % (65536-1024) + 1024"
    USER="dolt"
    dolt sql-server --host 0.0.0.0 --port=$PORT --user=$USER --loglevel=trace &
    SERVER_PID=$!
    # Give the server a chance to start
    sleep 1

    export MYSQL_PWD=""

    mkdir dumps
    run mysqldump $REPO_NAME -P $PORT -h 0.0.0.0 -u $USER > dumps/mysqldump.sql
    [ "$status" -eq 0 ]
    [ -f dumps/mysqldump.sql ]

    run dolt dump
    [ "$status" -eq 0 ]

    mv doltdump.sql dumps/doltdump.sql
    [ -f dumps/doltdump.sql ]

    cd dumps
    dolt init
    dolt branch dolt_branch

    dolt sql < mysqldump.sql
    dolt add .
    dolt commit --allow-empty -m "create tables from mysqldump"

    dolt checkout dolt_branch
    dolt sql < doltdump.sql
    dolt add .
    dolt commit --allow-empty -m "create tables from doltdump"

    run dolt diff --summary main dolt_branch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]]

    cd ..
    kill $SERVER_PID
}

@test "dump: dolt dump with Indexes" {
    dolt sql -q "CREATE TABLE mysqldump_table(pk int);"
    dolt sql -q "INSERT INTO mysqldump_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"
    dolt sql -q "CREATE TABLE onepk (pk1 BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT);"
    dolt sql -q "CREATE INDEX idx_v1 ON onepk(v1);"
    dolt sql -q "INSERT INTO onepk VALUES (1, 99, 51), (2, 11, 55), (3, 88, 52), (4, 22, 54), (5, 77, 53);"

    run dolt dump
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f doltdump.sql ]

    run dolt sql < doltdump.sql
    [ "$status" -eq 0 ]

    run dolt schema show onepk
    [ "$status" -eq "0" ]
    [[ "$output" =~ 'KEY `idx_v1` (`v1`)' ]] || false
}

@test "dump: dolt dump with foreign key and import" {
    skip "dolt dump foreign key option for import NOT implemented "
    dolt sql -q "CREATE TABLE mysqldump_table(pk int);"
    dolt sql -q "INSERT INTO mysqldump_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"
    dolt sql -q "CREATE TABLE parent (id int PRIMARY KEY, pv1 int, pv2 int, INDEX v1 (pv1), INDEX v2 (pv2));"
    dolt sql -q "CREATE TABLE child (id int primary key, cv1 int, cv2 int, CONSTRAINT fk_named FOREIGN KEY (cv1) REFERENCES parent(pv1));"

    dolt add .
    dolt commit -m "create tables"

    run dolt dump
    [ "$status" -eq 0 ]
    [ -f doltdump.sql ]

    run dolt sql < doltdump.sql
    [ "$status" -eq 0 ]
}

@test "dump: dolt dump with views/trigger" {
    skip "dolt dump views/trigger NOT implemented"
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT);"
    dolt sql -q "CREATE TRIGGER trigger1 BEFORE INSERT ON test FOR EACH ROW SET new.v1 = -new.v1;"
    dolt sql -q "CREATE VIEW view1 AS SELECT v1 FROM test;"
    dolt sql -q "CREATE TABLE a (x INT PRIMARY KEY);"
    dolt sql -q "CREATE TABLE b (y INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1, 1);"
    dolt sql -q "CREATE VIEW view2 AS SELECT y FROM b;"
    dolt sql -q "CREATE TRIGGER trigger2 AFTER INSERT ON a FOR EACH ROW INSERT INTO b VALUES (new.x * 2);"
    dolt sql -q "INSERT INTO a VALUES (2);"
}

@test "dump: dolt dump with keyless tables" {

    dolt sql -q "CREATE TABLE keyless (c0 int, c1 int);"
    dolt sql -q "INSERT INTO keyless VALUES (0,0),(2,2),(1,1),(1,1);"
    dolt sql -q "ALTER TABLE keyless ADD INDEX (c1);"
    dolt sql -q "CREATE TABLE keyless_warehouse(warehouse_id int, warehouse_name longtext);"
    dolt sql -q "INSERT into keyless_warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"

    dolt add .
    dolt commit -m "create tables"

    run dolt dump
    [ "$status" -eq 0 ]
    [ -f doltdump.sql ]

    dolt table rm keyless

    run dolt sql < doltdump.sql
    [ "$status" -eq 0 ]

    run dolt sql -q "UPDATE keyless SET c0 = 4 WHERE c0 = 2;"
    [ $status -eq 0 ]

    run dolt sql -q "SELECT * FROM keyless ORDER BY c0;" -r csv
    [ $status -eq 0 ]
    [[ "${lines[1]}" = "0,0" ]] || false
    [[ "${lines[2]}" = "1,1" ]] || false
    [[ "${lines[3]}" = "1,1" ]] || false
    [[ "${lines[4]}" = "4,2" ]] || false

}
