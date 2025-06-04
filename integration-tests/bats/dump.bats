#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "dump: no tables" {
    run dolt dump
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No tables to export." ]] || false
}

@test "dump: roundtrip on database with leading space character and hyphen" {
    mkdir ' test-db'
    cd ' test-db'
    dolt init
    create_tables
    insert_data_into_tables

    run dolt dump
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f doltdump.sql ]

    mkdir roundtrip
    cd roundtrip
    dolt init

    dolt sql < ../doltdump.sql
    run dolt sql -q "show databases"
    [ $status -eq 0 ]
    [[ $output =~ "|  test-db" ]] || false
}

@test "dump: SQL type - with multiple tables" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
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
    [ "${#lines[@]}" -eq 3 ]

    run grep CREATE doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]

    run grep "DATABASE IF NOT EXISTS" doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    run grep FOREIGN_KEY_CHECKS=0 doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    run grep UNIQUE_CHECKS=0 doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    # Sanity check
    run grep AUTOCOMMIT doltdump.sql
    [ "$status" -eq 1 ]

    run dolt dump
    [ "$status" -ne 0 ]
    [[ "$output" =~ "doltdump.sql already exists" ]] || false

    run dolt dump -f
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false

    dolt sql -b < doltdump.sql

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "warehouse" ]] || false
    [[ "$output" =~ "enums" ]] || false
    [[ "$output" =~ "new_table" ]] || false

    run dolt sql -q "select * from warehouse" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "$output" =~ "1,UPS" ]] || false
    [[ "$output" =~ "2,TV" ]] || false
    [[ "$output" =~ "3,Table" ]] || false
}

@test "dump: SQL type - no-create-db flag" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"
    dolt sql -q "create table enums (a varchar(10) primary key, b enum('one','two','three'))"
    dolt sql -q "insert into enums values ('abc', 'one'), ('def', 'two')"

    run dolt dump --no-create-db
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f doltdump.sql ]

    run grep "CREATE DATABASE" doltdump.sql
    [ "$status" -eq 1 ]
}

@test "dump: SQL type - database name is reserved word/keyword" {
    dolt sql -q "CREATE DATABASE \`interval\`;"
    cd interval
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"
    dolt sql -q "create table enums (a varchar(10) primary key, b enum('one','two','three'))"
    dolt sql -q "insert into enums values ('abc', 'one'), ('def', 'two')"

    run dolt dump
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f doltdump.sql ]

    run grep "CREATE DATABASE IF NOT EXISTS \`interval\`" doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    dolt sql -b < doltdump.sql

    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "warehouse" ]] || false
    [[ "$output" =~ "enums" ]] || false
    [[ "$output" =~ "new_table" ]] || false

    run dolt sql -q "select * from warehouse" -r csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "$output" =~ "1,UPS" ]] || false
    [[ "$output" =~ "2,TV" ]] || false
    [[ "$output" =~ "3,Table" ]] || false
}

@test "dump: SQL type - compare tables in database with tables imported file" {
    dolt branch new_branch
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"
    dolt sql -q "CREATE TABLE keyless (c0 int, c1 int);"
    dolt sql -q "INSERT INTO keyless VALUES (0,0),(2,2),(1,1),(1,1);"
    dolt add .
    dolt commit -m "create tables"

    run dolt dump
    [ "$status" -eq 0 ]
    [ -f doltdump.sql ]

    dolt checkout new_branch
    dolt sql < doltdump.sql
    dolt add .
    dolt commit --allow-empty -m "create tables from doltdump"

    run dolt diff --stat main new_branch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "dump: SQL type (no-batch) - compare tables in database with tables imported file" {
    dolt branch new_branch

    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"

    dolt sql -q "CREATE TABLE keyless (c0 int, c1 int);"
    dolt sql -q "INSERT INTO keyless VALUES (0,0),(2,2),(1,1),(1,1);"

    dolt add .
    dolt commit -m "create tables"

    run dolt dump --no-batch
    [ "$status" -eq 0 ]
    [ -f doltdump.sql ]

    run cat doltdump.sql
    [[ "$output" =~ "VALUES (1,'UPS');" ]] || false

    dolt checkout new_branch
    dolt sql < doltdump.sql
    dolt add .
    dolt commit --allow-empty -m "create tables from doltdump"

    run dolt diff --stat main new_branch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "dump: SQL type (batch is no-op) - compare tables in database with tables imported file" {
  dolt branch new_branch

    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"

    dolt add .
    dolt commit -m "create tables"

    run dolt dump
    [ "$status" -eq 0 ]
    [ -f doltdump.sql ]

    run cat doltdump.sql
    [[ "$output" =~ "VALUES (1,'UPS'), (2,'TV'), (3,'Table')" ]] || false

    run dolt dump -f --batch
    [ "$status" -eq 0 ]
    [ -f doltdump.sql ]

    run cat doltdump.sql
    [[ "$output" =~ "VALUES (1,'UPS'), (2,'TV'), (3,'Table')" ]] || false
}

@test "dump: SQL type - with Indexes" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
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

@test "dump: SQL type - with foreign key and import" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
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

@test "dump: SQL type - with views/triggers and procedures" {
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT);"
    dolt sql -q "CREATE TRIGGER trigger1 BEFORE INSERT ON test FOR EACH ROW SET new.v1 = -new.v1;"
    dolt sql -q "CREATE VIEW view1 AS SELECT v1 FROM test;"
    dolt sql -q "CREATE TABLE a (x INT PRIMARY KEY);"
    dolt sql -q "CREATE TABLE b (y INT PRIMARY KEY);"
    dolt sql -q "INSERT INTO test VALUES (1, 1);"
    dolt sql -q "CREATE VIEW view2 AS SELECT y FROM b;"
    dolt sql -q "CREATE TRIGGER trigger2 AFTER INSERT ON a FOR EACH ROW INSERT INTO b VALUES (new.x * 2);"
    dolt sql -q "INSERT INTO a VALUES (2);"
    dolt sql -q "CREATE TRIGGER trigger3 AFTER INSERT ON a FOR EACH ROW FOLLOWS trigger2 INSERT INTO b VALUES (new.x * 2);"
    dolt sql -q "CREATE TRIGGER trigger4 AFTER INSERT ON a FOR EACH ROW PRECEDES trigger3 INSERT INTO b VALUES (new.x * 2);"
    dolt sql -q "CREATE PROCEDURE p1 (in x int) select x from dual"
    dolt sql <<SQL
delimiter //
CREATE PROCEDURE dorepeat(p1 INT)
       BEGIN
          SET @x = 0;
          REPEAT SET @x = @x + 1; UNTIL @x > p1 END REPEAT;
       END
//
SQL

    dolt sql <<SQL
delimiter //
CREATE PROCEDURE dorepeat2(p2 INT)
       BEGIN
          SET @x = 0;
          REPEAT SET @x = @x + 1; UNTIL @x > p2 END REPEAT;
       END
//
SQL

    # decoy database in this directory to make sure we export the correct database's triggers etc.
    dolt sql -q "create database aadecoy"
    
    dolt add .
    dolt commit -m "create tables"

    run dolt dump --no-batch
    [ "$status" -eq 0 ]
    [ -f doltdump.sql ]

    rm -rf ./.dolt
    dolt init

    # We should not have literally dumped the dolt_schemas table, but equivalent DDL statements
    run grep dolt_schemas doltdump.sql
    [ "$status" -ne 0 ]
    [ "${#lines[@]}" -eq 0 ]
    
    run dolt sql < doltdump.sql
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT * from test"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "-1" ]] || false

    run dolt sql -q "SELECT * from a"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    run dolt sql -q "SELECT * from b"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "4" ]] || false

    run dolt sql -q "select * from view1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "-1" ]] || false

    run dolt sql -q "select * from view2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "4" ]] || false

    run dolt sql -q "show create view view1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'AS SELECT v1 FROM test' ]] || false

    run dolt sql -q "show create view view2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'AS SELECT y FROM b' ]] || false

    run dolt sql -q "show create trigger trigger1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'BEFORE INSERT ON test FOR EACH ROW SET new.v1 = -new.v1' ]] || false

    run dolt sql -q "show create trigger trigger2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'AFTER INSERT ON a FOR EACH ROW INSERT INTO b VALUES (new.x * 2)' ]] || false

    run dolt sql -q "show create trigger trigger3"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'AFTER INSERT ON a FOR EACH ROW FOLLOWS trigger2 INSERT INTO b VALUES (new.x * 2)' ]] || false

    run dolt sql -q "show create trigger trigger4"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'AFTER INSERT ON a FOR EACH ROW PRECEDES trigger3 INSERT INTO b VALUES (new.x * 2)' ]] || false

    run dolt sql -q "show create procedure p1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'CREATE PROCEDURE p1 (in x int) select x' ]] || false

    run dolt sql -q "show create procedure dorepeat"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'CREATE PROCEDURE dorepeat(p1' ]] || false

    run dolt sql -q "show create procedure dorepeat2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'CREATE PROCEDURE dorepeat2(p2' ]] || false
}

@test "dump: SQL type - with keyless tables" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"
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

@test "dump: SQL type - with empty tables" {
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 BIGINT);"

    dolt add .
    dolt commit -m "create tables"

    run dolt dump
    [ "$status" -eq 0 ]
    [ -f doltdump.sql ]

    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"

    run dolt sql < doltdump.sql
    [ "$status" -eq 0 ]

    run grep CREATE doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]

    run grep INSERT doltdump.sql
    [ "$status" -eq 1 ]
    [ "${#lines[@]}" -eq 0 ]
}

@test "dump: SQL type - with custom filename specified" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"
    dolt sql -q "create table enums (a varchar(10) primary key, b enum('one','two','three'))"
    dolt sql -q "insert into enums values ('abc', 'one'), ('def', 'two')"

    run dolt dump --file-name dumpfile.sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f dumpfile.sql ]

    run grep INSERT dumpfile.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]

    run grep CREATE dumpfile.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
}

@test "dump: SQL type - with directory name given" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    run dolt dump --directory dumps
    [ "$status" -eq 1 ]
    [[ "$output" =~ "directory is not supported for sql exports" ]] || false
    [ ! -f dumpfile.sql ]
}

@test "dump: SQL type - with both filename and directory name given" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    run dolt dump --file-name dumpfile.sql --directory dumps
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot pass both directory and file names" ]] || false
    [ ! -f dumpfile.sql ]
}

@test "dump: CSV type - with multiple tables and check -f flag" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"
    dolt sql -q "create table enums (a varchar(10) primary key, b enum('one','two','three'))"
    dolt sql -q "insert into enums values ('abc', 'one'), ('def', 'two')"

    run dolt dump -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f doltdump/enums.csv ]
    [ -f doltdump/new_table.csv ]
    [ -f doltdump/warehouse.csv ]

    run dolt dump -r csv
    [ "$status" -ne 0 ]
    [[ "$output" =~ "enums.csv already exists" ]] || false

    rm doltdump/enums.csv
    run dolt dump -r csv
    [ "$status" -ne 0 ]
    [[ "$output" =~ "new_table.csv already exists" ]] || false

    rm doltdump/enums.csv
    rm doltdump/new_table.csv
    run dolt dump -r csv
    [ "$status" -ne 0 ]
    [[ "$output" =~ "warehouse.csv already exists" ]] || false

    run dolt dump -f -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f doltdump/enums.csv ]
    [ -f doltdump/new_table.csv ]
    [ -f doltdump/warehouse.csv ]
}

@test "dump: CSV type - compare tables in database with tables imported from corresponding files" {
    create_tables

    dolt add .
    dolt commit -m "create tables"

    dolt branch new_branch

    insert_data_into_tables

    dolt add .
    dolt commit -m "insert to tables"

    run dolt dump -r csv
    [ "$status" -eq 0 ]
    check_for_files "csv"

    dolt checkout new_branch

    import_tables "csv"
    dolt add .
    dolt commit --allow-empty -m "create tables from doltdump"

    run dolt diff --stat main new_branch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "dump: CSV type - with empty tables" {
    dolt branch new_branch

    create_tables

    dolt add .
    dolt commit -m "create tables"

    run dolt dump -r csv
    [ "$status" -eq 0 ]
    check_for_files "csv"

    dolt checkout new_branch

    create_tables

    import_tables "csv"

    dolt add .
    dolt commit --allow-empty -m "create tables from doltdump"

    run dolt diff --stat main new_branch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "dump: CSV type - with custom directory name specified" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"
    dolt sql -q "create table enums (a varchar(10) primary key, b enum('one','two','three'))"
    dolt sql -q "insert into enums values ('abc', 'one'), ('def', 'two')"

    run dolt dump -r csv --directory dumps
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f dumps/enums.csv ]
    [ -f dumps/new_table.csv ]
    [ -f dumps/warehouse.csv ]

    run dolt dump -r csv --directory dumps
    [ "$status" -ne 0 ]
    [[ "$output" =~ "enums.csv already exists" ]] || false

    rm dumps/enums.csv
    run dolt dump -r csv --directory dumps
    [ "$status" -ne 0 ]
    [[ "$output" =~ "new_table.csv already exists" ]] || false

    rm dumps/enums.csv
    rm dumps/new_table.csv
    run dolt dump -r csv --directory dumps
    [ "$status" -ne 0 ]
    [[ "$output" =~ "warehouse.csv already exists" ]] || false

    run dolt dump -f -r csv --directory dumps
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f dumps/enums.csv ]
    [ -f dumps/new_table.csv ]
    [ -f dumps/warehouse.csv ]
}

@test "dump: CSV type - with filename given" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    run dolt dump -r .csv --file-name dumpfile.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "file-name is not supported for csv exports" ]] || false
    [ ! -f dumps/enums.csv ]
    [ ! -f dumps/new_table.csv ]
    [ ! -f dumps/warehouse.csv ]
}

@test "dump: JSON type - with multiple tables and check -f flag" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name varchar(100));"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"
    dolt sql -q "create table enums (a varchar(10) primary key, b enum('one','two','three'))"
    dolt sql -q "insert into enums values ('abc', 'one'), ('def', 'two')"

    run dolt dump -r json
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f doltdump/enums.json ]
    [ -f doltdump/new_table.json ]
    [ -f doltdump/warehouse.json ]

    run dolt dump -r json
    [ "$status" -ne 0 ]
    [[ "$output" =~ "enums.json already exists" ]] || false

    rm doltdump/enums.json
    run dolt dump -r json
    [ "$status" -ne 0 ]
    [[ "$output" =~ "new_table.json already exists" ]] || false

    rm doltdump/enums.json
    rm doltdump/new_table.json
    run dolt dump -r json
    [ "$status" -ne 0 ]
    [[ "$output" =~ "warehouse.json already exists" ]] || false

    run dolt dump -f -r json
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f doltdump/enums.json ]
    [ -f doltdump/new_table.json ]
    [ -f doltdump/warehouse.json ]
}

@test "dump: JSON type - compare tables in database with tables imported from corresponding files" {
    create_tables

    dolt add .
    dolt commit -m "create tables"

    dolt branch new_branch

    insert_data_into_tables

    dolt add .
    dolt commit -m "insert to tables"

    run dolt dump -r json
    [ "$status" -eq 0 ]
    check_for_files "json"

    dolt checkout new_branch

    import_tables "json"
    dolt add .
    dolt commit --allow-empty -m "create tables from doltdump"

    run dolt diff --stat main new_branch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "dump: JSON type - with empty tables" {
    dolt branch new_branch

    create_tables

    dolt add .
    dolt commit -m "create tables"

    run dolt dump -r json
    check_for_files "json"

    dolt checkout new_branch

    create_tables

    import_tables "json"

    dolt add .
    dolt commit --allow-empty -m "create tables from doltdump"

    run dolt diff --stat main new_branch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "dump: JSON type - with custom directory name specified" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"
    dolt sql -q "create table enums (a varchar(10) primary key, b enum('one','two','three'))"
    dolt sql -q "insert into enums values ('abc', 'one'), ('def', 'two')"

    run dolt dump -r json --directory dumps
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f dumps/enums.json ]
    [ -f dumps/new_table.json ]
    [ -f dumps/warehouse.json ]

    run dolt dump -r json --directory dumps
    [ "$status" -ne 0 ]
    [[ "$output" =~ "enums.json already exists" ]] || false

    rm dumps/enums.json
    run dolt dump -r json --directory dumps
    [ "$status" -ne 0 ]
    [[ "$output" =~ "new_table.json already exists" ]] || false

    rm dumps/enums.json
    rm dumps/new_table.json
    run dolt dump -r json --directory dumps
    [ "$status" -ne 0 ]
    [[ "$output" =~ "warehouse.json already exists" ]] || false

    run dolt dump -f -r json --directory dumps
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f dumps/enums.json ]
    [ -f dumps/new_table.json ]
    [ -f dumps/warehouse.json ]
}

@test "dump: JSON type - with filename name given" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    run dolt dump -r json --file-name dumpfile.json
    [ "$status" -eq 1 ]
    [[ "$output" =~ "file-name is not supported for json exports" ]] || false
    [ ! -f dumps/enums.json ]
    [ ! -f dumps/new_table.json ]
    [ ! -f dumps/warehouse.json ]
}

@test "dump: dump with schema-only flag" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1), (2);"
    run dolt dump --schema-only
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f doltdump_schema_only.sql ]

    run grep 'CREATE TABLE' doltdump_schema_only.sql
    [ "${#lines[@]}" -eq 1 ]

    run grep 'INSERT' doltdump_schema_only.sql
    [ "${#lines[@]}" -eq 0 ]
}

@test "dump: dump with schema-only flag errors with non-sql output file" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1), (2);"
    run dolt dump --schema-only -r csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "schema-only dump is not supported for csv exports" ]] || false
    [ ! -f doltdump_schema_only.csv ]
}

@test "dump: JSON type - export tables with types, longtext and blob" {
    skip "export table in json with these types not working"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "CREATE TABLE enums (a varchar(10) primary key, b enum('one','two','three'))"
    dolt add .
    dolt commit -m "create tables"

    dolt branch new_branch

    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"
    dolt sql -q "INSERT into enums VALUES ('abc', 'one'), ('def', 'two')"
    dolt add .
    dolt commit -m "insert rows to tables"

    run dolt dump -r json
    [ "$status" -eq 0 ]
    [ -f doltdump/enums.json ]
    [ -f doltdump/warehouse.json ]

    dolt checkout new_branch

    dolt table import -r enums doltdump/enums.json
    dolt table import -r warehouse doltdump/warehouse.json
    dolt add .
    dolt commit --allow-empty -m "create tables from dump files"

    run dolt diff --stat main new_branch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "dump: -na flag works correctly" {
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_Table values (1)"

    run dolt dump -na
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f doltdump.sql ]

    run head -n 3 doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
    [[ "$output" =~ "SET FOREIGN_KEY_CHECKS=0;" ]] || false
    [[ "$output" =~ "SET UNIQUE_CHECKS=0;" ]] || false

    run grep AUTOCOMMIT doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]

    run tail -n 2 doltdump.sql
    [[ "$output" =~ "COMMIT;" ]] || false

    dolt sql < doltdump.sql

    run dolt sql -r csv -q "select * from new_table"
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" = "pk" ]] || false
    [[ "${lines[1]}" = "1" ]] || false

    # try with a csv output and ensure that there are no problems
    run dolt dump -r csv -na
    [ "$status" -eq 0 ]
    [ -f doltdump/new_table.csv ]
}

@test "dump: --no-autocommit flag works with multiple tables" {
    dolt sql -q "CREATE TABLE table1(pk int primary key);"
    dolt sql -q "CREATE TABLE table2(pk int primary key);"

    dolt sql -q "INSERT INTO table1 VALUES (1)"
    dolt sql -q "INSERT INTO table2 VALUES (1)"

    run dolt dump --no-autocommit
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f doltdump.sql ]

    run grep AUTOCOMMIT doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]

    run grep "COMMIT;" doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]

    # test with batch mode
    run dolt dump --batch -f --no-autocommit
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully exported data." ]] || false
    [ -f doltdump.sql ]

    run grep AUTOCOMMIT doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]

    run grep "COMMIT;" doltdump.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
}

# Assert that we can create data in ANSI_QUOTES mode, and then correctly dump it
# out after disabling ANSI_QUOTES mode.
@test "dump: ANSI_QUOTES data" {
    dolt sql << SQL
SET @@SQL_MODE=ANSI_QUOTES;
CREATE TABLE "table1"("pk" int primary key, "col1" int DEFAULT ("pk"));
CREATE TRIGGER trigger1 BEFORE INSERT ON "table1" FOR EACH ROW SET NEW."pk" = NEW."pk" + 1;
INSERT INTO "table1" ("pk") VALUES (1);
CREATE VIEW "view1" AS select "pk", "col1" from "table1";
CREATE PROCEDURE procedure1() SELECT "pk", "col1" from "table1";
SQL

    run dolt dump
    [ $status -eq 0 ]
    [[ $output =~ "Successfully exported data." ]] || false
    [ -f doltdump.sql ]
    cp doltdump.sql ~/doltdump.sql

    mkdir roundtrip
    cd roundtrip
    dolt init

    dolt sql < ../doltdump.sql
    [ $status -eq 0 ]

    run dolt sql -q "USE \`dolt-repo-$$\`; SHOW TABLES;"
    [ $status -eq 0 ]
    [[ $output =~ "table1" ]] || false
    [[ $output =~ "view1" ]] || false

    run dolt sql -r csv -q "USE \`dolt-repo-$$\`; CALL procedure1;"
    [ $status -eq 0 ]
    [[ $output =~ "pk,col1" ]] || false
    [[ $output =~ "2,1" ]] || false
}

@test "dump: round trip dolt dump with all data types" {
    skip "export table in bit, binary and blob types not working"
    # table with all data types with one all null values row and one all non-null values row
    run dolt sql << SQL
CREATE TABLE \`all_types\` (
  \`pk\` int NOT NULL,
  \`v1\` binary(1) DEFAULT NULL,
  \`v2\` bigint DEFAULT NULL,
  \`v3\` bit(1) DEFAULT NULL,
  \`v4\` blob,
  \`v5\` char(1) DEFAULT NULL,
  \`v6\` date DEFAULT NULL,
  \`v7\` datetime DEFAULT NULL,
  \`v8\` decimal(5,2) DEFAULT NULL,
  \`v9\` double DEFAULT NULL,
  \`v10\` enum('s','m','l') DEFAULT NULL,
  \`v11\` float DEFAULT NULL,
  \`v12\` geometry DEFAULT NULL,
  \`v13\` int DEFAULT NULL,
  \`v14\` json DEFAULT NULL,
  \`v15\` linestring DEFAULT NULL,
  \`v16\` longblob,
  \`v17\` longtext,
  \`v18\` mediumblob,
  \`v19\` mediumint DEFAULT NULL,
  \`v20\` mediumtext,
  \`v21\` point DEFAULT NULL,
  \`v22\` polygon DEFAULT NULL,
  \`v23\` set('one','two') DEFAULT NULL,
  \`v24\` smallint DEFAULT NULL,
  \`v25\` text,
  \`v26\` time(6) DEFAULT NULL,
  \`v27\` timestamp DEFAULT NULL,
  \`v28\` tinyblob,
  \`v29\` tinyint DEFAULT NULL,
  \`v30\` tinytext,
  \`v31\` varchar(255) DEFAULT NULL,
  \`v32\` varbinary(255) DEFAULT NULL,
  \`v33\` year DEFAULT NULL,
  PRIMARY KEY (\`pk\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
INSERT INTO \`all_types\` (\`pk\`,\`v1\`,\`v2\`,\`v3\`,\`v4\`,\`v5\`,\`v6\`,\`v7\`,\`v8\`,\`v9\`,\`v10\`,\`v11\`,\`v12\`,\`v13\`,\`v14\`,\`v15\`,\`v16\`,\`v17\`,\`v18\`,\`v19\`,\`v20\`,\`v21\`,\`v22\`,\`v23\`,\`v24\`,\`v25\`,\`v26\`,\`v27\`,\`v28\`,\`v29\`,\`v30\`,\`v31\`,\`v32\`,\`v33\`) VALUES
(1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'null',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
(2, 1, 1, 1 ,('abc'),'a','2022-04-05','2022-10-05 10:14:41',2.34,2.34,'s',2.34,POINT(1,2),1,'{"a":1}',LINESTRING(POINT(0,0),POINT(1,2)),('abcd'),'abcd',('ab'),1,'abc',POINT(2,1),polygon(linestring(point(1,2),point(3,4),point(5,6),point(1,2))),'one',1,'abc','10:14:41','2022-10-05 10:14:41',('a'),1,'a','abcde',1,2022);
SQL
    [ "$status" -eq 0 ]

    dolt dump

    cat doltdump.sql

    dolt sql < doltdump.sql
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT ST_AsText(v12), ST_AsText(v21), ST_AsText(v15), ST_AsText(v22) from t1;"
    [[ "$output" =~ "POINT(1 2)   | POINT(2 1)   | LINESTRING(0 0,1 2) | POLYGON((1 2,3 4,5 6,1 2))" ]] || false

    # need to test binary, bit and blob types
}

function create_tables() {
  dolt sql -q "CREATE TABLE new_table(pk int primary key);"
  dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name varchar(100));"

  if [ "$DOLT_FORMAT_FEATURE_FLAG" != "true" ]
  then
    dolt sql -q "CREATE TABLE keyless (c0 int, c1 int);"
  fi
}

function insert_data_into_tables() {
  dolt sql -q "INSERT INTO new_table VALUES (1);"
  dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"

  if [ "$DOLT_FORMAT_FEATURE_FLAG" != "true" ]
  then
    dolt sql -q "INSERT INTO keyless VALUES (0,0),(2,2),(1,1),(1,1);"
  fi
}

function check_for_files() {
  [ -f "doltdump/new_table.$1" ]
  [ -f "doltdump/warehouse.$1" ]
  if [ "$DOLT_FORMAT_FEATURE_FLAG" != "true" ]
  then
    [ -f "doltdump/keyless.$1" ]
  fi
}

function import_tables() {
  dolt table import -r new_table "doltdump/new_table.$1"
  dolt table import -r warehouse "doltdump/warehouse.$1"
  if [ "$DOLT_FORMAT_FEATURE_FLAG" != "true" ]
  then
    dolt table import -r keyless "doltdump/keyless.$1"
  fi
}
