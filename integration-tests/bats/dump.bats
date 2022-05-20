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

    run dolt sql -b < doltdump.sql
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Rows inserted: 6 Rows updated: 0 Rows deleted: 0" ]] || false
}

@test "dump: SQL type - compare tables in database with tables imported file " {
    dolt branch new_branch
    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"
    if [ "$DOLT_FORMAT_FEATURE_FLAG" != "true" ] ; then
      # V1 storage format does not support keyless tables yet
      dolt sql -q "CREATE TABLE keyless (c0 int, c1 int);"
      dolt sql -q "INSERT INTO keyless VALUES (0,0),(2,2),(1,1),(1,1);"
    fi
    dolt add .
    dolt commit -m "create tables"

    run dolt dump
    [ "$status" -eq 0 ]
    [ -f doltdump.sql ]

    dolt checkout new_branch
    dolt sql < doltdump.sql
    dolt add .
    dolt commit --allow-empty -m "create tables from doltdump"

    skip_nbf_dolt_1
    run dolt diff --summary main new_branch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
}

@test "dump: SQL type (batched) - compare tables in database with tables imported file " {
    dolt branch new_branch

    dolt sql -q "CREATE TABLE new_table(pk int primary key);"
    dolt sql -q "INSERT INTO new_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext);"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"

    if ["$DOLT_FORMAT_FEATURE_FLAG" != true]
    then
      dolt sql -q "CREATE TABLE keyless (c0 int, c1 int);"
      dolt sql -q "INSERT INTO keyless VALUES (0,0),(2,2),(1,1),(1,1);"
    fi

    dolt add .
    dolt commit -m "create tables"

    run dolt dump --batch
    [ "$status" -eq 0 ]
    [ -f doltdump.sql ]

    run cat doltdump.sql
    [[ "$output" =~ "VALUES (1,'UPS'), (2,'TV'), (3,'Table')" ]] || false

    dolt checkout new_branch
    dolt sql < doltdump.sql
    dolt add .
    dolt commit --allow-empty -m "create tables from doltdump"

    skip_nbf_dolt_1
    run dolt diff --summary main new_branch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
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
    skip "dolt dump foreign key option for import NOT implemented"
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

    dolt add .
    dolt commit -m "create tables"

    run dolt dump
    [ "$status" -eq 0 ]
    [ -f doltdump.sql ]

    rm -rf ./.dolt
    dolt init

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
    [[ "$output" =~ 'CREATE VIEW `view1` AS select v1 from test' ]] || false

    run dolt sql -q "show create view view2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'CREATE VIEW `view2` AS select y from b' ]] || false

    run dolt sql -q "show create trigger trigger1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'CREATE TRIGGER `trigger1` before insert on test for each row set new.v1 = -new.v1' ]] || false

    run dolt sql -q "show create trigger trigger2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'CREATE TRIGGER `trigger2` after insert on a for each row insert into b values (new.x * 2)' ]] || false

    run dolt sql -q "show create trigger trigger3"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'CREATE TRIGGER `trigger3` after insert on a for each row follows trigger2 insert into b values (new.x * 2)' ]] || false

    run dolt sql -q "show create trigger trigger4"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'CREATE TRIGGER `trigger4` after insert on a for each row precedes trigger3 insert into b values (new.x * 2)' ]] || false

    run dolt sql -q "show create procedure p1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'CREATE PROCEDURE `p1` (in x int) select x from dual' ]] || false
}

@test "dump: SQL type - with keyless tables" {
    skip_nbf_dolt_1

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
    [ "${#lines[@]}" -eq 2 ]

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
    [ "${#lines[@]}" -eq 6 ]

    run grep CREATE dumpfile.sql
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
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

@test "dump: CSV type - compare tables in database with tables imported from corresponding files " {
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

    skip_nbf_dolt_1
    run dolt diff --summary main new_branch
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

    skip_nbf_dolt_1
    run dolt diff --summary main new_branch
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

@test "dump: JSON type - compare tables in database with tables imported from corresponding files " {
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

    skip_nbf_dolt_1
    import_tables "json"
    dolt add .
    dolt commit --allow-empty -m "create tables from doltdump"

    run dolt diff --summary main new_branch
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

    skip_nbf_dolt_1
    dolt add .
    dolt commit --allow-empty -m "create tables from doltdump"

    run dolt diff --summary main new_branch
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

    run dolt diff --summary main new_branch
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
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
