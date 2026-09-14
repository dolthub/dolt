#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    dolt sql <<SQL
create table t (i int, j int, k int);
insert into t (i, j, k) values (1, 2, 3), (4, 5, 6), (7, 8, 9);
create table emptytbl (i int, j int, k int);
SQL
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-export: basic outfile" {
    run dolt sql -q "select * from t order by i, j, k into outfile './outfile.out';"
    [ "$status" -eq 0 ]
    [ -f outfile.out ]

    run cat outfile.out
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" = "3" ]] || false
    [[ "${lines[0]}" =~ "1	2	3" ]] || false
    [[ "${lines[1]}" =~ "4	5	6" ]] || false
    [[ "${lines[2]}" =~ "7	8	9" ]] || false
}

@test "sql-export: basic dumpfile" {
    run dolt sql -q "select * from t order by i, j, k limit 1 into dumpfile './dumpfile.out';"
    [ "$status" -eq 0 ]
    [ -f dumpfile.out ]

    run cat dumpfile.out
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" = "1" ]] || false
    [[ "${lines[0]}" =~ "123" ]] || false
}

@test "sql-export: test abs path outfile" {
    CURR_DIR=$(pwd)
    run dolt sql -q "select * from t order by i, j, k into outfile '$CURR_DIR/outfile.out';"
    [ "$status" -eq 0 ]
    [ -f outfile.out ]

    run cat outfile.out
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" = "3" ]] || false
    [[ "${lines[0]}" =~ "1	2	3" ]] || false
    [[ "${lines[1]}" =~ "4	5	6" ]] || false
    [[ "${lines[2]}" =~ "7	8	9" ]] || false
}

@test "sql-export: test abs path dumpfile" {
    CURR_DIR=$(pwd)
    run dolt sql -q "select * from t order by i, j, k limit 1 into dumpfile '$CURR_DIR/dumpfile.out';"
    [ "$status" -eq 0 ]
    [ -f dumpfile.out ]

    run cat dumpfile.out
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" = "1" ]] || false
    [[ "${lines[0]}" =~ "123" ]] || false
}

@test "sql-export: empty outfile" {
    run dolt sql -q "select * from emptytbl into outfile './outfile.out';"
    [ "$status" -eq 0 ]
    [ -f outfile.out ]

    run cat outfile.out
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" = "0" ]] || false
}

@test "sql-export: empty dumpfile" {
    run dolt sql -q "select * from emptytbl into dumpfile './dumpfile.out';"
    [ "$status" -eq 0 ]
    [ -f dumpfile.out ]

    run cat dumpfile.out
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" = "0" ]] || false
}

@test "sql-export: fails with existing files" {
    touch exists.out
    run dolt sql -q "select * from t order by i, j, k into outfile './exists.out';"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "already exists" ]] || false

    run dolt sql -q "select * from t order by i, j, k limit 1 into dumpfile './exists.out';"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "already exists" ]] || false
}

@test "sql-export: dumpfile fails with too many rows" {
    run dolt sql -q "select * from t order by i, j, k limit 3 into dumpfile './dumpfile.out';"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Result consisted of more than one row" ]] || false
}

@test "sql-export: bit union csv output regression test for dolt#9641" {
    dolt sql -q "CREATE TABLE collection (id INT, archived BIT(1));"
    dolt sql -q "INSERT INTO collection VALUES (1, b'0'), (2, b'0');"
    run dolt sql --result-format=csv -q "SELECT * FROM (SELECT archived FROM collection WHERE archived = FALSE UNION ALL SELECT NULL AS archived FROM collection WHERE id = 1) AS dummy_alias;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "archived" ]] || false
}

@test "sql-export: dump and reload a populated virtual column table" {
    # https://github.com/dolthub/dolt/issues/8325
    run dolt sql -r csv <<'SQL'
CREATE TABLE virtual_dump(pk INT PRIMARY KEY,g INT AS(pk*pk)); INSERT INTO virtual_dump(pk) VALUES(1),(3);
SQL
    [ "$status" -eq 0 ]
    dolt dump --no-create-db -f -fn virtual_dump.sql
    dolt sql -q "DROP TABLE virtual_dump"
    dolt sql < virtual_dump.sql
    run dolt sql -r csv <<'SQL'
SELECT * FROM virtual_dump ORDER BY pk;
SQL
    [ "$status" -eq 0 ]
    [ "$output" = $'pk,g\n1,1\n3,9' ]
}

@test "sql-export: CSV round trip distinguishes empty strings from NULL" {
    # https://github.com/dolthub/dolt/issues/8388
    run dolt sql -r csv <<'SQL'
CREATE TABLE empty_string_export(pk INT PRIMARY KEY,v VARBINARY(255)); INSERT INTO empty_string_export VALUES(1,''),(2,NULL);
SQL
    [ "$status" -eq 0 ]
    dolt table export empty_string_export empty_string_export.csv
    dolt table import -c empty_string_copy empty_string_export.csv
    run dolt sql -r csv <<'SQL'
SELECT pk,IF(v IS NULL,1,0) AS is_null FROM empty_string_copy ORDER BY pk;
SQL
    [ "$status" -eq 0 ]
    [ "$output" = $'pk,is_null\n1,0\n2,1' ]
}

@test "sql-export: CSV round trip preserves embedded line breaks" {
    # https://github.com/dolthub/dolt/issues/8389
    run dolt sql -r csv <<'SQL'
CREATE TABLE newline_export(a INT PRIMARY KEY,b VARBINARY(255)); INSERT INTO newline_export VALUES(1,'line\nbreak'),(2,'sorry\ncsv');
SQL
    [ "$status" -eq 0 ]
    dolt table export newline_export newline_export.csv
    dolt table import -c newline_copy newline_export.csv
    run dolt sql -r csv <<'SQL'
SELECT a,HEX(b) AS bytes FROM newline_copy ORDER BY a;
SQL
    [ "$status" -eq 0 ]
    [ "$output" = $'a,bytes\n1,6C696E650A627265616B\n2,736F7272790A637376' ]
}
