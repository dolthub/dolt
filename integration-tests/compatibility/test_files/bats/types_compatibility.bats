#!/usr/bin/env bats
# Tests that the current Dolt build can read, write, and alter tables containing
# every supported MySQL type that were originally created by an older Dolt version.
# Special emphasis on TEXT and BLOB variants, which may be stored out-of-band.
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    cp -Rpf $REPO_DIR bats_repo
    cd bats_repo
}

teardown() {
    teardown_common
    cd ..
    rm -rf bats_repo
}

# ---------------------------------------------------------------------------
# Basic read tests: verify current Dolt can read all type columns written by
# old Dolt without error or data corruption.
# ---------------------------------------------------------------------------

@test "types_compatibility: all_types table has expected row count" {
    run dolt sql -q "SELECT count(*) FROM all_types;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3" ]] || false
}

@test "types_compatibility: integer columns readable from old dolt" {
    run dolt sql -q "SELECT pk, c_tinyint, c_smallint, c_mediumint, c_int, c_bigint, c_bigint_u FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,100,1000,100000,2000000,9223372036854775807,18446744073709551615" ]] || false
}

@test "types_compatibility: negative integer columns readable from old dolt" {
    run dolt sql -q "SELECT pk, c_tinyint, c_smallint, c_mediumint, c_int, c_bigint, c_bigint_u FROM all_types WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,-100,-1000,-100000,-2000000,-9223372036854775807,0" ]] || false
}

@test "types_compatibility: float and decimal columns readable from old dolt" {
    run dolt sql -q "SELECT pk, c_float, c_double, c_decimal FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,1.5,2.5,12345.67" ]] || false
}

@test "types_compatibility: negative float and decimal columns readable from old dolt" {
    run dolt sql -q "SELECT pk, c_float, c_double, c_decimal FROM all_types WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,-1.5,-2.5,-12345.67" ]] || false
}

@test "types_compatibility: char and varchar columns readable from old dolt" {
    run dolt sql -q "SELECT pk, c_char, c_varchar FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,hello,hello world" ]] || false
}

@test "types_compatibility: tinytext column readable from old dolt" {
    run dolt sql -q "SELECT pk, c_tinytext FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,tinytext val" ]] || false
}

@test "types_compatibility: text column readable from old dolt" {
    run dolt sql -q "SELECT pk, c_text FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,text val" ]] || false
}

@test "types_compatibility: mediumtext column readable from old dolt" {
    run dolt sql -q "SELECT pk, c_mediumtext FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,mediumtext val" ]] || false
}

@test "types_compatibility: longtext column readable from old dolt" {
    run dolt sql -q "SELECT pk, c_longtext FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,longtext val" ]] || false
}

@test "types_compatibility: large text values readable from old dolt" {
    run dolt sql -q "SELECT pk, LENGTH(c_text), LENGTH(c_mediumtext), LENGTH(c_longtext) FROM all_types WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3,500,500,500" ]] || false
}

@test "types_compatibility: varbinary column readable from old dolt" {
    run dolt sql -q "SELECT pk, c_varbinary FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,varbinary val" ]] || false
}

@test "types_compatibility: tinyblob column readable from old dolt" {
    run dolt sql -q "SELECT pk, c_tinyblob FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,tinyblob val" ]] || false
}

@test "types_compatibility: blob column readable from old dolt" {
    run dolt sql -q "SELECT pk, c_blob FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,blob val" ]] || false
}

@test "types_compatibility: mediumblob column readable from old dolt" {
    run dolt sql -q "SELECT pk, c_mediumblob FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,mediumblob val" ]] || false
}

@test "types_compatibility: longblob column readable from old dolt" {
    run dolt sql -q "SELECT pk, c_longblob FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,longblob val" ]] || false
}

@test "types_compatibility: large blob values readable from old dolt" {
    run dolt sql -q "SELECT pk, LENGTH(c_blob), LENGTH(c_mediumblob), LENGTH(c_longblob) FROM all_types WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3,500,500,500" ]] || false
}

@test "types_compatibility: date and time columns readable from old dolt" {
    run dolt sql -q "SELECT pk, c_date, c_time, c_year FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,2024-01-15,13:30:45,2024" ]] || false
}

@test "types_compatibility: datetime column readable from old dolt" {
    run dolt sql -q "SELECT pk, c_datetime FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,2024-01-15 13:30:45" ]] || false
}

@test "types_compatibility: timestamp column readable from old dolt" {
    dolt sql -q "SELECT pk, c_timestamp IS NOT NULL FROM all_types WHERE pk=1;" -r csv
    run dolt sql -q "SELECT pk, c_timestamp IS NOT NULL FROM all_types WHERE pk=1;" -r csv
    # note that dolt SQL shell uses "true" and "false" for boolean results, instead of 1 and 0
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,true" ]] || false

    run dolt sql -q "SELECT pk, c_timestamp IS NULL FROM all_types WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,true" ]] || false
}

@test "types_compatibility: json column readable from old dolt" {
    [[ "$DOLT_VERSION" =~ 0\.50 ]] && skip "JSON type test not run for Dolt version 0.50"
    run dolt sql -q "SELECT pk, JSON_EXTRACT(c_json, '$.k') FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "v" ]] || false
}

@test "types_compatibility: enum column readable from old dolt" {
    run dolt sql -q "SELECT pk, c_enum FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,val2" ]] || false

    run dolt sql -q "SELECT pk, c_enum FROM all_types WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,val1" ]] || false
}

@test "types_compatibility: set column readable from old dolt" {
    run dolt sql -q "SELECT pk, c_set FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,a" ]] || false

    run dolt sql -q "SELECT pk, c_set FROM all_types WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,b" ]] || false
}

# ---------------------------------------------------------------------------
# DML tests: INSERT, UPDATE, DELETE on a table written by old Dolt.
# ---------------------------------------------------------------------------

@test "types_compatibility: insert row into all_types written by old dolt" {
    run dolt sql -q "INSERT INTO all_types (pk, c_tinyint, c_int, c_bigint, c_float, c_double, c_decimal,
      c_varchar, c_text, c_longtext, c_blob, c_longblob, c_date, c_datetime, c_enum, c_set)
      VALUES (100, 42, 999, 12345, 3.5, 7.5, 99.99, 'new row', 'new text', 'new longtext',
      'new blob', 'new longblob', '2025-06-01', '2025-06-01 12:00:00', 'val3', 'c');"
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT pk, c_tinyint, c_varchar, c_text, c_enum FROM all_types WHERE pk=100;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "100,42,new row,new text,val3" ]] || false

    run dolt sql -q "SELECT count(*) FROM all_types;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "4" ]] || false
}

@test "types_compatibility: update all_types written by old dolt" {
    dolt sql -q "UPDATE all_types SET c_varchar = 'updated', c_text = 'updated text', c_blob = 'updated blob' WHERE pk = 1;"
    run dolt sql -q "SELECT pk, c_varchar, c_text, c_blob FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,updated,updated text,updated blob" ]] || false
}

@test "types_compatibility: update text and blob columns to large values in old table" {
    dolt sql -q "UPDATE all_types SET c_text = REPEAT('u', 1000), c_longtext = REPEAT('v', 1000),
      c_blob = REPEAT('w', 1000), c_longblob = REPEAT('z', 1000) WHERE pk = 1;"
    run dolt sql -q "SELECT pk, LENGTH(c_text), LENGTH(c_longtext), LENGTH(c_blob), LENGTH(c_longblob) FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,1000,1000,1000,1000" ]] || false
}

@test "types_compatibility: delete from all_types written by old dolt" {
    dolt sql -q "DELETE FROM all_types WHERE pk = 2;"
    run dolt sql -q "SELECT count(*) FROM all_types;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2" ]] || false

    run dolt sql -q "SELECT pk FROM all_types WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" -eq 1 ]] || false  # only header line
}

@test "types_compatibility: dml round-trip: insert then read back all column types" {
    dolt sql -q "INSERT INTO all_types (pk, c_tinyint, c_smallint, c_mediumint, c_int, c_bigint, c_bigint_u,
      c_float, c_double, c_decimal, c_char, c_varchar,
      c_tinytext, c_text, c_mediumtext, c_longtext,
      c_varbinary, c_tinyblob, c_blob, c_mediumblob, c_longblob,
      c_date, c_time, c_datetime, c_year, c_enum, c_set)
      VALUES (50, 55, 555, 55555, 555555, 5555555555, 9999999999,
      0.5, 0.25, 55.55, 'round', 'round trip',
      'rt tinytext', 'rt text', 'rt mediumtext', 'rt longtext',
      'rt varbinary', 'rt tinyblob', 'rt blob', 'rt mediumblob', 'rt longblob',
      '2025-03-18', '09:00:00', '2025-03-18 09:00:00', 2025, 'val3', 'c');"

    run dolt sql -q "SELECT pk, c_tinyint, c_int, c_bigint_u FROM all_types WHERE pk=50;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "50,55,555555,9999999999" ]] || false

    run dolt sql -q "SELECT pk, c_tinytext, c_text, c_longtext FROM all_types WHERE pk=50;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "50,rt tinytext,rt text,rt longtext" ]] || false

    run dolt sql -q "SELECT pk, c_tinyblob, c_blob, c_longblob FROM all_types WHERE pk=50;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "50,rt tinyblob,rt blob,rt longblob" ]] || false
}

# ---------------------------------------------------------------------------
# ADD COLUMN tests: verify that current Dolt can add columns of every type to
# a table that was originally created and written by an older Dolt version.
# This specifically tests the encoding path for newly added columns.
# ---------------------------------------------------------------------------

@test "types_compatibility: add tinytext column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_tinytext TINYTEXT;"
    dolt sql -q "UPDATE abc SET new_tinytext = 'tinytext for row';"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_tinytext) VALUES (99, 'test', 1.0, 'inserted tinytext');"

    run dolt sql -q "SELECT pk, new_tinytext FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,inserted tinytext" ]] || false

    run dolt sql -q "SELECT pk, new_tinytext FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,tinytext for row" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "types_compatibility: add text column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_text TEXT;"
    dolt sql -q "UPDATE abc SET new_text = 'text value for row';"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_text) VALUES (99, 'test', 1.0, 'inserted text');"

    run dolt sql -q "SELECT pk, new_text FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,inserted text" ]] || false

    run dolt sql -q "SELECT pk, new_text FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,text value for row" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "types_compatibility: add mediumtext column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_mediumtext MEDIUMTEXT;"
    dolt sql -q "UPDATE abc SET new_mediumtext = REPEAT('m', 300);"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_mediumtext) VALUES (99, 'test', 1.0, REPEAT('i', 300));"

    run dolt sql -q "SELECT pk, LENGTH(new_mediumtext) FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,300" ]] || false

    run dolt sql -q "SELECT pk, LENGTH(new_mediumtext) FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,300" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "types_compatibility: add longtext column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_longtext LONGTEXT;"
    dolt sql -q "UPDATE abc SET new_longtext = REPEAT('L', 1000);"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_longtext) VALUES (99, 'test', 1.0, REPEAT('I', 1000));"

    run dolt sql -q "SELECT pk, LENGTH(new_longtext) FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,1000" ]] || false

    run dolt sql -q "SELECT pk, LENGTH(new_longtext) FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,1000" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "types_compatibility: add tinyblob column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_tinyblob TINYBLOB;"
    dolt sql -q "UPDATE abc SET new_tinyblob = 'tinyblob data';"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_tinyblob) VALUES (99, 'test', 1.0, 'inserted tinyblob');"

    run dolt sql -q "SELECT pk, new_tinyblob FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,inserted tinyblob" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "types_compatibility: add blob column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_blob BLOB;"
    dolt sql -q "UPDATE abc SET new_blob = 'blob data for row';"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_blob) VALUES (99, 'test', 1.0, 'inserted blob');"

    run dolt sql -q "SELECT pk, new_blob FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,inserted blob" ]] || false

    run dolt sql -q "SELECT pk, new_blob FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,blob data for row" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "types_compatibility: add mediumblob column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_mediumblob MEDIUMBLOB;"
    dolt sql -q "UPDATE abc SET new_mediumblob = REPEAT('M', 300);"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_mediumblob) VALUES (99, 'test', 1.0, REPEAT('J', 300));"

    run dolt sql -q "SELECT pk, LENGTH(new_mediumblob) FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,300" ]] || false

    run dolt sql -q "SELECT pk, LENGTH(new_mediumblob) FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,300" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "types_compatibility: add longblob column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_longblob LONGBLOB;"
    dolt sql -q "UPDATE abc SET new_longblob = REPEAT('Z', 1000);"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_longblob) VALUES (99, 'test', 1.0, REPEAT('W', 1000));"

    run dolt sql -q "SELECT pk, LENGTH(new_longblob) FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,1000" ]] || false

    run dolt sql -q "SELECT pk, LENGTH(new_longblob) FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,1000" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "types_compatibility: add varchar column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_varchar VARCHAR(255);"
    dolt sql -q "UPDATE abc SET new_varchar = 'varchar for row';"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_varchar) VALUES (99, 'test', 1.0, 'inserted varchar');"

    run dolt sql -q "SELECT pk, new_varchar FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,inserted varchar" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "types_compatibility: add varbinary column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_varbinary VARBINARY(255);"
    dolt sql -q "UPDATE abc SET new_varbinary = 'varbinary for row';"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_varbinary) VALUES (99, 'test', 1.0, 'inserted varbinary');"

    run dolt sql -q "SELECT pk, new_varbinary FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,inserted varbinary" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "types_compatibility: add integer columns to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_tinyint TINYINT, ADD COLUMN new_int INT, ADD COLUMN new_bigint BIGINT;"
    dolt sql -q "UPDATE abc SET new_tinyint = 42, new_int = 100000, new_bigint = 9999999999;"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_tinyint, new_int, new_bigint) VALUES (99, 'test', 1.0, -1, -100000, -9999999999);"

    run dolt sql -q "SELECT pk, new_tinyint, new_int, new_bigint FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,-1,-100000,-9999999999" ]] || false

    run dolt sql -q "SELECT pk, new_tinyint, new_int, new_bigint FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,42,100000,9999999999" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "types_compatibility: add decimal column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_decimal DECIMAL(12,4);"
    dolt sql -q "UPDATE abc SET new_decimal = 9876.5432;"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_decimal) VALUES (99, 'test', 1.0, -9876.5432);"

    run dolt sql -q "SELECT pk, new_decimal FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,-9876.5432" ]] || false

    run dolt sql -q "SELECT pk, new_decimal FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,9876.5432" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "types_compatibility: add datetime and date columns to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_date DATE, ADD COLUMN new_datetime DATETIME;"
    dolt sql -q "UPDATE abc SET new_date = '2025-03-18', new_datetime = '2025-03-18 10:00:00';"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_date, new_datetime) VALUES (99, 'test', 1.0, '2025-06-01', '2025-06-01 12:00:00');"

    run dolt sql -q "SELECT pk, new_date, new_datetime FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,2025-06-01,2025-06-01 12:00:00" ]] || false

    run dolt sql -q "SELECT pk, new_date, new_datetime FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,2025-03-18,2025-03-18 10:00:00" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "types_compatibility: add enum column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_enum ENUM('x','y','z');"
    dolt sql -q "UPDATE abc SET new_enum = 'x';"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_enum) VALUES (99, 'test', 1.0, 'z');"

    run dolt sql -q "SELECT pk, new_enum FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,z" ]] || false

    run dolt sql -q "SELECT pk, new_enum FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,x" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "types_compatibility: add json column to old table and use dml" {
    [[ "$DOLT_VERSION" =~ 0\.50 ]] && skip "JSON type test not run for Dolt version 0.50"
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_json JSON;"
    dolt sql -q "UPDATE abc SET new_json = '{\"updated\": true}';"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_json) VALUES (99, 'test', 1.0, '{\"inserted\": 1}');"

    run dolt sql -q "SELECT pk, JSON_EXTRACT(new_json, '$.inserted') FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false

    run dolt sql -q "SELECT pk, JSON_EXTRACT(new_json, '$.updated') FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "true" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

# ---------------------------------------------------------------------------
# Schema tests: verify old table schemas are correctly deserialized.
# ---------------------------------------------------------------------------

@test "types_compatibility: all_types schema readable from old dolt" {
    run dolt schema show all_types
    [ "$status" -eq 0 ]
    output_lower=$(echo "$output" | tr '[:upper:]' '[:lower:]')
    [[ "$output_lower" =~ "tinytext" ]] || false
    [[ "$output_lower" =~ "mediumtext" ]] || false
    [[ "$output_lower" =~ "longtext" ]] || false
    [[ "$output_lower" =~ "tinyblob" ]] || false
    [[ "$output_lower" =~ "mediumblob" ]] || false
    [[ "$output_lower" =~ "longblob" ]] || false
    [[ "$output_lower" =~ "decimal" ]] || false
    [[ "$output_lower" =~ "datetime" ]] || false
    [[ "$output_lower" =~ "enum" ]] || false
}

@test "types_compatibility: dolt diff works on all_types after dml" {
    dolt sql -q "INSERT INTO all_types (pk, c_text, c_blob) VALUES (99, 'diff test text', 'diff test blob');"
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "99" ]] || false
}

@test "types_compatibility: dolt commit works after dml on all_types" {
    dolt sql -q "INSERT INTO all_types (pk, c_text, c_longtext, c_blob, c_longblob)
      VALUES (98, 'commit text', REPEAT('C', 500), 'commit blob', REPEAT('D', 500));"
    dolt add .
    run dolt commit -m "added row to all_types"
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT pk, c_text FROM all_types WHERE pk=98;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "98,commit text" ]] || false
}

# ---------------------------------------------------------------------------
# View tests: verify that views over all_types written by old Dolt are
# correctly deserialized and queryable by the current Dolt version.
# ---------------------------------------------------------------------------

@test "types_compatibility: all_types_view has expected row count" {
    run dolt sql -q "SELECT count(*) FROM all_types_view;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3" ]] || false
}

@test "types_compatibility: all_types_view returns same rows as underlying table" {
    run dolt sql -q "SELECT pk, c_tinyint, c_varchar FROM all_types_view WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,100,hello world" ]] || false

    run dolt sql -q "SELECT pk, c_tinyint, c_varchar FROM all_types_view WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,-100,hi there" ]] || false
}

@test "types_compatibility: all_types_view supports filtering" {
    run dolt sql -q "SELECT count(*) FROM all_types_view WHERE pk < 3;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2" ]] || false
}

@test "types_compatibility: all_types_view text and blob columns readable" {
    run dolt sql -q "SELECT pk, c_text, c_tinyblob FROM all_types_view WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,text val,tinyblob val" ]] || false
}

@test "types_compatibility: all_types_view shows in dolt_schemas" {
    run dolt sql -q "SELECT name FROM dolt_schemas WHERE name='all_types_view';" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "all_types_view" ]] || false
}
