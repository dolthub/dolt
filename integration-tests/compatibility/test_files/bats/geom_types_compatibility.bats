#!/usr/bin/env bats
# Tests that the current Dolt build can read, write, and alter tables containing
# geometry types that were originally created by an older Dolt version.
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
# Basic read tests: verify current Dolt can read geometry columns written by
# old Dolt without error or data corruption.
# ---------------------------------------------------------------------------

@test "geom_types table has expected row count" {
    run dolt sql -q "SELECT count(*) FROM geom_types;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2" ]] || false
}

@test "point column readable from old dolt" {
    run dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geom_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,1,2" ]] || false

    run dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geom_types WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,10,20" ]] || false
}

@test "point column st_astext readable from old dolt" {
    run dolt sql -q "SELECT pk, ST_AsText(c_point) FROM geom_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "POINT(1 2)" ]] || false
}

@test "linestring column st_astext readable from old dolt" {
    run dolt sql -q "SELECT pk, ST_AsText(c_linestring) FROM geom_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "LINESTRING" ]] || false
    [[ "$output" =~ "0 0" ]] || false
}

@test "polygon column st_astext readable from old dolt" {
    run dolt sql -q "SELECT pk, ST_AsText(c_polygon) FROM geom_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "POLYGON" ]] || false
}

@test "geometry column (stores point) readable from old dolt" {
    run dolt sql -q "SELECT pk, ST_AsText(c_geometry) FROM geom_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "POINT(3 4)" ]] || false
}

@test "geometry column (stores linestring) readable from old dolt" {
    run dolt sql -q "SELECT pk, ST_AsText(c_geometry) FROM geom_types WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "LINESTRING" ]] || false
}

@test "multipoint column readable from old dolt" {
    run dolt sql -q "SELECT pk, ST_AsText(c_multipoint) FROM geom_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "MULTIPOINT" ]] || false
}

@test "multilinestring column readable from old dolt" {
    run dolt sql -q "SELECT pk, ST_AsText(c_multilinestring) FROM geom_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "MULTILINESTRING" ]] || false
}

@test "multipolygon column readable from old dolt" {
    run dolt sql -q "SELECT pk, ST_AsText(c_multipolygon) FROM geom_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "MULTIPOLYGON" ]] || false
}

@test "geometrycollection column readable from old dolt" {
    run dolt sql -q "SELECT pk, ST_AsText(c_geometrycollection) FROM geom_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "GEOMETRYCOLLECTION" ]] || false
}

@test "null geometry columns readable from old dolt" {
    run dolt sql -q "SELECT pk, c_linestring IS NULL, c_polygon IS NULL FROM geom_types WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,true,true" ]] || false
}

# ---------------------------------------------------------------------------
# DML tests: INSERT, UPDATE, DELETE on a geometry table written by old Dolt.
# ---------------------------------------------------------------------------

@test "insert row into geom_types written by old dolt" {
    run dolt sql -q "INSERT INTO geom_types (pk, c_point, c_geometry)
      VALUES (100, ST_GeomFromText('POINT(5 6)'), ST_GeomFromText('POINT(7 8)'));"
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geom_types WHERE pk=100;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "100,5,6" ]] || false

    run dolt sql -q "SELECT count(*) FROM geom_types;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3" ]] || false
}

@test "update point column in geom_types written by old dolt" {
    dolt sql -q "UPDATE geom_types SET c_point = ST_GeomFromText('POINT(99 88)') WHERE pk = 1;"
    run dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geom_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,99,88" ]] || false
}

@test "update all geometry columns in geom_types written by old dolt" {
    dolt sql -q "UPDATE geom_types SET
      c_linestring = ST_GeomFromText('LINESTRING(0 0,10 10)'),
      c_polygon = ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))')
      WHERE pk = 1;"
    run dolt sql -q "SELECT pk, ST_AsText(c_linestring) FROM geom_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "LINESTRING" ]] || false
}

@test "delete from geom_types written by old dolt" {
    dolt sql -q "DELETE FROM geom_types WHERE pk = 2;"
    run dolt sql -q "SELECT count(*) FROM geom_types;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1" ]] || false

    run dolt sql -q "SELECT pk FROM geom_types WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" -eq 1 ]] || false  # only header line
}

@test "dml round-trip: insert then read back geometry types" {
    dolt sql -q "INSERT INTO geom_types (pk, c_point, c_linestring, c_polygon,
      c_multipoint, c_multilinestring, c_multipolygon, c_geometrycollection)
      VALUES (50,
        ST_GeomFromText('POINT(7 8)'),
        ST_GeomFromText('LINESTRING(1 1,3 3)'),
        ST_GeomFromText('POLYGON((0 0,3 0,3 3,0 3,0 0))'),
        ST_GeomFromText('MULTIPOINT(1 1,2 2,3 3)'),
        ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,4 4),(5 5,6 6))'),
        ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,3 2,3 3,2 3,2 2)))'),
        ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 1),POINT(2 2),POINT(3 3))')
      );"

    run dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geom_types WHERE pk=50;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "50,7,8" ]] || false

    run dolt sql -q "SELECT pk, ST_AsText(c_linestring) FROM geom_types WHERE pk=50;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "LINESTRING" ]] || false

    run dolt sql -q "SELECT pk, ST_AsText(c_multipoint) FROM geom_types WHERE pk=50;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "MULTIPOINT" ]] || false

    run dolt sql -q "SELECT pk, ST_AsText(c_multilinestring) FROM geom_types WHERE pk=50;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "MULTILINESTRING" ]] || false

    run dolt sql -q "SELECT pk, ST_AsText(c_multipolygon) FROM geom_types WHERE pk=50;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "MULTIPOLYGON" ]] || false

    run dolt sql -q "SELECT pk, ST_AsText(c_geometrycollection) FROM geom_types WHERE pk=50;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "GEOMETRYCOLLECTION" ]] || false
}

# ---------------------------------------------------------------------------
# ADD COLUMN tests: verify that current Dolt can add geometry columns to a
# table that was originally created and written by an older Dolt version.
# ---------------------------------------------------------------------------

@test "add point column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_point POINT;"
    dolt sql -q "UPDATE abc SET new_point = ST_GeomFromText('POINT(1 1)');"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_point) VALUES (99, 'test', 1.0, ST_GeomFromText('POINT(5 10)'));"

    run dolt sql -q "SELECT pk, ST_X(new_point), ST_Y(new_point) FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,5,10" ]] || false

    run dolt sql -q "SELECT pk, ST_X(new_point), ST_Y(new_point) FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "0,1,1" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "add geometry column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_geometry GEOMETRY;"
    dolt sql -q "UPDATE abc SET new_geometry = ST_GeomFromText('POINT(2 3)');"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_geometry) VALUES (99, 'test', 1.0, ST_GeomFromText('LINESTRING(0 0,1 1)'));"

    run dolt sql -q "SELECT pk, ST_AsText(new_geometry) FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "POINT(2 3)" ]] || false

    run dolt sql -q "SELECT pk, ST_AsText(new_geometry) FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "LINESTRING" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "add linestring column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_linestring LINESTRING;"
    dolt sql -q "UPDATE abc SET new_linestring = ST_GeomFromText('LINESTRING(0 0,1 1)');"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_linestring) VALUES (99, 'test', 1.0, ST_GeomFromText('LINESTRING(0 0,2 2,4 4)'));"

    run dolt sql -q "SELECT pk, ST_AsText(new_linestring) FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "LINESTRING" ]] || false

    run dolt sql -q "SELECT pk, ST_AsText(new_linestring) FROM abc WHERE pk=0;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "LINESTRING" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

@test "add polygon column to old table and use dml" {
    dolt sql -q "ALTER TABLE abc ADD COLUMN new_polygon POLYGON;"
    dolt sql -q "UPDATE abc SET new_polygon = ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))');"
    dolt sql -q "INSERT INTO abc (pk, a, b, new_polygon) VALUES (99, 'test', 1.0, ST_GeomFromText('POLYGON((0 0,3 0,3 3,0 3,0 0))'));"

    run dolt sql -q "SELECT pk, ST_AsText(new_polygon) FROM abc WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "POLYGON" ]] || false

    dolt sql -q "DELETE FROM abc WHERE pk=99;"
}

# ---------------------------------------------------------------------------
# Schema tests: verify old geometry table schemas are correctly deserialized.
# ---------------------------------------------------------------------------

@test "geom_types schema readable from old dolt" {
    run dolt schema show geom_types
    [ "$status" -eq 0 ]
    output_lower=$(echo "$output" | tr '[:upper:]' '[:lower:]')
    [[ "$output_lower" =~ "point" ]] || false
    [[ "$output_lower" =~ "linestring" ]] || false
    [[ "$output_lower" =~ "polygon" ]] || false
    [[ "$output_lower" =~ "multipoint" ]] || false
    [[ "$output_lower" =~ "multilinestring" ]] || false
    [[ "$output_lower" =~ "multipolygon" ]] || false
    [[ "$output_lower" =~ "geometrycollection" ]] || false
}

@test "dolt diff works on geom_types after dml" {
    dolt sql -q "INSERT INTO geom_types (pk, c_point) VALUES (99, ST_GeomFromText('POINT(1 1)'));"
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "99" ]] || false
}

@test "dolt commit works after dml on geom_types" {
    dolt sql -q "INSERT INTO geom_types (pk, c_point, c_linestring)
      VALUES (98, ST_GeomFromText('POINT(4 5)'), ST_GeomFromText('LINESTRING(0 0,4 4)'));"
    dolt add .
    run dolt commit -m "added row to geom_types"
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT pk, ST_X(c_point) FROM geom_types WHERE pk=98;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "98,4" ]] || false
}

# ---------------------------------------------------------------------------
# View tests: verify that a view over geom_types written by old Dolt is
# correctly deserialized and queryable by the current Dolt version.
# ---------------------------------------------------------------------------

@test "geom_view has expected row count" {
    run dolt sql -q "SELECT count(*) FROM geom_view;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2" ]] || false
}

@test "geom_view returns same rows as underlying table" {
    run dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geom_view WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1,1,2" ]] || false

    run dolt sql -q "SELECT pk, ST_X(c_point), ST_Y(c_point) FROM geom_view WHERE pk=2;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "2,10,20" ]] || false
}

@test "geom_view supports filtering" {
    run dolt sql -q "SELECT count(*) FROM geom_view WHERE pk = 1;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "1" ]] || false
}

@test "geom_view geometry columns readable via view" {
    run dolt sql -q "SELECT pk, ST_AsText(c_point) FROM geom_view WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "POINT(1 2)" ]] || false
}

@test "geom_view shows in dolt_schemas" {
    run dolt sql -q "SELECT name FROM dolt_schemas WHERE name='geom_view';" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "geom_view" ]] || false
}

@test "insert into base table visible through geom_view" {
    dolt sql -q "INSERT INTO geom_types (pk, c_point) VALUES (99, ST_GeomFromText('POINT(7 7)'));"
    run dolt sql -q "SELECT count(*) FROM geom_view;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "3" ]] || false

    run dolt sql -q "SELECT pk, ST_X(c_point) FROM geom_view WHERE pk=99;" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "99,7" ]] || false
}
