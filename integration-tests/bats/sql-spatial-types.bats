#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-spatial-types: can make spatial types" {
    run dolt sql -q "create table point_tbl (p point)"
    [ "$status" -eq 0 ]
    [ "$output" = "" ] || false
}

@test "sql-spatial-types: create point table and insert points" {
    run dolt sql -q "create table point_tbl (p point)"
    [ "$status" -eq 0 ]
    [ "$output" = "" ] || false
    run dolt sql -q "insert into point_tbl () values (point(1,2))"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK" ]] || false
    run dolt sql -q "insert into point_tbl () values (point(3,4)), (point(5,6))"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK" ]] || false
    run dolt sql -q "insert into point_tbl () values (point(123.456, 0.789))"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK" ]] || false
    run dolt sql -q "select st_aswkt(p) from point_tbl"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "POINT(1 2)" ]] || false
    [[ "$output" =~ "POINT(3 4)" ]] || false
    [[ "$output" =~ "POINT(5 6)" ]] || false
    [[ "$output" =~ "POINT(123.456 0.789)" ]] || false
}

@test "sql-spatial-types: create linestring table and insert linestrings" {
    run dolt sql -q "create table line_tbl (l linestring)"
    [ "$status" -eq 0 ]
    [ "$output" = "" ] || false
    run dolt sql -q "insert into line_tbl () values (linestring(point(1,2),point(3,4)))"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK" ]] || false
    run dolt sql -q "insert into line_tbl () values (linestring(point(1.2345, 678.9), point(111.222, 333.444), point(55.66, 77.88))), (linestring(point(1.1, 2.2),point(3.3, 4)))"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK" ]] || false
    run dolt sql -q "select st_aswkt(l) from line_tbl"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "LINESTRING(1 2,3 4)" ]] || false
    [[ "$output" =~ "LINESTRING(1.2345 678.9,111.222 333.444,55.66 77.88)" ]] || false
    [[ "$output" =~ "LINESTRING(1.1 2.2,3.3 4)" ]] || false
}

@test "sql-spatial-types: create polygon table and insert polygon" {
    run dolt sql -q "create table poly_tbl (p polygon)"
    [ "$status" -eq 0 ]
    [ "$output" = "" ] || false
    run dolt sql -q "insert into poly_tbl () values (polygon(linestring(point(1,2),point(3,4),point(5,6),point(7,8))))"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Invalid GIS data" ]] || false
    run dolt sql -q "insert into poly_tbl () values (polygon(linestring(point(1,2),point(3,4),point(5,6),point(1,2))))"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK" ]] || false
    run dolt sql -q "insert into poly_tbl () values (polygon(linestring(point(1,1),point(2,2),point(3,3),point(1,1)))), (polygon(linestring(point(0.123,0.456),point(1.22,1.33),point(1.11,0.99),point(0.123,0.456))))"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK" ]] || false
    run dolt sql -q "select st_aswkt(p) from poly_tbl"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "POLYGON((1 2,3 4,5 6,1 2))" ]] || false
    [[ "$output" =~ "POLYGON((1 1,2 2,3 3,1 1))" ]] || false
    [[ "$output" =~ "POLYGON((0.123 0.456,1.22 1.33,1.11 0.99,0.123 0.456))" ]] || false
}

@test "sql-spatial-types: create geometry table and insert existing spetial types" {
    skip_nbf_dolt_1

    # create geometry table
    run dolt sql -q "create table geom_tbl (g geometry)"
    [ "$status" -eq 0 ]
    [ "$output" = "" ] || false

    # inserting point
    run dolt sql -q "insert into geom_tbl () values (point(1,2))"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK" ]] || false

    # inserting linestring
    run dolt sql -q "insert into geom_tbl () values (linestring(point(1,2),point(3,4)))"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK" ]] || false

    # inserting polygon
    run dolt sql -q "insert into geom_tbl () values (polygon(linestring(point(1,2),point(3,4),point(5,6),point(1,2))))"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK" ]] || false

    # select everything
    run dolt sql -q "select st_aswkt(g) from geom_tbl"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "POINT(1 2)" ]] || false
    [[ "$output" =~ "LINESTRING(1 2,3 4)" ]] || false
    [[ "$output" =~ "POLYGON((1 2,3 4,5 6,1 2))" ]] || false
}


@test "sql-spatial-types: prevent point as primary key" {
    run dolt sql -q "create table point_tbl (p point primary key)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't use Spatial Types as Primary Key" ]] || false
}

@test "sql-spatial-types: prevent linestring as primary key" {
    run dolt sql -q "create table line_tbl (l linestring primary key)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't use Spatial Types as Primary Key" ]] || false
}

@test "sql-spatial-types: prevent polygon as primary key" {
    run dolt sql -q "create table poly_tbl (p polygon primary key)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't use Spatial Types as Primary Key" ]] || false
}

@test "sql-spatial-types: prevent geometry as primary key" {
    run dolt sql -q "create table geom_tbl (g geometry primary key)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't use Spatial Types as Primary Key" ]] || false
}

@test "sql-spatial-types: prevent altering table to use point type as primary key" {
    skip_nbf_dolt_1
    dolt sql -q "create table point_tbl (p int primary key)"
    run dolt sql -q "alter table point_tbl modify column p point primary key"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't use Spatial Types as Primary Key" ]] || false
}

@test "sql-spatial-types: prevent altering table to use linestring type as primary key" {
    skip_nbf_dolt_1
    dolt sql -q "create table line_tbl (l int primary key)"
    run dolt sql -q "alter table line_tbl modify column l linestring primary key"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't use Spatial Types as Primary Key" ]] || false
}

@test "sql-spatial-types: prevent altering table to use polygon type as primary key" {
    skip_nbf_dolt_1
    dolt sql -q "create table poly_tbl (p int primary key)"
    run dolt sql -q "alter table poly_tbl modify column p polygon primary key"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't use Spatial Types as Primary Key" ]] || false
}

@test "sql-spatial-types: prevent altering table to use geometry type as primary key" {
    skip_nbf_dolt_1
    dolt sql -q "create table geom_tbl (g int primary key)"
    run dolt sql -q "alter table geom_tbl modify column g geometry primary key"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't use Spatial Types as Primary Key" ]] || false
}

@test "sql-spatial-types: prevent creating index on point type" {
    dolt sql -q "create table point_tbl (p point)"
    run dolt sql -q "create index idx on point_tbl (p)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot create an index over spatial type columns" ]] || false
}

@test "sql-spatial-types: prevent creating index on linestring types" {
    dolt sql -q "create table line_tbl (l linestring)"
    run dolt sql -q "create index idx on line_tbl (l)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot create an index over spatial type columns" ]] || false
}

@test "sql-spatial-types: prevent creating index on polygon types" {
    dolt sql -q "create table poly_tbl (p polygon)"
    run dolt sql -q "create index idx on poly_tbl (p)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot create an index over spatial type columns" ]] || false
}

@test "sql-spatial-types: prevent creating index on geometry types" {
    dolt sql -q "create table geom_tbl (g geometry)"
    run dolt sql -q "create index idx on geom_tbl (g)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot create an index over spatial type columns" ]] || false
}

@test "sql-spatial-types: allow index on non-spatial columns of spatial table" {
    dolt sql -q "create table poly_tbl (a int, p polygon)"
    dolt sql -q "create index idx on poly_tbl (a)"
}

@test "sql-spatial-types: SRID defined in column definition in CREATE TABLE" {
    run dolt sql -q "CREATE TABLE pt (i int primary key, p POINT NOT NULL SRID 1)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "unsupported feature: unsupported SRID value" ]] || false

    run dolt sql -q "CREATE TABLE pt (i int primary key, p POINT NOT NULL SRID 0)"
    [ "$status" -eq 0 ]

    run dolt sql -q "SHOW CREATE TABLE pt"
    [[ "$output" =~ "\`p\` point NOT NULL SRID 0" ]] || false

    dolt sql -q "INSERT INTO pt VALUES (1, POINT(5,6))"
    run dolt sql -q "SELECT ST_ASWKT(p) FROM pt"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "POINT(5 6)" ]] || false

    run dolt sql -q "INSERT INTO pt VALUES (2, ST_GEOMFROMTEXT(ST_ASWKT(POINT(1,2)), 4326))"
    [ "$status" -eq 1 ]
    [ "$output" = "The SRID of the geometry does not match the SRID of the column 'p'. The SRID of the geometry is 4326, but the SRID of the column is 0. Consider changing the SRID of the geometry or the SRID property of the column." ]

    run dolt sql -q "SELECT ST_ASWKT(p) FROM pt"
    [[ ! "$output" =~ "POINT(1 2)" ]] || false
}

@test "sql-spatial-types: SRID defined in column definition in ALTER TABLE" {
    run dolt sql << SQL
CREATE TABLE table1 (i int primary key, p LINESTRING NOT NULL SRID 4326);
INSERT INTO table1 VALUES (1, ST_GEOMFROMTEXT(ST_ASWKT(LINESTRING(POINT(0,0),POINT(1,2))), 4326));
SQL
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT ST_ASWKT(p) FROM table1"
    [[ "$output" =~ "LINESTRING(0 0,1 2)" ]] || false

    run dolt sql -q "ALTER TABLE table1 MODIFY COLUMN p GEOMETRY NOT NULL SRID 4326"
    [ "$status" -eq 0 ]

    run dolt sql -q "SHOW CREATE TABLE table1"
    [[ "$output" =~ "\`p\` geometry NOT NULL SRID 4326" ]] || false

    run dolt sql -q "SELECT ST_ASWKT(p) FROM table1"
    [[ "$output" =~ "LINESTRING(0 0,1 2)" ]] || false

    run dolt sql -q "INSERT INTO table1 VALUES (2, ST_SRID(POINT(1,2), 4326))"
    [ "$status" -eq 0 ]

    run dolt sql -q "SELECT ST_ASWKT(p) FROM table1"
    [[ "$output" =~ "LINESTRING(0 0,1 2)" ]] || false
    [[ "$output" =~ "POINT(2 1)" ]] || false

    run dolt sql -q "ALTER TABLE table1 MODIFY COLUMN p LINESTRING SRID 4326"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Cannot get geometry object from data you send to the GEOMETRY field" ]] || false

    dolt sql -q "DELETE FROM table1 WHERE i = 1"
    run dolt sql -q "SELECT ST_ASWKT(p) FROM pt"
    [[ ! "$output" =~ "LINESTRING(0 0,1 2)" ]] || false

    run dolt sql -q "ALTER TABLE table1 MODIFY COLUMN p POINT SRID 4326"
    [ "$status" -eq 0 ]

    run dolt sql -q "ALTER TABLE table1 MODIFY COLUMN p POINT SRID 0"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "The SRID of the geometry does not match the SRID of the column 'p'. The SRID of the geometry is 4326, but the SRID of the column is 0. Consider changing the SRID of the geometry or the SRID property of the column." ]] || false
}
