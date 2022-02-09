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

@test "sql-spatial-types: prevent point as primary key" {
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "create table point_tbl (p point primary key)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't use Spatial Types as Primary Key" ]] || false
}

@test "sql-spatial-types: prevent linestring as primary key" {
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "create table line_tbl (l linestring primary key)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't use Spatial Types as Primary Key" ]] || false
}

@test "sql-spatial-types: prevent polygon as primary key" {
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "create table poly_tbl (p polygon primary key)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't use Spatial Types as Primary Key" ]] || false
}

@test "sql-spatial-types: prevent altering table to use point type as primary key" {
    DOLT_ENABLE_SPATIAL_TYPES=true dolt sql -q "create table point_tbl (p int primary key)"
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "alter table point_tbl modify column p point primary key"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't use Spatial Types as Primary Key" ]] || false
}

@test "sql-spatial-types: prevent altering table to use linestring type as primary key" {
    DOLT_ENABLE_SPATIAL_TYPES=true dolt sql -q "create table line_tbl (l int primary key)"
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "alter table line_tbl modify column l linestring primary key"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't use Spatial Types as Primary Key" ]] || false
}

@test "sql-spatial-types: prevent altering table to use polygon type as primary key" {
    DOLT_ENABLE_SPATIAL_TYPES=true dolt sql -q "create table poly_tbl (p int primary key)"
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "alter table poly_tbl modify column p polygon primary key"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "can't use Spatial Types as Primary Key" ]] || false
}

@test "sql-spatial-types: prevent creating index on point type" {
    DOLT_ENABLE_SPATIAL_TYPES=true dolt sql -q "create table point_tbl (p point)"
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "create index idx on point_tbl (p)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot create an index over spatial type columns" ]] || false
}

@test "sql-spatial-types: prevent creating index on linestring types" {
    DOLT_ENABLE_SPATIAL_TYPES=true dolt sql -q "create table line_tbl (l linestring)"
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "create index idx on line_tbl (l)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot create an index over spatial type columns" ]] || false
}

@test "sql-spatial-types: prevent creating index on spatial types" {
    DOLT_ENABLE_SPATIAL_TYPES=true dolt sql -q "create table poly_tbl (p polygon)"
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "create index idx on poly_tbl (p)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot create an index over spatial type columns" ]] || false
}