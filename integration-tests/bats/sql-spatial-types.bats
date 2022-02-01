#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "sql-spatial-types: can't make spatial types without enabling feature flag" {
    run dolt sql -q "create table point_tbl (p point)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "cannot be made" ]] || false
}

@test "sql-spatial-types: can make spatial types with flag" {
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "create table point_tbl (p point)"
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
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