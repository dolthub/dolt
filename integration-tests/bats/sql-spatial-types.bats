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

@test "sql-spatial-types: create point table and insert point" {
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "create table point_tbl (p point)"
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "insert into point_tbl () values (point(1,2))"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK" ]] || false
}

@test "sql-spatial-types: create linestring table and insert linestring" {
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "create table line_tbl (l linestring)"
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "insert into line_tbl () values (linestring(point(1,2),point(3,4)))"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK" ]] || false
}

@test "sql-spatial-types: create polygon table and insert polygon" {
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "create table poly_tbl (p polygon)"
    [ "$status" -eq 0 ]
    [[ "$output" = "" ]] || false
    DOLT_ENABLE_SPATIAL_TYPES=true run dolt sql -q "insert into poly_tbl () values (polygon(linestring(point(1,2),point(3,4),point(5,6),point(1,2))))"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Query OK" ]] || false
}