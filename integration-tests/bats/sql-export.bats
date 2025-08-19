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
