#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    cp -a $BATS_TEST_DIRNAME/helper/testdata/. ./
}

teardown() {
    teardown_common
}

@test "back-compat: data check" {
    for testdir in */; do
        cd "$testdir"
        dolt status
        run dolt migrate
        [ "$status" -eq "0" ]
        [[ "$output" =~ "Migrating database to the latest data format" ]] || false
        run dolt branch
        [ "$status" -eq "0" ]
        [[ "$output" =~ "master" ]] || false
        [[ "$output" =~ "conflict" ]] || false
        [[ "$output" =~ "newcolumn" ]] || false
        run dolt schema show
        [ "$status" -eq "0" ]
        [[ "$output" =~ "\`pk\` bigint NOT NULL" ]] || false
        [[ "$output" =~ "\`a\` longtext" || "$output" =~ "\`a\` varchar(16383)" ]] || false
        [[ "$output" =~ "\`b\` datetime" ]] || false
        run dolt sql -q "select * from abc order by pk asc"
        [ "$status" -eq "0" ]
        [[ "${lines[3]}" =~ " 1 " ]] || false
        [[ "${lines[3]}" =~ " data " ]] || false
        [[ "${lines[3]}" =~ " 2020-01-13 20:45:18.53558 " ]] || false
        dolt checkout conflict
        run dolt schema show
        [ "$status" -eq "0" ]
        [[ "$output" =~ "\`pk\` bigint NOT NULL" ]] || false
        [[ "$output" =~ "\`a\` longtext" || "$output" =~ "\`a\` varchar(16383)" ]] || false
        [[ "$output" =~ "\`b\` datetime" ]] || false
        run dolt sql -q "select * from abc order by pk asc"
        [ "$status" -eq "0" ]
        [[ "${lines[3]}" =~ " 1 " ]] || false
        [[ "${lines[3]}" =~ " data " ]] || false
        [[ "${lines[3]}" =~ " 2020-01-13 20:45:18.53558 " ]] || false
        [[ "${lines[4]}" =~ " 2 " ]] || false
        [[ "${lines[4]}" =~ " something " ]] || false
        [[ "${lines[4]}" =~ " 2020-01-14 20:48:37.13061 " ]] || false
        dolt checkout newcolumn
        run dolt schema show
        [ "$status" -eq "0" ]
        [[ "$output" =~ "\`pk\` bigint NOT NULL" ]] || false
        [[ "$output" =~ "\`a\` longtext" || "$output" =~ "\`a\` varchar(16383)" ]] || false
        [[ "$output" =~ "\`b\` datetime" ]] || false
        [[ "$output" =~ "\`c\` bigint unsigned" ]] || false
        run dolt sql -q "select * from abc order by pk asc"
        [ "$status" -eq "0" ]
        [[ "${lines[3]}" =~ " 1 " ]] || false
        [[ "${lines[3]}" =~ " data " ]] || false
        [[ "${lines[3]}" =~ " 2020-01-13 20:45:18.53558 " ]] || false
        [[ "${lines[3]}" =~ " 2133" ]] || false
        [[ "${lines[4]}" =~ " 2 " ]] || false
        [[ "${lines[4]}" =~ " something " ]] || false
        [[ "${lines[4]}" =~ " 2020-01-13 20:48:37.13061 " ]] || false
        [[ "${lines[4]}" =~ " 1132020" ]] || false
        cd ..
    done
}

@test "back-compat: table operations" {
    # Broken by dropped err fixed in https://github.com/dolthub/dolt/pull/1465
    skip
    for testdir in */; do
        cd "$testdir"
        dolt table cp abc copy
        dolt table mv abc move
        run dolt ls
        [ "$status" -eq "0" ]
        [[ "$output" =~ "copy" ]] || false
        [[ "$output" =~ "move" ]] || false
        cd ..
    done
}

@test "back-compat: adding commits" {
    for testdir in */; do
        cd "$testdir"
        run dolt migrate
        [ "$status" -eq "0" ]
        [[ "$output" =~ "Migrating database to the latest data format" ]] || false
        dolt sql -q "insert into abc values (2, 'text', '2020-01-15 20:49:22.28427')"
        dolt add .
        dolt commit -m "Add value during test"
        run dolt sql -q "select * from abc order by pk asc"
        [ "$status" -eq "0" ]
        [[ "${lines[4]}" =~ " 2 " ]] || false
        [[ "${lines[4]}" =~ " text " ]] || false
        [[ "${lines[4]}" =~ " 2020-01-15 20:49:22.28427 " ]] || false
        dolt checkout newcolumn
        dolt checkout -b testaddcommit
        dolt sql -q "insert into abc values (3, 'text', '2020-01-15 20:49:22.28427', 9241)"
        dolt add .
        dolt commit -m "Add value during test"
        run dolt sql -q "select * from abc order by pk asc"
        [ "$status" -eq "0" ]
        [[ "${lines[5]}" =~ " 3 " ]] || false
        [[ "${lines[5]}" =~ " text " ]] || false
        [[ "${lines[5]}" =~ " 2020-01-15 20:49:22.28427 " ]] || false
        [[ "${lines[5]}" =~ " 9241 " ]] || false
        cd ..
    done
}

@test "back-compat: merging" {
    for testdir in */; do
        cd "$testdir"
        run dolt migrate
        [ "$status" -eq "0" ]
        run dolt merge newcolumn
        [ "$status" -eq "0" ]
        [[ "$output" =~ "Fast-forward" ]] || false
        cd ..
    done
}

@test "back-compat: resolving conflicts" {
    skip https://github.com/dolthub/dolt/issues/773
    for testdir in */; do
        cd "$testdir"
        run dolt migrate
        [ "$status" -eq "0" ]
        [[ "$output" =~ "Migrating database to the latest data format" ]] || false
        dolt checkout conflict
        run dolt merge newcolumn
        [ "$status" -eq "0" ]
        [[ "$output" =~ "CONFLICT" ]] || false
        run dolt conflicts cat abc
        [ "$status" -eq "0" ]
        [[ "${lines[3]}" =~ " ours " ]] || false
        [[ "${lines[3]}" =~ " 2 " ]] || false
        [[ "${lines[3]}" =~ " something " ]] || false
        [[ "${lines[3]}" =~ " 2020-01-14 20:48:37.13061 " ]] || false
        [[ "${lines[3]}" =~ "  NULL  " ]] || false
        [[ "${lines[4]}" =~ " theirs " ]] || false
        [[ "${lines[4]}" =~ " 2 " ]] || false
        [[ "${lines[4]}" =~ " something " ]] || false
        [[ "${lines[4]}" =~ " 2020-01-13 20:48:37.13061 " ]] || false
        [[ "${lines[4]}" =~ " 1132020 " ]] || false
        dolt conflicts resolve --theirs abc
        dolt add .
        dolt commit -m "Merged newcolumn into conflict"
        run dolt sql -q "select * from abc order by pk asc"
        [ "$status" -eq "0" ]
        [[ "${lines[3]}" =~ " 1 " ]] || false
        [[ "${lines[3]}" =~ " data " ]] || false
        [[ "${lines[3]}" =~ " 2020-01-13 20:45:18.53558 " ]] || false
        [[ "${lines[3]}" =~ " 2133" ]] || false
        [[ "${lines[4]}" =~ " 2 " ]] || false
        [[ "${lines[4]}" =~ " something " ]] || false
        [[ "${lines[4]}" =~ " 2020-01-13 20:48:37.13061 " ]] || false
        [[ "${lines[4]}" =~ " 1132020" ]] || false
        cd ..
    done
}
