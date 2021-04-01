#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "json: JSON hidden behind feature flag" {
    run dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
SQL
    [ $status -ne 0 ]

    run dolt --json sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
SQL
    [ "$status" -eq 0 ]
}

@test "json: query JSON values" {
    dolt --json sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":1}'), (2, '{"b":2}');
SQL
    run dolt --json sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"": 1}"' ]
    [ "${lines[2]}" = '2,"{""b"": 2}"' ]

    dolt --json sql <<SQL
    UPDATE js SET js = '{"c":3}' WHERE pk = 2;
SQL
    run dolt --json sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"": 1}"' ]
    [ "${lines[2]}" = '2,"{""c"": 3}"' ]

    dolt --json sql <<SQL
    DELETE FROM js WHERE pk = 2;
SQL
    run dolt --json sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"": 1}"' ]
}

@test "json: JSON value printing" {
    dolt --json sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":1}'), (2, '{"b":2}');
SQL

    run dolt --json sql -q "SELECT * FROM js;"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '+----+----------+' ]
    [ "${lines[1]}" = '| pk | js       |' ]
    [ "${lines[2]}" = '+----+----------+' ]
    [ "${lines[3]}" = '| 1  | {"a": 1} |' ]
    [ "${lines[4]}" = '| 2  | {"b": 2} |' ]
    [ "${lines[5]}" = '+----+----------+' ]

    run dolt --json sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = 'pk,js' ]
    [ "${lines[1]}" = '1,"{""a"": 1}"' ]
    [ "${lines[2]}" = '2,"{""b"": 2}"' ]

    run dolt --json sql -q "SELECT * FROM js;" -r json
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '{"rows": [{"pk":1,"js":{"a": 1}},{"pk":2,"js":{"b": 2}}]}' ]
}