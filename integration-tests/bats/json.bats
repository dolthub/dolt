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

@test "json: diff JSON values" {
    dolt --json sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":1}'), (2, '{"b":2}');
SQL
    dolt --json add .
    dolt --json commit -am "added JSON table"

    dolt --json sql <<SQL
    UPDATE js SET js = '{"a":11}' WHERE pk = 1;
    DELETE FROM js WHERE pk = 2;
    INSERT INTO js VALUES (3, '{"c":3}');
SQL

   run dolt --json diff
   [ "$status" -eq 0 ]
   [ "${lines[0]}"  = 'diff --dolt a/js b/js' ]
   [ "${lines[1]}"  = '--- a/js @ hkngn01jojsm81hqrtbqnvr1buhooove' ]
   [ "${lines[2]}"  = '+++ b/js @ di5agqioh8cn0osrk220vtlgs9vu02jh' ]
   [ "${lines[3]}"  = '+-----+----+-----------+' ]
   [ "${lines[4]}"  = '|     | pk | js        |' ]
   [ "${lines[5]}"  = '+-----+----+-----------+' ]
   [ "${lines[6]}"  = '|  <  | 1  | {"a": 1}  |' ]
   [ "${lines[7]}"  = '|  >  | 1  | {"a": 11} |' ]
   [ "${lines[8]}"  = '|  -  | 2  | {"b": 2}  |' ]
   [ "${lines[9]}"  = '|  +  | 3  | {"c": 3}  |' ]
   [ "${lines[10]}" = '+-----+----+-----------+' ]
}

@test "json: merge JSON values" {
    dolt --json sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":1}'), (2, '{"b":2}');
SQL
    dolt --json add .
    dolt --json commit -am "added JSON table"
    dolt --json branch other
    dolt --json branch another

    dolt --json sql <<SQL
    UPDATE js SET js = '{"a":11}' WHERE pk = 1;
SQL
    dolt --json commit -am "made changes on branch master"

    dolt --json checkout other
    dolt --json sql <<SQL
    UPDATE js SET js = '{"b":22}' WHERE pk = 2;
SQL
    dolt --json commit -am "made changes on branch other"

    dolt --json checkout master
    dolt --json merge other
    run dolt --json sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"": 11}"' ]
    [ "${lines[2]}" = '2,"{""b"": 22}"' ]
    dolt --json commit -am "merged other into master"

    # test merge conflicts
    dolt --json checkout another
    dolt --json sql <<SQL
    UPDATE js SET js = '{"b":99}' WHERE pk = 2;
SQL
    dolt --json commit -am "made changes on branch another"

    run dolt --json merge other
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT" ]] || false
    run dolt --json conflicts resolve --ours js
    [ "$status" -eq 0 ]
    run dolt --json sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"": 1}"' ]
    [ "${lines[2]}" = '2,"{""b"": 99}"' ]
}