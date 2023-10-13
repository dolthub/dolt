#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

# A bats test just for new format json. It's in a separate file since the old format has
# different formatting for json.

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "json-new-fmt: Create table with JSON column" {
    run dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
SQL
    [ "$status" -eq 0 ]
}

@test "json-new-fmt: query JSON values" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":1}'), (2, '{"b":2}');
SQL
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"": 1}"' ]
    [ "${lines[2]}" = '2,"{""b"": 2}"' ]

    dolt sql <<SQL
    UPDATE js SET js = '{"c":3}' WHERE pk = 2;
SQL
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"": 1}"' ]
    [ "${lines[2]}" = '2,"{""c"": 3}"' ]

    dolt sql <<SQL
    DELETE FROM js WHERE pk = 2;
SQL
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"": 1}"' ]
}

@test "json-new-fmt: JSON value printing" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":1}'), (2, '{"b":2}');
SQL

    run dolt sql -q "SELECT * FROM js;"
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '+----+----------+' ]
    [ "${lines[1]}" = '| pk | js       |' ]
    [ "${lines[2]}" = '+----+----------+' ]
    [ "${lines[3]}" = '| 1  | {"a": 1} |' ]
    [ "${lines[4]}" = '| 2  | {"b": 2} |' ]
    [ "${lines[5]}" = '+----+----------+' ]

    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = 'pk,js' ]
    [ "${lines[1]}" = '1,"{""a"": 1}"' ]
    [ "${lines[2]}" = '2,"{""b"": 2}"' ]

    dolt sql -q "SELECT * FROM js;" -r json
    run dolt sql -q "SELECT * FROM js;" -r json
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '{"rows": [{"js":{"a":1},"pk":1},{"js":{"b":2},"pk":2}]}' ]

    dolt sql <<SQL
insert into js values (3, '["abc", 123, 1.5, {"a": 123, "b":[456, "def"]}]');
SQL

    dolt sql -q "SELECT * FROM js where pk = 3" -r json
    run dolt sql -q "SELECT * FROM js where pk = 3" -r json
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = '{"rows": [{"js":["abc",123,1.5,{"a":123,"b":[456,"def"]}],"pk":3}]}' ]

}

@test "json-new-fmt: diff JSON values" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":1}'), (2, '{"b":2}');
SQL
    dolt add .
    dolt commit -am "added JSON table"

    dolt sql <<SQL
    UPDATE js SET js = '{"a":11}' WHERE pk = 1;
    DELETE FROM js WHERE pk = 2;
    INSERT INTO js VALUES (3, '{"c":3}');
SQL

    dolt diff
    run dolt diff
    echo "output: $output"
    [ "$status" -eq 0 ]
    [ "${lines[0]}"  = 'diff --dolt a/js b/js' ]
    [ "${lines[3]}"  = '+---+----+-----------+' ]
    [ "${lines[4]}"  = '|   | pk | js        |' ]
    [ "${lines[5]}"  = '+---+----+-----------+' ]
    [ "${lines[6]}"  = '| < | 1  | {"a": 1}  |' ]
    [ "${lines[7]}"  = '| > | 1  | {"a": 11} |' ]
    [ "${lines[8]}"  = '| - | 2  | {"b": 2}  |' ]
    [ "${lines[9]}"  = '| + | 3  | {"c": 3}  |' ]
    [ "${lines[10]}" = '+---+----+-----------+' ]
}

@test "json-new-fmt: merge JSON values" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":1}'), (2, '{"b":2}');
SQL
    dolt add .
    dolt commit -am "added JSON table"
    dolt branch other
    dolt branch another

    dolt sql <<SQL
    UPDATE js SET js = '{"a":11}' WHERE pk = 1;
SQL
    dolt commit -am "made changes on branch main"

    dolt checkout other
    dolt sql <<SQL
    UPDATE js SET js = '{"b":22}' WHERE pk = 2;
SQL
    dolt commit -am "made changes on branch other"

    dolt checkout main
    dolt merge other --no-commit
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"": 11}"' ]
    [ "${lines[2]}" = '2,"{""b"": 22}"' ]
    dolt commit -am "merged other into main"

    # test merge conflicts
    dolt checkout another
    dolt sql <<SQL
    UPDATE js SET js = '{"b":99}' WHERE pk = 2;
SQL
    dolt commit -am "made changes on branch another"

    run dolt merge other -m "merge"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT" ]] || false
    run dolt conflicts resolve --ours js
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"": 1}"' ]
    [ "${lines[2]}" = '2,"{""b"": 99}"' ]
}

    @test "json-new-fmt: merge JSON values with stored procedure" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":1}'), (2, '{"b":2}');
SQL
    dolt add .
    dolt commit -am "added JSON table"
    dolt branch other
    dolt branch another

    dolt sql <<SQL
    UPDATE js SET js = '{"a":11}' WHERE pk = 1;
SQL
    dolt commit -am "made changes on branch main"

    dolt checkout other
    dolt sql <<SQL
    UPDATE js SET js = '{"b":22}' WHERE pk = 2;
SQL
    dolt commit -am "made changes on branch other"

    dolt checkout main
    dolt merge other --no-commit
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"": 11}"' ]
    [ "${lines[2]}" = '2,"{""b"": 22}"' ]
    dolt commit -am "merged other into main"

    # test merge conflicts
    dolt checkout another
    dolt sql <<SQL
    UPDATE js SET js = '{"b":99}' WHERE pk = 2;
SQL
    dolt commit -am "made changes on branch another"

    run dolt merge other -m "merge"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "CONFLICT" ]] || false
    run dolt sql -q "call dolt_conflicts_resolve('--ours', 'js')"
    [ "$status" -eq 0 ]
    run dolt sql -q "SELECT * FROM js;" -r csv
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"": 1}"' ]
    [ "${lines[2]}" = '2,"{""b"": 99}"' ]
}

@test "json-new-fmt: insert value with special characters" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '{"a":"<>&"}');
SQL
    run dolt sql -q "SELECT * FROM js;" -r csv
    echo "output: $output"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"{""a"": ""<>&""}"' ]
}


@test "json-new-fmt: insert array with special characters" {
    dolt sql <<SQL
    CREATE TABLE js (
        pk int PRIMARY KEY,
        js json
    );
    INSERT INTO js VALUES (1, '[{"a":"<>&"}]');
SQL
    run dolt sql -q "SELECT * FROM js;" -r csv
    echo "output: $output"
    [ "$status" -eq 0 ]
    [ "${lines[1]}" = '1,"[{""a"": ""<>&""}]"' ]
}

@test "json-new-fmt: json casting behavior" {
    run dolt sql -r csv <<SQL
SELECT cast('6\n' as JSON);
SQL
    [ $status -eq 0 ]
    [ ${lines[1]} = "6" ]
    # no newline should be returned
    [ ${#lines[@]} = "2" ]

    skip "following assertion fails but is true in MySQL"
    run dolt sql -r csv <<SQL
SELECT cast('{"key":"\\u0068\\u0069"}' as JSON);
SQL
    [ $status -eq 0 ]
    echo ${lines[1]}
    [ ${lines[1]} = '{"key":"hi"}' ]
}

@test "json-new-fmt: round-tripping behavior" {
    dolt sql -q "create table t (pk int primary key, col1 json);"
    dolt commit -Am "add table"

    # test 6\n

    dolt sql -r csv <<SQL
    INSERT INTO t VALUES (1, '6\n')
SQL
    run dolt sql -r csv -q "select pk, col1 from t;"
    [ $status -eq 0 ]
    [[ $output =~ "1,6" ]] || false

    dolt reset --hard

    # test escaped

    dolt sql -q "INSERT INTO t VALUES (2, '{\"key\":\"\\u0068\\u0069\"}');"
    run dolt sql -q "select pk, col1 from t;"
    [ $status -eq 0 ]
    skip "following assertion fails but is true in MySQL"
    [[ $output =~ '2,{"key":"hi"}' ]] || false
}

@test "json-new-fmt: should store json minimally escaped" {
    dolt sql -q "create table t (pk int primary key, col1 json);"
    dolt commit -Am "add table"
    dolt branch other

    dolt sql -q "INSERT INTO t VALUES (1, '{\"key\":\"\\u0068\\u0069\"}');"
    run dolt sql -r csv -q "select pk, col1 from t;"
    [ $status -eq 0 ]
    skip "following assertion fails but is true in MySQL"
    [[ $output =~ '1,{"key":"hi"}' ]] || false
    dolt commit -Am "left"

    dolt checkout other
    dolt sql -q "INSERT INTO t VALUES (1, '{\"key\":\"hi\"}');"
    run dolt sql -r csv -q "select pk, col1 from t;"
    [ $status -eq 0 ]
    [[ $output =~ '1,{"key":"hi"}' ]] || false
    dolt commit -Am "right"

    run dolt diff main
    [ $status -eq 0 ]
    [ ${#lines[@]} = 0 ]
}

