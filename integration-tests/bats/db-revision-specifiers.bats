#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    export DOLT_DBNAME_REPLACE="true"
    database_name=dolt_repo_$$

    dolt sql -q "CREATE TABLE test(pk int PRIMARY KEY, color varchar(200))"
    dolt add -A
    dolt commit -am "Created table"
    dolt sql -q "INSERT INTO test VALUES (1, 'green');"
    dolt commit -am "Inserted 1, green"
    dolt tag v1
    dolt sql -q "INSERT INTO test VALUES (2, 'blue');"
    dolt commit -am "Inserted 2, blue"
    dolt branch branch1
    dolt sql -q "INSERT INTO test VALUES (3, 'purple');"
    dolt commit -am "Inserted 3, purple"
    dolt sql -q "DELETE FROM test;"
    dolt commit -am "Deleted all rows"
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "db-revision-specifiers: branch-qualified database revisions" {
    # Can be selected as the current database
    run dolt sql -r=csv << SQL
use $database_name/branch1;
select database(), active_branch();
select * from test;
SQL
    [ "$status" -eq "0" ]
    [[ "$output" =~ "1,green" ]] || false
    [[ "$output" =~ "2,blue" ]] || false
    [[ ! "$output" =~ "3,purple" ]] || false

    # Selecting a database revision as the current database includes it in show databases
    run dolt sql -r=csv << SQL
use $database_name/branch1;
show databases;
SQL
    [ "$status" -eq "0" ]
    [[ "$output" =~ "$database_name" ]] || false
    [[ "$output" =~ "$database_name/branch1" ]] || false

    # Can be used as part of a fully qualified table name
    run dolt sql -q "SELECT * FROM \`$database_name/branch1\`.test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "1,green" ]] || false
    [[ "$output" =~ "2,blue" ]] || false
    [[ ! "$output" =~ "3,purple" ]] || false

    # Branch-qualified database revisions are writable
    run dolt sql -r=csv << SQL
use $database_name/branch1;
insert into test values (100, 'beige');
SQL
    [ "$status" -eq "0" ]
    [[ "$output" = "" ]] || false
}

@test "db-revision-specifiers: tag-qualified database revisions" {
    # Can be selected as the current database
    run dolt sql -r=csv << SQL
use $database_name/v1;
select * from test;
SQL
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "Database changed" ]] || false
    [[ "$output" =~ "1,green" ]] || false
    [[ ! "$output" =~ "2,blue" ]] || false
    [[ ! "$output" =~ "3,purple" ]] || false

    # Selecting a database revision as the current database includes it in show databases
    run dolt sql -r=csv << SQL
use $database_name/v1;
show databases;
SQL
    [ "$status" -eq "0" ]
    [[ "$output" =~ "$database_name" ]] || false
    [[ "$output" =~ "$database_name/v1" ]] || false

    # Can be used as part of a fully qualified table name
    run dolt sql -q "SELECT * FROM \`$database_name/v1\`.test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "1,green" ]] || false
    [[ ! "$output" =~ "2,blue" ]] || false
    [[ ! "$output" =~ "3,purple" ]] || false

    # Tag-qualified database revisions are read-only
    run dolt sql -r=csv << SQL
use $database_name/v1;
insert into test values (100, 'beige');
SQL
    [ "$status" -ne "0" ]
    [[ "$output" =~ "$database_name/v1 is read-only" ]] || false
}

@test "db-revision-specifiers: commit-qualified database revisions" {
    commit=$(dolt sql -q "SELECT hashof('HEAD~1');" -r=csv | tail -1)

    # Can be selected as the current database
    run dolt sql -r=csv << SQL
use $database_name/$commit;
select * from test;
SQL
    [ "$status" -eq "0" ]
    [[ ! "$output" =~ "Database changed" ]] || false
    [[ "$output" =~ "1,green" ]] || false
    [[ "$output" =~ "2,blue" ]] || false
    [[ "$output" =~ "3,purple" ]] || false

    # Selecting a database revision as the current database includes it in show databases
    run dolt sql -r=csv << SQL
use $database_name/$commit;
show databases;
SQL
    [ "$status" -eq "0" ]
    [[ "$output" =~ "$database_name" ]] || false
    [[ "$output" =~ "$database_name/$commit" ]] || false

    # Can be used as part of a fully qualified table name
    run dolt sql -q "SELECT * FROM \`$database_name/$commit\`.test" -r=csv
    [ "$status" -eq "0" ]
    [[ "$output" =~ "1,green" ]] || false
    [[ "$output" =~ "2,blue" ]] || false
    [[ "$output" =~ "3,purple" ]] || false

    # Commit-qualified database revisions are read-only
    run dolt sql -r=csv << SQL
use $database_name/$commit;
insert into test values (100, 'beige');
SQL
    [ "$status" -ne "0" ]
    [[ "$output" =~ "$database_name/$commit is read-only" ]] || false
}
