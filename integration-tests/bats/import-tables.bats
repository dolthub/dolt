#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "import-tables: error if no operation is provided" {
    run dolt table import t test.csv

    [ "$status" -eq 1 ]
    [[ "$output" =~ "Must specify exactly one of -c, -u, -a, or -r." ]] || false
}

@test "import-tables: error if multiple operations are provided" {
    run dolt table import -c -u -r t test.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Must specify exactly one of -c, -u, -a, or -r." ]] || false
}

@test "import-tables: import tables where field names need to be escaped" {
    dolt table import -c t `batshelper escaped-characters.csv`

    run dolt sql -q "show create table t;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '`--Key1` int' ]] || false
    [[ "$output" =~ '`"Key2` int' ]] || false
    [[ "$output" =~ '`'"'"'Key3` int' ]] || false
    [[ "$output" =~ '```Key4` int' ]] || false
    [[ "$output" =~ '`/Key5` int' ]] || false

    run dolt sql -q "select * from t;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ '| --Key1 | "Key2 | '"'"'Key3 | `Key4 | /Key5 |' ]] || false
    [[ "$output" =~ '| 0      | 1     | 2     | 3     | 4     |' ]] || false
}

@test "import-tables: import tables where primary key names need to be escaped" {
    dolt table import -c -pk "--Key1" t `batshelper escaped-characters.csv`
    dolt table import -r -pk '"Key2' t `batshelper escaped-characters.csv`
    dolt table import -r -pk "'Key3" t `batshelper escaped-characters.csv`
    dolt table import -r -pk '`Key4' t `batshelper escaped-characters.csv`
    dolt table import -r -pk "/Key5" t `batshelper escaped-characters.csv`
}

@test "import-tables: validate primary keys exist in CSV file (issue #1083)" {
    # Create a test CSV file
    cat <<DELIM > test_pk_validation.csv
id,name,email,age
1,Alice,alice@example.com,30
2,Bob,bob@example.com,25
3,Charlie,charlie@example.com,35
DELIM

    # Test 1: Invalid single primary key should fail immediately
    run dolt table import -c --pk "invalid_column" test_table test_pk_validation.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "primary key 'invalid_column' not found" ]] || false
    [[ "$output" =~ "Available columns: id, name, email, age" ]] || false

    # Test 2: Multiple invalid primary keys should fail immediately
    run dolt table import -c --pk "invalid1,invalid2" test_table test_pk_validation.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "primary keys \[invalid1 invalid2\] not found" ]] || false
    [[ "$output" =~ "Available columns: id, name, email, age" ]] || false

    # Test 3: Mix of valid and invalid primary keys should fail
    run dolt table import -c --pk "id,invalid_col,name" test_table test_pk_validation.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "primary key 'invalid_col' not found" ]] || false
    [[ "$output" =~ "Available columns: id, name, email, age" ]] || false

    # Test 4: Valid primary key should succeed
    run dolt table import -c --pk "id" test_table test_pk_validation.csv
    [ "$status" -eq 0 ]
    
    # Verify table was created correctly
    run dolt sql -q "DESCRIBE test_table;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "id" ]] || false
    [[ "$output" =~ "PRI" ]] || false

    # Test 5: Valid multiple primary keys should succeed
    rm -f test_table
    run dolt table import -c --pk "id,name" test_table2 test_pk_validation.csv
    [ "$status" -eq 0 ]
    
    # Test 6: PSV file with invalid primary key should also fail immediately
    cat <<DELIM > test_pk_validation.psv
id|name|email|age
1|Alice|alice@example.com|30
2|Bob|bob@example.com|25
3|Charlie|charlie@example.com|35
DELIM

    run dolt table import -c --pk "nonexistent" test_table3 test_pk_validation.psv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "primary key 'nonexistent' not found" ]] || false
    [[ "$output" =~ "Available columns: id, name, email, age" ]] || false

    # Test 7: Large CSV should fail quickly (not after reading entire file)
    # Create a larger CSV to simulate the original issue
    {
        echo "year,state_fips,county_fips,precinct,candidate,votes"
        for i in {1..1000}; do
            echo "2020,$i,$i,precinct$i,candidate$i,$i"
        done
    } > large_test.csv

    # Time the command - it should fail immediately, not after processing all rows
    start_time=$(date +%s)
    run dolt table import -c --pk "year,state_fips,county_fips,precinct,invalid_column" precinct_results large_test.csv
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    [ "$status" -eq 1 ]
    [[ "$output" =~ "primary key 'invalid_column' not found" ]] || false
    [[ "$output" =~ "Available columns: year, state_fips, county_fips, precinct, candidate, votes" ]] || false
    # Should fail in less than 2 seconds (immediate validation)
    [ "$duration" -lt 2 ] || false

    # Cleanup
    rm -f test_pk_validation.csv test_pk_validation.psv large_test.csv
}

@test "import-tables: primary key validation with schema file should skip validation" {
    # Create a test CSV file
    cat <<DELIM > test_data.csv
id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
DELIM

    # Create a schema file with different column as primary key
    cat <<SQL > test_schema.sql
CREATE TABLE test_with_schema (
  id INT,
  name VARCHAR(100),
  email VARCHAR(100),
  PRIMARY KEY (name)
);
SQL

    # Even though 'name' is specified as PK in command line, schema file takes precedence
    # and validation should be skipped
    run dolt table import -c --pk "nonexistent_column" --schema test_schema.sql test_with_schema test_data.csv
    [ "$status" -eq 0 ]
    
    # Verify that 'name' is the primary key (from schema file)
    run dolt sql -q "SHOW CREATE TABLE test_with_schema;"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "PRIMARY KEY (\`name\`)" ]] || false

    # Cleanup
    rm -f test_data.csv test_schema.sql
}
