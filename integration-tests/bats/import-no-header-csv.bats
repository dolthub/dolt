#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    # Create a CSV file with no header row
    cat <<DELIM > no-header.csv
1,John,Doe,35,New York
2,Jane,Smith,28,Los Angeles
3,Bob,Johnson,42,Chicago
DELIM
}

teardown() {
    teardown_common
}

@test "import-no-header-csv: import with --no-header and --columns options" {
    # Test regular import with no header (first row is row of data; not column names)
    run dolt table import -c --no-header --columns id,first_name,last_name,age,city people no-header.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully" ]] || false

    # Verify the data was imported correctly, including the first row
    run dolt sql -q "SELECT COUNT(*) FROM people"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false

    # Verify that column names came from --columns option, not from first row
    run dolt sql -q "DESCRIBE people"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "id" ]] || false
    [[ "$output" =~ "first_name" ]] || false
    [[ "$output" =~ "last_name" ]] || false
    [[ "$output" =~ "age" ]] || false
    [[ "$output" =~ "city" ]] || false

    # Verify the first row was imported as data
    run dolt sql -q "SELECT * FROM people WHERE id = 1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "John" ]] || false
    [[ "$output" =~ "Doe" ]] || false
    [[ "$output" =~ "35" ]] || false
    [[ "$output" =~ "New York" ]] || false
}

@test "import-no-header-csv: import with --no-header but without --columns (error case)" {
    # Should fail with a helpful error message for create
    run dolt table import -c --no-header people no-header.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "must also specify --columns" ]] || false
    [[ "$output" =~ "create table" ]] || false

    # Create a table for update test
    dolt sql -q "CREATE TABLE existing_table (id int, first_name varchar(255), last_name varchar(255), age int, city varchar(255))"

    # Should also fail with update operations but with a different message
    run dolt table import -u --no-header existing_table no-header.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "must also specify --columns" ]] || false
    [[ "$output" =~ "existing tables" ]] || false
}

@test "import-no-header-csv: import with --no-header and --columns for existing table" {
    # Create a table first
    dolt sql -q "CREATE TABLE existing_table2 (id int, first_name varchar(255), last_name varchar(255), age int, city varchar(255))"

    # Import into existing table with no-header and columns
    run dolt table import -u --no-header --columns id,first_name,last_name,age,city existing_table2 no-header.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully" ]] || false

    # Verify the data was imported correctly
    run dolt sql -q "SELECT COUNT(*) FROM existing_table2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false

    # Verify the first row data is in the table
    run dolt sql -q "SELECT * FROM existing_table2 WHERE id = 1"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "John" ]] || false
    [[ "$output" =~ "35" ]] || false
}

@test "import-no-header-csv: import without --no-header (original behavior)" {
    # Test regular import without no-header (should use first row as header)
    run dolt table import -c people no-header.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully" ]] || false

    # Verify that column names came from first row
    run dolt sql -q "DESCRIBE people"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "1" ]] || false
    [[ "$output" =~ "John" ]] || false
    [[ "$output" =~ "Doe" ]] || false
    [[ "$output" =~ "35" ]] || false

    # Verify that first row was NOT imported as data
    run dolt sql -q "SELECT COUNT(*) FROM people"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false
}

@test "import-no-header-csv: import with --columns but without --no-header (override column names)" {
    # Test import with columns option but without no-header flag
    # This should use the custom column names instead of the names from the first row
    run dolt table import -c --columns col1,col2,col3,col4,col5 with_columns_table no-header.csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully" ]] || false

    # Verify that column names came from --columns option, not from first row
    run dolt sql -q "DESCRIBE with_columns_table"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "col1" ]] || false
    [[ "$output" =~ "col2" ]] || false
    [[ "$output" =~ "col3" ]] || false
    [[ "$output" =~ "col4" ]] || false
    [[ "$output" =~ "col5" ]] || false

    # Verify that first row was NOT imported as data (still treated as header)
    run dolt sql -q "SELECT COUNT(*) FROM with_columns_table"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false

    # Verify second row was imported as data
    run dolt sql -q "SELECT * FROM with_columns_table WHERE col1 = 2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Jane" ]] || false
    [[ "$output" =~ "Smith" ]] || false
    [[ "$output" =~ "28" ]] || false
    [[ "$output" =~ "Los Angeles" ]] || false
}

@test "import-no-header-csv: import from stdin with --create-table requires schema file" {
    # Test importing from stdin with --create-table but without a schema file
    # This should fail with a specific error message
    run bash -c "cat no-header.csv | dolt table import -c --no-header --columns id,first_name,last_name,age,city stdin_table"
    [ "$status" -eq 1 ]
    
    # Check for a specific error message about schema
    [[ "$output" =~ "fatal: when importing from stdin with --create-table, you must provide a schema file with --schema" ]] || false
    
    # Verify that trying to use stdin with --create-table and --columns but without --schema also fails
    run bash -c "cat no-header.csv | dolt table import -c --columns id,first_name,last_name,age,city stdin_table"
    [ "$status" -eq 1 ]
    
    # Check for the same error message
    [[ "$output" =~ "fatal: when importing from stdin with --create-table, you must provide a schema file with --schema" ]] || false
    
    # Show that the workaround is to create the table first, then import with -u
    # Create the table
    dolt sql -q "CREATE TABLE stdin_table (id int PRIMARY KEY, first_name text, last_name text, age int, city text)"
    
    # Import data with -u instead of -c
    run bash -c "cat no-header.csv | dolt table import -u --no-header --columns id,first_name,last_name,age,city stdin_table"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Import completed successfully" ]] || false
    
    # Verify the import worked
    run dolt sql -q "SELECT COUNT(*) FROM stdin_table"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "3" ]] || false
}