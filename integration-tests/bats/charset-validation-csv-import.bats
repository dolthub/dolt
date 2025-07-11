#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    # Create CSV with mixed valid and invalid UTF-8 data (mimicking customer scenario from issue #8893)
    # Using printf to create actual invalid UTF-8 bytes
    printf "id,name\n" > mixed-charset.csv
    printf "1,DoltLab" >> mixed-charset.csv
    printf "\xAE" >> mixed-charset.csv  # Invalid UTF-8 byte (latin1 ®)
    printf "\n" >> mixed-charset.csv
    printf "2,Invalid UTF8 " >> mixed-charset.csv
    printf "\xAE" >> mixed-charset.csv  # Invalid UTF-8 byte
    printf "\n" >> mixed-charset.csv
    printf "3,Another Invalid " >> mixed-charset.csv
    printf "\xFF\xFE" >> mixed-charset.csv  # Invalid UTF-8 bytes
    printf "\n" >> mixed-charset.csv
    printf "4,Normal Text\n" >> mixed-charset.csv

    # Create schema with UTF8MB4 charset
    cat <<SQL > charset-schema.sql
CREATE TABLE products (
    id int primary key,
    name text character set utf8mb4
);
SQL
}

@test "charset-validation-csv-import: CSV import properly validates charset encoding" {
    dolt sql < charset-schema.sql
    
    # CSV import should show proper charset validation error messages  
    # Use -f to force overwrite and capture stderr with stdout
    run bash -c "echo 'n' | dolt table import -c -f --file-type=csv products mixed-charset.csv 2>&1"
    [[ "$output" =~ "Incorrect string value:" ]] || false || false
    [[ "$output" =~ "for column 'name'" ]] || false || false
    [[ "$output" =~ "at row" ]] || false || false
}

@test "charset-validation-csv-import: demonstrates charset validation during CSV import" {
    dolt sql < charset-schema.sql
    
    # This test shows that dolt's CSV import now properly validates charset
    # and provides meaningful error messages (addressing issue #8893)
    run bash -c "echo 'n' | dolt table import -c -f --file-type=csv products mixed-charset.csv 2>&1"
    
    # Verify that charset validation is working properly
    [[ "$output" =~ "Incorrect string value:" ]] || false || false
    [[ "$output" =~ "for column 'name'" ]] || false || false
}

@test "charset-validation-csv-import: customer scenario with mixed encoding data" {
    dolt sql < charset-schema.sql
    
    # Customer's original problem: had Latin1-encoded data in UTF8MB4 columns
    # This caused issues when trying to query or modify the data
    run bash -c "echo 'n' | dolt table import -c -f --file-type=csv products mixed-charset.csv 2>&1"
    
    # Verify charset validation now properly handles mixed encoding during import
    [[ "$output" =~ "Incorrect string value:" ]] || false || false
    [[ "$output" =~ "for column 'name'" ]] || false || false
    [[ "$output" =~ "at row" ]] || false || false
    
    # Verify error shows the actual problematic data for debugging
    [[ "$output" =~ "DoltLab" ]] || false || false  # Shows the actual data that failed
}

@test "charset-validation-csv-import: error messages show proper charset validation details" {
    dolt sql < charset-schema.sql
    
    # Import should show specific charset validation error details  
    run bash -c "echo 'n' | dolt table import -c -f --file-type=csv products mixed-charset.csv 2>&1"
    
    # Verify error message format matches MySQL charset validation
    [[ "$output" =~ "Incorrect string value:" ]] || false || false
    [[ "$output" =~ "\\x" ]] || false || false  # Should show hex byte format like \xAE
    [[ "$output" =~ "for column 'name'" ]] || false || false
    [[ "$output" =~ "at row" ]] || false || false
}