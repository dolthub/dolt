#!/usr/bin/env bats


@test "bats: all assertions work on Mac" {
    run pwd
    echo "PWD: $output"

    run grep -E ' *\]\][[:space:]]*$' -n *.bats
    # grep returns 0 if matches are found, 1 if matches are NOT found, and 2 if no input files were found
    echo -e "Incorrect bash test constructs: \n$output"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "No tables to export." ]]
    [[ "$output" =~ "another" ]]
}