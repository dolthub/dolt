#!/usr/bin/env bats

# Assert that all bash test constructs will correctly fail on Mac OS's older bash version
# by ending them with '|| false'.
# https://github.com/sstephenson/bats/issues/49
@test "bats: all bash test constructs end with '|| false' {
    run grep -E ' *\]\][[:space:]]*$' -n *.bats
    # grep returns 0 if matches are found, 1 if matches are NOT found, and 2 if no input files were found
    echo -e "Incorrect bash test constructs: \n$output"
    [ $status -eq 1 ]
}