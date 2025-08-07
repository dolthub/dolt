#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "author-committer: basic commit preserves backward compatibility" {
    dolt sql -q "CREATE TABLE test1 (id INT PRIMARY KEY, name VARCHAR(50));"
    dolt add test1
    run dolt commit -m "Test basic commit"
    [ "$status" -eq 0 ]
    
    # Check that author and committer are the same for basic commits
    run dolt sql -q "SELECT author, committer FROM dolt_commits WHERE message='Test basic commit'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ Bats\ Tests,Bats\ Tests ]]
}

@test "author-committer: --author flag works as before" {
    dolt sql -q "CREATE TABLE test2 (id INT PRIMARY KEY);"
    dolt add test2
    run dolt commit -m "Test author flag" --author "Custom Author <custom@example.com>"
    [ "$status" -eq 0 ]
    
    # Check author is set correctly
    run dolt sql -q "SELECT author, author_email FROM dolt_commits WHERE message='Test author flag'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ Custom\ Author,custom@example.com ]]
}

@test "author-committer: --committer flag sets committer separately from author" {
    dolt sql -q "CREATE TABLE test3 (id INT PRIMARY KEY);"
    dolt add test3
    run dolt commit -m "Test committer flag" --author "Original Author <original@example.com>" --committer "Different Committer <committer@example.com>"
    [ "$status" -eq 0 ]
    
    # Check both author and committer are set correctly
    run dolt sql -q "SELECT author, author_email, committer, committer_email FROM dolt_commits WHERE message='Test committer flag'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ Original\ Author,original@example.com,Different\ Committer,committer@example.com ]]
}

@test "author-committer: --committer-date flag sets custom committer date" {
    dolt sql -q "CREATE TABLE test4 (id INT PRIMARY KEY);"
    dolt add test4
    
    # Set a specific date in the past
    CUSTOM_DATE="2023-01-15T10:30:00"
    run dolt commit -m "Test committer date" --committer-date "$CUSTOM_DATE"
    [ "$status" -eq 0 ]
    
    # Verify the committer date was set (checking format since exact match might have timezone differences)
    run dolt sql -q "SELECT committer_date FROM dolt_commits WHERE message='Test committer date'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ 2023-01-15 ]]
}

@test "author-committer: DOLT_AUTHOR_DATE environment variable sets author date" {
    dolt sql -q "CREATE TABLE test5 (id INT PRIMARY KEY);"
    dolt add test5
    
    # Set author date via environment variable
    DOLT_AUTHOR_DATE="2022-06-20T14:45:00" run dolt commit -m "Test author date env"
    [ "$status" -eq 0 ]
    
    # Verify the author date was set
    run dolt sql -q "SELECT author_date FROM dolt_commits WHERE message='Test author date env'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ 2022-06-20 ]]
}

@test "author-committer: DOLT_COMMITTER_DATE environment variable sets committer date" {
    dolt sql -q "CREATE TABLE test6 (id INT PRIMARY KEY);"
    dolt add test6
    
    # Set committer date via environment variable
    DOLT_COMMITTER_DATE="2021-12-25T09:00:00" run dolt commit -m "Test committer date env"
    [ "$status" -eq 0 ]
    
    # Verify the committer date was set
    run dolt sql -q "SELECT committer_date FROM dolt_commits WHERE message='Test committer date env'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ 2021-12-25 ]]
}

@test "author-committer: dolt_commits table has all new columns" {
    dolt sql -q "CREATE TABLE test7 (id INT PRIMARY KEY);"
    dolt add test7
    dolt commit -m "Test schema"
    
    # Check that all new columns exist in dolt_commits
    run dolt sql -q "SELECT author, author_email, author_date, committer, committer_email, committer_date FROM dolt_commits LIMIT 1" -r csv
    [ "$status" -eq 0 ]
    
    # Verify column headers are present
    [[ "$output" =~ author,author_email,author_date,committer,committer_email,committer_date ]]
}

@test "author-committer: backward compatibility columns still work" {
    dolt sql -q "CREATE TABLE test8 (id INT PRIMARY KEY);"
    dolt add test8
    dolt commit -m "Test backward compat"
    
    # Old column names should still work
    run dolt sql -q "SELECT committer, email, date FROM dolt_commits WHERE message='Test backward compat'" -r csv
    [ "$status" -eq 0 ]
    
    # These should map to author fields for backward compatibility
    [[ "$output" =~ Bats\ Tests,bats@email.fake ]]
}

@test "author-committer: SQL commit procedure with committer parameters" {
    dolt sql -q "CREATE TABLE test9 (id INT PRIMARY KEY);"
    dolt sql -q "CALL dolt_add('test9');"
    
    # Test SQL commit with author and committer
    run dolt sql -q "CALL dolt_commit('-m', 'SQL commit test', '--author', 'SQL Author <sql.author@example.com>', '--committer', 'SQL Committer <sql.committer@example.com>');"
    [ "$status" -eq 0 ]
    
    # Verify both were set
    run dolt sql -q "SELECT author, author_email, committer, committer_email FROM dolt_commits WHERE message='SQL commit test'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ SQL\ Author,sql.author@example.com,SQL\ Committer,sql.committer@example.com ]]
}

@test "author-committer: dolt log shows author information" {
    dolt sql -q "CREATE TABLE test10 (id INT PRIMARY KEY);"
    dolt add test10
    dolt commit -m "Test log display" --author "Log Author <log@example.com>"
    
    # Check that dolt log shows the author
    run dolt log -n 1
    [ "$status" -eq 0 ]
    [[ "$output" =~ Author:\ Log\ Author\ \<log@example.com\> ]]
}

@test "author-committer: committer defaults to author when not specified" {
    dolt sql -q "CREATE TABLE test11 (id INT PRIMARY KEY);"
    dolt add test11
    dolt commit -m "Test default committer" --author "Only Author <only@example.com>"
    
    # When only author is specified, committer should default to author
    run dolt sql -q "SELECT author, committer FROM dolt_commits WHERE message='Test default committer'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ Only\ Author,Only\ Author ]]
}

@test "author-committer: invalid date format shows error" {
    dolt sql -q "CREATE TABLE test12 (id INT PRIMARY KEY);"
    dolt add test12
    
    # Test invalid date format
    run dolt commit -m "Test invalid date" --committer-date "invalid-date-format"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "not in a supported format" ]]
}

@test "author-committer: committer without author uses config values" {
    dolt sql -q "CREATE TABLE test13 (id INT PRIMARY KEY);"
    dolt add test13
    
    # Set only committer, author should come from config
    run dolt commit -m "Test committer only" --committer "Only Committer <onlycommit@example.com>"
    [ "$status" -eq 0 ]
    
    # Author should be from config, committer should be custom
    run dolt sql -q "SELECT author, committer FROM dolt_commits WHERE message='Test committer only'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ Bats\ Tests,Only\ Committer ]]
}

@test "author-committer: environment variables work with SQL commands" {
    dolt sql -q "CREATE TABLE test14 (id INT PRIMARY KEY);"
    dolt sql -q "CALL dolt_add('test14');"
    
    # Test that environment variables work with SQL commits
    DOLT_AUTHOR_DATE="2020-01-01T12:00:00" run dolt sql -q "CALL dolt_commit('-m', 'SQL with env var');"
    [ "$status" -eq 0 ]
    
    # Check that the date was set correctly
    run dolt sql -q "SELECT author_date FROM dolt_commits WHERE message='SQL with env var'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ 2020-01-01 ]]
}