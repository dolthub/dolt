#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    cat <<TXT > README.md
# Dolt is Git for Data!

Dolt is a SQL database that you can fork, clone, branch, merge, push
and pull just like a git repository. Connect to Dolt just like any
MySQL database to run queries or update the data using SQL
commands. Use the command line interface to import CSV files, commit
your changes, push them to a remote, or merge your teammate's changes.

All the commands you know for Git work exactly the same for Dolt. Git
versions files, Dolt versions tables. It's like Git and MySQL had a
baby.

We also built [DoltHub](https://www.dolthub.com), a place to share
Dolt databases. We host public data for free. If you want to host
your own version of DoltHub, we have [DoltLab](https://www.doltlab.com). If you want us to run a Dolt server for you, we have [Hosted Dolt](https://hosted.doltdb.com).
TXT

    cat <<TXT > LICENSE.md
        DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
                    Version 2, December 2004

 Copyright (C) 2004 Sam Hocevar <sam@hocevar.net>

 Everyone is permitted to copy and distribute verbatim or modified
 copies of this license document, and changing it is allowed as long
 as the name is changed.

            DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
   TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION

  0. You just DO WHAT THE FUCK YOU WANT TO.
TXT

}

teardown() {
    assert_feature_version
    teardown_common
}

@test "docs: doc read creates dolt_docs table" {
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! $output =~ "dolt_docs" ]] || false

    dolt docs upload README.md README.md
    run dolt docs upload README.md README.md
    [ "$status" -eq 0 ]

    run dolt status
    [ "$status" -eq 0 ]
    [[ $output =~ "dolt_docs" ]] || false
}

@test "docs: doc read outputs doc correctly" {
    dolt docs upload LICENSE.md LICENSE.md

    dolt docs print LICENSE.md > other.md
    diff LICENSE.md other.md
    run diff LICENSE.md other.md
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" = "0" ]] || false
}

@test "docs: docs can be staged" {
    dolt docs upload LICENSE.md LICENSE.md
    dolt add .

    dolt status
    run dolt status
    [ "$status" -eq 0 ]
}

@test "docs: doc can be committed" {
    dolt docs upload LICENSE.md LICENSE.md
    dolt add .

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "new table:        dolt_docs" ]] || false

    dolt commit -am "added a license file"

    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "docs: docs are available from SQL" {
    run dolt sql -q "SELECT * FROM dolt_docs"
    [ "$status" -eq 0 ]

    dolt docs upload LICENSE.md LICENSE.md
    dolt sql -q "SELECT doc_name FROM dolt_docs" -r csv
    run dolt sql -q "SELECT doc_name FROM dolt_docs" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "doc_name" ]] || false
    [[ "$output" =~ "LICENSE.md" ]] || false
}

@test "docs: docs can be created from SQL" {
    run dolt sql -q "CREATE TABLE dolt_docs (x int);"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Invalid table name dolt_docs" ]] || false

    run dolt sql -q "CREATE TABLE dolt_docs (
        doc_name varchar(1023) NOT NULL, 
        doc_text longtext, 
        PRIMARY KEY (doc_name)
    );"
    [ "$status" -ne 0 ]
    [[ "$output" =~ "Invalid table name dolt_docs" ]] || false

    dolt sql -q "INSERT INTO dolt_docs VALUES ('README.md', 'this is a README')"

    run dolt sql -q "SELECT * FROM dolt_docs"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "this is a README" ]] || false
}

@test "docs: docs diff" {
    dolt docs upload LICENSE.md LICENSE.md
    dolt add -A && dolt commit -m "added LICENSE"

    cat <<TXT > LICENSE.md
        DO WHAT THE F*CK YOU WANT TO PUBLIC LICENSE
                    Version 2, December 2004

 Copyright (C) 2004 Sam Hocevar <sam@hocevar.net>

 Everyone is permitted to copy and distribute verbatim or modified
 copies of this license document, and changing it is allowed as long
 as the name is changed.

            DO WHAT THE F*CK YOU WANT TO PUBLIC LICENSE
   TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION

  0. You just DO WHAT THE F*CK YOU WANT TO
TXT

    dolt docs upload LICENSE.md LICENSE.md
    run dolt docs diff LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "-        DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE"      ]] || false
    [[ "$output" =~ "+        DO WHAT THE F*CK YOU WANT TO PUBLIC LICENSE"      ]] || false
    [[ "$output" =~ "-            DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE"  ]] || false
    [[ "$output" =~ "+            DO WHAT THE F*CK YOU WANT TO PUBLIC LICENSE"  ]] || false
    [[ "$output" =~ "-  0. You just DO WHAT THE FUCK YOU WANT TO"               ]] || false
    [[ "$output" =~ "+  0. You just DO WHAT THE F*CK YOU WANT TO"               ]] || false
}

# AGENT document tests
@test "docs: AGENT document is created automatically during init" {
    # Create a fresh repo to test init behavior
    cd ..
    rm -rf test-agent-init
    mkdir test-agent-init
    cd test-agent-init
    
    # Initialize new repo
    dolt init
    
    # Check that AGENT.md document exists
    run dolt docs print AGENT.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "# Dolt Database Repository" ]] || false
    [[ "$output" =~ "This directory contains a Dolt database" ]] || false
    [[ "$output" =~ "About Dolt" ]] || false
    [[ "$output" =~ "Quick Start" ]] || false
    [[ "$output" =~ "dolt sql" ]] || false
    [[ "$output" =~ "dolt add" ]] || false
    [[ "$output" =~ "dolt commit" ]] || false
    [[ "$output" =~ "dolt diff" ]] || false
    [[ "$output" =~ "https://doltdb.com" ]] || false
    
    # Verify AGENT.md is in the docs table
    run dolt sql -q "SELECT doc_name FROM dolt_docs WHERE doc_name = 'AGENT.md'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "AGENT.md" ]] || false
    
    # CRITICAL: Verify there's no diff after init (clean working tree)
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ] || false
    
    # Also verify status shows clean working tree
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
    
    # Clean up
    cd ../dolt-repo-$$
}

@test "docs: AGENT document print functionality" {
    # Check basic print functionality
    run dolt docs print AGENT.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "# Dolt Database Repository" ]] || false
    [[ "$output" =~ "SQL database with Git-like version control" ]] || false
    
    # Check that it contains expected sections
    [[ "$output" =~ "## About Dolt" ]] || false
    [[ "$output" =~ "## Quick Start" ]] || false
    [[ "$output" =~ "## Common Commands" ]] || false
    [[ "$output" =~ "## SQL + Version Control" ]] || false
}

@test "docs: AGENT document can be uploaded and modified" {
    # Create a custom AGENT.md file
    cat <<TXT > custom_agent.md
# Custom Agent Documentation

This is a custom agent document for testing.

## Custom Section

- Custom command 1
- Custom command 2

Visit: https://example.com
TXT

    # Upload the custom document
    dolt docs upload AGENT.md custom_agent.md
    
    # Verify the upload worked
    run dolt docs print AGENT.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "# Custom Agent Documentation" ]] || false
    [[ "$output" =~ "This is a custom agent document for testing" ]] || false
    [[ "$output" =~ "Custom command 1" ]] || false
    [[ "$output" =~ "https://example.com" ]] || false
    
    # Verify original content is replaced
    [[ ! "$output" =~ "# Dolt Database Repository" ]] || false
}

@test "docs: AGENT document diff functionality" {
    # First, stage and commit the initial AGENT.md
    dolt add .
    dolt commit -m "Initial commit with AGENT.md"
    
    # Create a modified AGENT.md
    cat <<TXT > modified_agent.md
# Modified Dolt Database Repository

This directory contains a modified Dolt database.

## About Dolt - Modified

Dolt is a modified SQL database with Git-like version control.

## Modified Quick Start

- **Modified Access**: Use \`dolt sql\` to start an interactive SQL shell
- **Modified version control**: Dolt commands work like Git commands

For more information, visit: https://modified.example.com
TXT

    # Upload the modified document
    dolt docs upload AGENT.md modified_agent.md
    
    # Test diff functionality
    run dolt docs diff AGENT.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "-# Dolt Database Repository" ]] || false
    [[ "$output" =~ "+# Modified Dolt Database Repository" ]] || false
    [[ "$output" =~ "-This directory contains a Dolt database" ]] || false
    [[ "$output" =~ "+This directory contains a modified Dolt database" ]] || false
    [[ "$output" =~ "-https://doltdb.com" ]] || false
    [[ "$output" =~ "+https://modified.example.com" ]] || false
}

@test "docs: AGENT document export to CLAUDE.md functionality" {
    # Export AGENT.md to CLAUDE.md
    dolt docs print AGENT.md > CLAUDE.md
    
    # Verify the file was created
    [ -f CLAUDE.md ]
    
    # Verify the content matches
    run cat CLAUDE.md
    [[ "$output" =~ "# Dolt Database Repository" ]] || false
    [[ "$output" =~ "This directory contains a Dolt database" ]] || false
    [[ "$output" =~ "About Dolt" ]] || false
    [[ "$output" =~ "dolt sql" ]] || false
    [[ "$output" =~ "https://doltdb.com" ]] || false
    
    # Compare with direct docs print
    dolt docs print AGENT.md > direct_output.md
    run diff CLAUDE.md direct_output.md
    [ "$status" -eq 0 ]
    [[ "${#lines[@]}" = "0" ]] || false
}

@test "docs: AGENT document is available from SQL" {
    # Check that AGENT.md is available via SQL
    run dolt sql -q "SELECT doc_name FROM dolt_docs WHERE doc_name = 'AGENT.md'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "AGENT.md" ]] || false
    
    # Check that we can query the content
    run dolt sql -q "SELECT doc_text FROM dolt_docs WHERE doc_name = 'AGENT.md'" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "# Dolt Database Repository" ]] || false
    [[ "$output" =~ "SQL database with Git-like version control" ]] || false
}

@test "docs: AGENT document can be modified via SQL" {
    # Modify AGENT.md via SQL
    dolt sql -q "UPDATE dolt_docs SET doc_text = '# SQL Modified Agent Doc\n\nThis was modified via SQL.' WHERE doc_name = 'AGENT.md'"
    
    # Verify the modification
    run dolt docs print AGENT.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "# SQL Modified Agent Doc" ]] || false
    [[ "$output" =~ "This was modified via SQL" ]] || false
    
    # Verify original content is gone
    [[ ! "$output" =~ "# Dolt Database Repository" ]] || false
}

@test "docs: AGENT document validation works correctly" {
    # Test valid document name
    run dolt docs upload AGENT.md README.md
    [ "$status" -eq 0 ]
    
    # Test invalid document name should fail
    run dolt docs upload INVALID.md README.md
    [ "$status" -ne 0 ]
    [[ "$output" =~ "invalid doc name" ]] || false
    [[ "$output" =~ "valid names are" ]] || false
    [[ "$output" =~ "AGENT.md" ]] || false
    [[ "$output" =~ "LICENSE.md" ]] || false
    [[ "$output" =~ "README.md" ]] || false
}

@test "docs: AGENT document can be staged and committed" {
    # Modify AGENT.md
    cat <<TXT > test_agent.md
# Test Agent Document

This is a test modification.
TXT

    dolt docs upload AGENT.md test_agent.md
    
    # Check that dolt_docs table shows up in status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "dolt_docs" ]] || false
    
    # Stage and commit
    dolt add .
    dolt commit -m "Modified AGENT.md"
    
    # Verify clean status
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
    
    # Verify the content persists
    run dolt docs print AGENT.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "# Test Agent Document" ]] || false
    [[ "$output" =~ "This is a test modification" ]] || false
}

@test "docs: AGENT document works with multiple document types" {
    # Upload all three document types
    dolt docs upload README.md README.md
    dolt docs upload LICENSE.md LICENSE.md
    # AGENT.md is already created during init
    
    # Verify all three exist
    run dolt sql -q "SELECT doc_name FROM dolt_docs ORDER BY doc_name" -r csv
    [ "$status" -eq 0 ]
    [[ "$output" =~ "AGENT.md" ]] || false
    [[ "$output" =~ "LICENSE.md" ]] || false
    [[ "$output" =~ "README.md" ]] || false
    
    # Verify each can be printed
    run dolt docs print AGENT.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "# Dolt Database Repository" ]] || false
    
    run dolt docs print README.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "# Dolt is Git for Data!" ]] || false
    
    run dolt docs print LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE" ]] || false
}
