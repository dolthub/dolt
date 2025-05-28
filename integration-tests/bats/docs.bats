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
        doc_name varchar(200) NOT NULL, 
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
