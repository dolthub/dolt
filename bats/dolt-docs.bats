#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "dolt status and ls to view valid docs on dolt init" {
    run ls
    [[ "$output" =~ "LICENSE.md" ]] || false
    [[ "$output" =~ "README.md" ]] || false
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run cat LICENSE.md
    [ "$output" = "This is a repository level LICENSE. Either edit it, add it, and commit it, or remove the file." ]
    run cat README.md
    [ "$output" = "This is a repository level README. Either edit it, add it, and commit it, or remove the file." ]
    touch INVALID.md
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    [[ ! "$output" =~ "INVALID.md" ]] || false
}

@test "dolt add . and dolt commit dolt docs" {
    run dolt status
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    echo testing123 > LICENSE.md
    run cat LICENSE.md
    [ "$output" = "testing123" ]
    run dolt add dolt_docs
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt commit -m "adding license and readme"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "adding license and readme" ]] || false
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "dolt add . and dolt commit dolt docs with another table" {
    run dolt status
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt table create -s `batshelper 1pk5col-ints.schema` test
    [ "$status" -eq 0 ]
    run dolt add test
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt commit -m "adding license and readme, and test table"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "adding license and readme, and test table" ]] || false
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "dolt add LICENSE.md stages license" {
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]]
    [[ "$output" =~ "Untracked files" ]]
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt add LICENSE.md
    [ "$status" -eq 0 ] || false
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt commit -m "license commit"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "license commit" ]] || false
}

@test "dolt add README.md stages readme" {
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt add README.md
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    run dolt commit -m "readme commit"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "readme commit" ]] || false
}

@test "dolt add doesn't add files that are not LICENSE.md or README.md" {
    touch invalid
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt add README.md invalid
    [ "$status" -eq 1 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt add LICENSE.md invalid
    [ "$status" -eq 1 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt add invalid README.md LICENSE.md
    [ "$status" -eq 1 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
}

@test "dolt reset --hard should move doc files to untracked files when there are no doc values on the head commit" {
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt reset --hard 
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run ls
    [[ "$output" =~ "LICENSE.md" ]] || false
    [[ "$output" =~ "README.md" ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt reset --hard
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
}

@test "dolt reset --hard should update doc files on the fs when doc values exist on the head commit" {
    echo license-text > LICENSE.md
    echo readme-text > README.md
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt commit -m "first docs commit"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "first docs commit" ]]
    echo 'updated readme' > README.md
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*README.md) ]] || false
    run cat README.md
    [ "$output" = "updated readme" ]
    run dolt reset --hard
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
    run cat README.md
    [ "$output" = "readme-text" ]
    echo newLicenseText > LICENSE.md
    run dolt table create -s `batshelper 1pk5col-ints.schema` test
    [ "$status" -eq 0 ]
    run dolt add test LICENSE.md
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test) ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*LICENSE.md) ]] || false
    run dolt reset --hard
    [ "$status" -eq 0 ]
    run dolt status
    echo "otuput = $output"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test) ]] || false
    [[ ! "$output" =~ "LICENSE.md" ]] || false
    run cat LICENSE.md
    [ "$output" = "license-text" ]
 }

@test "dolt reset . should remove docs from staging area" {
    echo ~license~ > LICENSE.md
    echo ~readme~ > README.md
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt reset .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt commit -m "initial doc commit"
    [ "$status" -eq 0 ]
    echo ~new-text~ > README.md
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*README.md) ]] || false
    run dolt reset .
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*README.md) ]] || false
    run cat README.md
    [[ "$output" =~ "~new-text~" ]] 
}

@test "dolt reset --soft should remove docs from staging area" {
    echo ~license~ > LICENSE.md
    echo ~readme~ > README.md
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt reset --soft
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt commit -m "initial doc commit"
    [ "$status" -eq 0 ]
    echo ~new-text~ > README.md
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*README.md) ]] || false
    run dolt reset --soft
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*README.md) ]] || false
    run cat README.md
    [[ "$output" =~ "~new-text~" ]] 
}

@test "dolt reset should remove docs from staging area" {
    echo ~license~ > LICENSE.md
    echo ~readme~ > README.md
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt reset
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt commit -m "initial doc commit"
    [ "$status" -eq 0 ]
    echo ~new-text~ > README.md
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*README.md) ]] || false
    run dolt reset
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*README.md) ]] || false
    run cat README.md
    [[ "$output" =~ "~new-text~" ]] 
}

@test "dolt reset <doc> should remove doc from staging area" {
    run dolt add LICENSE.md
    [ "$status" -eq 0 ]
    run dolt status
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    run dolt reset dolt_docs
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt reset LICENSE.md
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt reset LICENSE.md invalid
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Invalid Table(s)" ]] || false
    [[ "$output" =~ "invalid" ]] || false
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    run dolt reset README.md
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    run dolt commit -m "initial license commit"
    [ "$status" -eq 0 ]
    echo new > LICENSE.md
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt reset README.md LICENSE.md
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
}

@test "dolt reset <table> <doc> resets tables and docs from staging area" {
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt table create -s `batshelper 1pk5col-ints.schema` test
    [ "$status" -eq 0 ]
    run dolt add test
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt reset test LICENSE.md README.md
    [ "$status" -eq 0 ]
    run dolt status
    [[ ! "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
}

@test "dolt table * does not allow operations on dolt_docs" {
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt commit -m "First commit of docs"
    [ "$status" -eq 0 ]
    run dolt table cp dolt_docs another_table
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt table create -s=`batshelper 1pk5col-ints.schema` dolt_docs
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt table export dolt_docs test.csv
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt table import dolt_docs -c `batshelper 1pk5col-ints.csv`
    echo "output = $output"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt table mv dolt_docs new
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt table put-row dolt_docs doc_name:new doc_text:new
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt table rm dolt_docs
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt table rm-row dolt_docs LICENSE.md
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt table select dolt_docs
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
}

@test "dolt schema * does not allow operations on dolt_docs" {
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt commit -m "First commit of docs"
    [ "$status" -eq 0 ]
    run dolt schema add-column dolt_docs type string
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt schema drop-column dolt_docs doc_text string
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt schema export dolt_docs export.schema
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt schema export dolt_docs export.schema
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt schema import -c --pks=pk dolt_docs `batshelper 1pk5col-ints.csv`
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt schema rename-column dolt_docs doc_text something_else
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    run dolt schema show dolt_docs
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
}

 @test "dolt ls should not show dolt_docs table" {
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "dolt_docs" ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "dolt_docs" ]] || false
    run dolt commit -m "First commit of docs"
    [ "$status" -eq 0 ]
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "dolt_docs" ]] || false
 }


# TO DO: Expose dolt_docs for read commands
@test "dolt sql does not expose dolt_docs" {
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "dolt_docs" ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run dolt commit -m "initial doc commits"
    [ "$status" -eq 0 ]
    
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "dolt_docs" ]] || false

    run dolt sql -q "INSERT INTO dolt_docs VALUES (new_doc, new_text)"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found: dolt_docs" ]] || false

    run dolt sql -q "DELETE FROM dolt_docs WHERE pk=REAMDE.md"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found: dolt_docs" ]] || false

    run dolt sql -q "UPDATE dolt_docs SET pk=NotValid WHERE pk=README.md"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found: dolt_docs" ]] || false

    run dolt sql -q "SELECT * FROM dolt_docs"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "table not found: dolt_docs" ]] || false

    run dolt sql -q "CREATE TABLE dolt_docs (doc_name TEXT, doc_text LONGTEXT, PRIMARY KEY(doc_name))"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Invalid table name: 'dolt_docs'" ]] || false
}

# @test "dolt diff shows diffs between working root and file system docs" {

# }

@test "dolt branch/merge with conflicts for docs" {
    dolt add .
    dolt commit -m "Committing initial docs"
    dolt branch test-a
    dolt branch test-b
    dolt checkout test-a
    echo test-a branch > README.md
    dolt add .
    dolt commit -m "Changed README.md on test-a branch"
    dolt checkout test-b
    run cat README.md
    skip "This does not change the contents of README.md to what is stored on test-b right now. Keeps what is on test-a"
    [[ $output =~ "This is a repository level README" ]] || false
    [[ !$output =~ "test-a branch" ]] || false
    echo test-b branch > README.md
    dolt add .
    dolt commit -m "Changed README.md on test-a branch"
    dolt checkout master
    run dolt merge test-a
    [ "$status" -eq 0 ]
    [[ $output =~ "Fast-forward" ]] || false
    run dolt merge test-b
    [ "$status" -eq 1 ]
    [[ $output =~ "CONFLICT" ]] || false
    run dolt conflicts cat dolt_docs
    [ "$status" -eq 0 ]
    [[ $output =~ "test-a branch" ]] || false
    [[ $output =~ "test-b branch" ]] || false
    dolt conflicts resolve dolt_docs --ours
    run cat README.md
    [[ $output =~ "test-b branch" ]] || false
    [[ !$output =~ "test-a branch" ]] || false
    dolt add .
    dolt commit -m "Resolved docs conflict with --ours"
    # Again but resolve theirs
    dolt branch test-a-again
    dolt branch test-b-again
    dolt checkout test-a-again
    echo test-a-again branch > README.md
    dolt add .
    dolt commit -m "Changed README.md on test-a-again branch"
    dolt checkout test-b-again
    echo test-b-again branch > README.md
    dolt add .
    dolt commit -m "Changed README.md on test-b-again branch"
    dolt merge test-a-again
    run dolt merge test-b-again
    [ "$status" -eq 1 ]
    dolt conflicts resolve dolt_docs --theirs
    run cat README.md
    [[ $output =~ "test-a-again branch" ]] || false
    [[ !$output =~ "test-b-again branch" ]] || false
}
