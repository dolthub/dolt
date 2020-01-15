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
    echo testing123 > LICENSE.md
    run dolt add dolt_docs
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    dolt add .
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
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
    rm LICENSE.md
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ deleted:[[:space:]]*LICENSE.md ]] || false
    dolt add .
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*deleted:[[:space:]]*LICENSE.md) ]] || false
    dolt commit -m "delete license"
    run ls
    [[ ! "$output" =~ "LICENSE.md" ]] || false
}

@test "dolt add . and dolt commit dolt docs with another table" {
    dolt add .
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    dolt table create -s `batshelper 1pk5col-ints.schema` test
    dolt add test
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    dolt commit -m "adding license and readme, and test table"
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch master" ]] || false
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
}

@test "dolt add LICENSE.md stages license" {
    dolt add LICENSE.md
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
    dolt add README.md
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
    run dolt add README.md invalid
    [ "$status" -eq 1 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    [[ ! "$output" =~ "invalid" ]] || false
    
    run dolt add invalid LICENSE.md
    [ "$status" -eq 1 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ ! "$output" =~ "invalid" ]] || false
    
    run dolt add invalid README.md LICENSE.md
    [ "$status" -eq 1 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
}

@test "dolt reset --hard should move doc files to untracked files when there are no doc values on the head commit" {
    dolt reset --hard 
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    run ls
    [[ "$output" =~ "LICENSE.md" ]] || false
    [[ "$output" =~ "README.md" ]] || false
    dolt add .
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    dolt reset --hard
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
}

@test "dolt reset --hard should update doc files on the fs when doc values exist on the head commit" {
    echo license-text > LICENSE.md
    echo readme-text > README.md
    dolt add .
    dolt commit -m "first docs commit"
    echo updated readme > README.md
    dolt status
    dolt reset --hard
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
    run cat README.md
    [ "$output" = readme-text ]
    
    
    echo newLicenseText > LICENSE.md
    dolt table create -s `batshelper 1pk5col-ints.schema` test
    dolt add test LICENSE.md
    dolt reset --hard
    run dolt status
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
    dolt add .
    dolt reset .
    run dolt status
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    
    dolt add .
    dolt commit -m "initial doc commit"
    echo ~new-text~ > README.md
    dolt add .
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
    dolt add .
    dolt reset --soft
    run dolt status
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    
    
    dolt add .
    dolt commit -m "initial doc commit"
    echo ~new-text~ > README.md
    dolt add .
    dolt reset --soft
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
    dolt add .
    dolt reset
    run dolt status
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false

    dolt add .
    dolt commit -m "initial doc commit"
    echo ~new-text~ > README.md
    dolt add .
    dolt reset
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*README.md) ]] || false
    run cat README.md
    [[ "$output" =~ "~new-text~" ]] 
}

@test "dolt reset <doc> should remove doc from staging area" {
    dolt add LICENSE.md
    
    run dolt reset dolt_docs
    [ "$status" -eq 1 ]
    [[ "$output" =~ "'dolt_docs' is not a valid table name" ]] || false
    
    dolt reset LICENSE.md
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    
    dolt add .
    run dolt reset LICENSE.md invalid
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Invalid Table(s)" ]] || false
    [[ "$output" =~ "invalid" ]] || false
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false

    dolt reset README.md
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    dolt commit -m "initial license commit"

    echo new > LICENSE.md
    dolt add .
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    dolt reset README.md LICENSE.md
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*modified:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
}

@test "dolt reset <table> <doc> resets tables and docs from staging area" {
    dolt add .
    dolt table create -s `batshelper 1pk5col-ints.schema` test
    dolt add test
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*new table:[[:space:]]*test) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ ([[:space:]]*new doc:[[:space:]]*README.md) ]] || false
    dolt reset test LICENSE.md README.md
    run dolt status
    [[ ! "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "Untracked files:" ]] || false
    [[ "$output" =~ (new table:[[:space:]]*test) ]] || false
    [[ "$output" =~ (new doc:[[:space:]]*LICENSE.md) ]] || false
    [[ "$output" =~ (new doc:[[:space:]]*README.md) ]] || false
}

 @test "dolt checkout <doc> should save the staged docs to the filesystem if the doc has already been added" {
    echo "this is my license" > LICENSE.md
    dolt add .
    dolt checkout LICENSE.md
    run cat LICENSE.md
    [[ "$output" =~ "this is my license" ]] || false
    
    
    echo "testing-modified-doc" > LICENSE.md
    dolt checkout LICENSE.md
    run cat LICENSE.md
    [[ "$output" =~ "this is my license" ]] || false
 }

 @test "dolt checkout <doc> should save the head docs to the filesystem when the doc exists on the head, and has not been staged" {
    echo "this is my license" > LICENSE.md
    dolt add .
    dolt commit -m "committing license"
    echo "this is new" > LICENSE.md
    dolt checkout LICENSE.md
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "nothing to commit, working tree clean" ]] || false
    run cat LICENSE.md
    [[ "$output" =~  "this is my license" ]] || false
 }

 @test "dolt checkout <doc> should delete the doc from filesystem if it doesn't exist on staged or head roots" {
    run ls
    [[ "$output" =~ "README.md" ]] || false
    [[ "$output" =~ "LICENSE.md" ]] || false
    dolt checkout README.md
    run dolt status
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "README.md" ]] || false
    run ls 
    [[ ! "$output" =~ "README.md" ]] || false
    skip "This isn't working, both docs being deleted"
    [[ "$output" =~ "LICENSE.md" ]] || false
 }

  @test "dolt checkout <doc> <table> should checkout both doc and table" {
    dolt table create -s=`batshelper 1pk5col-ints.schema` test1
    dolt status
    dolt checkout LICENSE.md test1
    run dolt status
    [[ ! "$output" =~ "LICENSE.md" ]] || false
    [[ ! "$output" =~ "test1" ]] || false
    run ls 
    [[ ! "$output" =~ "LICENSE.md" ]] || false

    echo This is my readme > README.md
    dolt table create -s=`batshelper 1pk5col-ints.schema` test2
    dolt add .
    dolt table put-row test2 pk:100
    echo New text in readme > README.md
    dolt checkout test2 README.md
    run cat README.md
    [[ "$output" =~ "This is my readme" ]] || false
    run dolt table select test2
    [[ ! "$output" =~ "100" ]] || false
 }

 @test "dolt checkout <doc> <invalid_arg> should return an error and leave doc unchanged" {
     echo original license text > LICENSE.md
     dolt add .
     dolt commit -m "initial doc commit"
     echo updated license > LICENSE.md
     run dolt checkout LICENSE.md invalid
     [ "$status" -eq 1 ]
     [[ "$output" =~ "table not found" ]] || false
     run cat LICENSE.md
     [[ "$output" =~ "updated license" ]] || false
 }

@test "dolt diff shows diffs between working root and file system docs" {
    echo "testing readme" > README.md
    echo "testing license" > LICENSE.md
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/LICENSE.md b/LICENSE.md" ]] || false
    [[ "$output" =~ "diff --dolt a/README.md b/README.md" ]] || false
    [[ "$output" =~ "added doc" ]] || false
    dolt add .
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    dolt commit -m "docs"
    echo "a new readme" > README.md
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/README.md b/README.md" ]] || false
    [[ "$output" =~  "--- a/README.md" ]] || false
    [[ "$output" =~  "+++ b/README.md" ]] || false
    [[ "$output" =~  "- testing readme" ]] || false
    [[ "$output" =~  "+ a new readme" ]] || false
    [[ ! "$output" =~  "LICENSE.md" ]] || false
    rm LICENSE.md
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/LICENSE.md b/LICENSE.md" ]] || false
    [[ "$output" =~ "- testing license" ]] || false
    [[ "$output" =~ "deleted doc" ]] || false
}

@test "dolt diff <doc> shows diff of one <doc> between working root and file system docs" {
    echo "testing readme" > README.md
    echo "testing license" > LICENSE.md
    run dolt diff README.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/README.md b/README.md" ]] || false
    [[ "$output" =~ "added doc" ]] || false
    [[ ! "$output" =~ "LICENSE.md" ]] || false
    run dolt diff LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/LICENSE.md b/LICENSE.md" ]] || false
    [[ "$output" =~ "added doc" ]] || false
    [[ ! "$output" =~ "README.md" ]] || false
    dolt add .
    dolt commit -m "docs"
    echo "a new readme" > README.md
    echo "a new license" > LICENSE.md
    run dolt diff README.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/README.md b/README.md" ]] || false
    [[ "$output" =~  "--- a/README.md" ]] || false
    [[ "$output" =~  "+++ b/README.md" ]] || false
    [[ "$output" =~  "- testing readme" ]] || false
    [[ "$output" =~  "+ a new readme" ]] || false
    [[ ! "$output" =~  "LICENSE.md" ]] || false
    run dolt diff LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/LICENSE.md b/LICENSE.md" ]] || false
    [[ "$output" =~  "--- a/LICENSE.md" ]] || false
    [[ "$output" =~  "+++ b/LICENSE.md" ]] || false
    [[ "$output" =~  "- testing license" ]] || false
    [[ "$output" =~  "+ a new license" ]] || false
    [[ ! "$output" =~  "README.md" ]] || false
    rm README.md
    rm LICENSE.md
    run dolt diff LICENSE.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/LICENSE.md b/LICENSE.md" ]] || false
    [[ "$output" =~ "- testing license" ]] || false
    [[ "$output" =~ "deleted doc" ]] || false
    [[ ! "$output" =~ "README" ]] || false
    run dolt diff README.md
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/README.md b/README.md" ]] || false
    [[ "$output" =~ "- testing readme" ]] || false
    [[ "$output" =~ "deleted doc" ]] || false
    [[ ! "$output" =~ "LICENSE" ]] || false
}

@test "dolt table * does not allow operations on dolt_docs" {
    dolt add .
    dolt commit -m "First commit of docs"
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
    dolt add .
    dolt commit -m "First commit of docs"
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
    dolt add .
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "dolt_docs" ]] || false
    dolt commit -m "First commit of docs"
    run dolt ls
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "dolt_docs" ]] || false
 }


# TO DO: Expose dolt_docs for read commands
@test "dolt sql does not expose dolt_docs" {
    run dolt sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "dolt_docs" ]] || false
    dolt add .
    dolt commit -m "initial doc commits"
    
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
    [[ $output =~ "This is a repository level README" ]] || false
    [[ ! $output =~ "test-a branch" ]] || false
    echo test-b branch > README.md
    dolt add .
    dolt commit -m "Changed README.md on test-a branch"
    dolt checkout master

    # On successful FF merge, docs match the new working root
    run dolt merge test-a
    [ "$status" -eq 0 ]
    [[ $output =~ "Fast-forward" ]] || false
    run cat README.md
    [[ "$output" =~ "test-a branch" ]] || false

    # A merge with conflicts does not change the working root.
    # If the conflicts are resolved with --ours, the working root and the docs on the filesystem remain the same.
    run dolt merge test-b
    [ "$status" -eq 0 ]
    [[ $output =~ "CONFLICT" ]] || false
    run cat README.md
    [[ "$output" =~ "test-a branch" ]] || false
    run dolt conflicts cat dolt_docs
    [ "$status" -eq 0 ]
    [[ $output =~ "test-a branch" ]] || false
    [[ $output =~ "test-b branch" ]] || false
    dolt conflicts resolve dolt_docs --ours
    run cat README.md
    [[ ! $output =~ "test-b branch" ]] || false
    [[ $output =~ "test-a branch" ]] || false
    # Only allow `dolt add dolt_docs` when dolt_docs is in conflict
    dolt add dolt_docs
    dolt commit -m "Resolved docs conflict with --ours"

    # If the conflicts are resolved with --theirs, the working root and the docs on the filesystem are updated.
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
    dolt checkout master
    dolt merge test-a-again
    dolt merge test-b-again
    dolt conflicts resolve dolt_docs --theirs
    run cat README.md
    [[ ! $output =~ "test-a-again branch" ]] || false
    [[ $output =~ "test-b-again branch" ]] || false
    dolt add .
    dolt commit -m "merge test-b-again with fixed conflicts"
    
    # A merge with auto-resolved conflicts updates the working root. The docs should match the new working root.
    dolt checkout test-b-again
    echo test-b-one-more-time > README.md
    dolt add .
    dolt commit -m "test-b-one-more-time"
    dolt checkout master
    dolt merge test-b-again
    run cat README.md
    [[ "$output" =~ "one-more-time" ]] || false
    run dolt status
    echo "output = $output"
    [[ "$output" =~ "All conflicts fixed" ]] || false
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ "README.md" ]] || false
}
