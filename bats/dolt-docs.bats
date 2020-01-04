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
    rm LICENSE.md
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes not staged for commit:" ]] || false
    [[ "$output" =~ ([[:space:]]*deleted:[[:space:]]*LICENSE.md) ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed:" ]] || false
    [[ "$output" =~ ([[:space:]]*deleted:[[:space:]]*LICENSE.md) ]] || false
    run dolt commit -m "delete license"
    [ "$status" -eq 0 ]
    run ls
    [[ ! "$output" =~ "LICENSE.md" ]] || false

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

@test "dolt diff shows diffs between working root and file system docs" {
    echo "testing readme" > README.md
    echo "testing license" > LICENSE.md
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "diff --dolt a/LICENSE.md b/LICENSE.md" ]] || false
    [[ "$output" =~ "diff --dolt a/README.md b/README.md" ]] || false
    [[ "$output" =~ "added doc" ]] || false
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt diff
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt commit -m "docs"
    [ "$status" -eq 0 ]
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
    run dolt add .
    [ "$status" -eq 0 ]
    run dolt commit -m "docs"
    [ "$status" -eq 0 ]
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

#  @test "dolt ls should not show dolt_docs table" {

#  }

# @test "dolt table * does not allow operations on dolt_docs" {

# }

# @test "dolt sql does not allow queries or edits to dolt_docs" {

# }