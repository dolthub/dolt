#!/usr/bin/env bats
#
# Tests for the global -C / --directory flag (and its --chdir synonym), which
# changes the directory dolt operates in before running a subcommand, matching
# `git -C` behaviour. These were ported from go/cmd/dolt/dolt_test.go and
# expanded to exercise real read/write functionality rather than just argument
# parsing.
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
    # setup_common leaves us inside a freshly-initialized dolt repository. This
    # is the target directory that the -C/--directory/--chdir flags will point at.
    REPO=$(pwd)

    # A scratch directory that is NOT a dolt repository. Tests cd into here to
    # prove the flags operate on the target dir and not the current dir. Created
    # as a sibling of the test repo and cleaned up in teardown.
    NONREPO="$BATS_TMPDIR/chdir-nonrepo-$$"
    rm -rf "$NONREPO"
    mkdir -p "$NONREPO"
}

teardown() {
    cd "$REPO"
    assert_feature_version
    teardown_common
    rm -rf "$BATS_TMPDIR/chdir-nonrepo-$$"
}

@test "directory-flag: -C runs the subcommand against the target repository" {
    dolt sql -q "create table t1 (pk int primary key)"

    cd "$NONREPO"
    run dolt -C "$REPO" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t1" ]] || false
}

@test "directory-flag: --directory is the long form of -C" {
    dolt sql -q "create table t2 (pk int primary key)"

    cd "$NONREPO"
    run dolt --directory "$REPO" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t2" ]] || false
}

@test "directory-flag: --chdir is a synonym for -C" {
    dolt sql -q "create table t3 (pk int primary key)"

    cd "$NONREPO"
    run dolt --chdir "$REPO" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "t3" ]] || false
}

@test "directory-flag: -C runs status from outside the repository" {
    cd "$NONREPO"
    run dolt -C "$REPO" status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "On branch" ]] || false
}

@test "directory-flag: -C writes persist in the target repository" {
    cd "$NONREPO"
    dolt -C "$REPO" sql -q "create table persisted (pk int primary key)"
    dolt -C "$REPO" sql -q "insert into persisted values (1), (2)"

    # Verify from inside the repo that the changes really landed there.
    cd "$REPO"
    run dolt sql -q "select count(*) as c from persisted"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "2" ]] || false
}

@test "directory-flag: -C can commit and read history in the target repository" {
    cd "$NONREPO"
    dolt -C "$REPO" sql -q "create table committed (pk int primary key)"
    dolt -C "$REPO" add committed
    dolt -C "$REPO" commit -m "add committed table"

    run dolt -C "$REPO" log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "add committed table" ]] || false
}

@test "directory-flag: -C does not change the process working directory" {
    cd "$NONREPO"

    # Operating on the repo via -C succeeds...
    run dolt -C "$REPO" status
    [ "$status" -eq 0 ]

    # ...but a plain command in the (non-repo) cwd still fails, proving -C did
    # not leave the process pointed at the target directory.
    run dolt status
    [ "$status" -ne 0 ]
    [[ "$output" =~ "not a valid dolt repository" ]] || false
}

@test "directory-flag: -C accepts a relative path" {
    dolt sql -q "create table rel (pk int primary key)"

    # From the parent of the repo, reference the repo by its base name.
    cd "$REPO/.."
    base=$(basename "$REPO")
    run dolt -C "$base" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "rel" ]] || false
}

@test "directory-flag: -C works alongside other global flags" {
    dolt sql -q "create table withflags (pk int primary key)"

    cd "$NONREPO"
    run dolt --user root -C "$REPO" sql -q "show tables"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "withflags" ]] || false
}

@test "directory-flag: -C with a nonexistent directory errors cleanly" {
    cd "$NONREPO"
    run dolt -C "/nonexistent/dolt-C-test-path" status
    [ "$status" -ne 0 ]
    [[ "$output" =~ "cannot change to directory" ]] || false
}

@test "directory-flag: -C into a directory that is not a repository reports no repository" {
    run dolt -C "$NONREPO" status
    [ "$status" -ne 0 ]
    [[ "$output" =~ "not a valid dolt repository" ]] || false
}
