#!/usr/bin/env bats

# This file contains tests of the `dolt config` with the $HOME
# environment variable set. These can't use standard setup, because
# the DOLT_ROOT_PATH env var takes precedent over HOME, and we set
# that by default.

load helper/windows-compat

if [ -z "$BATS_TMPDIR" ]; then
    export BATS_TMPDIR=$HOME/batstmp/
    mkdir $BATS_TMPDIR
fi

setup_no_dolt_init() {
    export PATH=$PATH:~/go/bin
    cd $BATS_TMPDIR
    # Append the directory name with the pid of the calling process so
    # multiple tests can be run in parallel on the same machine
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
}

setup() {
    setup_no_dolt_init
    mkdir $BATS_TMPDIR/config-test$$
    cd $BATS_TMPDIR/dolt-repo-$$
}

teardown() {
    teardown_common
    rm -rf "$BATS_TMPDIR/config-test$$"
}

@test "config-home: different HOME vars" {
    mkdir "$BATS_TMPDIR/config-test$$/homeA"
    HOME="$BATS_TMPDIR/config-test$$/homeA"

    HOME=$HOME dolt config --global --add metrics.disabled true > /dev/null 2>&1
    HOME=$HOME run dolt config --global --add user.name "Your Name"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Config successfully updated" ]] || false
    HOME=$HOME run dolt config --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "user.name = Your Name" ]] || false
    HOME=$HOME run dolt config --get user.name
    [ "$status" -eq 0 ]
    [ "$output" = "Your Name" ]

    mkdir "$BATS_TMPDIR/config-test$$/homeB"
    HOME="$BATS_TMPDIR/config-test$$/homeB"

    HOME=$HOME dolt config --global --add metrics.disabled true > /dev/null 2>&1
    HOME=$HOME run dolt config --global --add core.editor foo
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Config successfully updated" ]] || false
    HOME=$HOME run dolt config --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "core.editor = foo" ]] || false
    [[ ! "$output" =~ "user.name = Your Name" ]] || false
    HOME=$HOME run dolt config --get core.editor
    [ "$status" -eq 0 ]
    [ "$output" = "foo" ]

    HOME="$BATS_TMPDIR/config-test$$/homeA"
    
    HOME=$HOME run dolt config --global --add user.email "you@example.com"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Config successfully updated" ]] || false
    HOME=$HOME run dolt config --list
    [ "$status" -eq 0 ]
    [[ "$output" =~ "user.email = you@example.com" ]] || false
    [[ ! "$output" =~ "core.editor = foo" ]] || false
    HOME=$HOME run dolt config --get user.email
    [ "$status" -eq 0 ]
    [ "$output" = "you@example.com" ]
}
