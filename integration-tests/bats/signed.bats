#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

# Signed-commit tests need a GPG keyring, and they must not depend on (or pollute)
# the developer/CI shared ~/.gnupg. Each test gets a private, throwaway GNUPGHOME
# with the checked-in test key imported fresh. The test key is passphrase-less, so
# signing is fully non-interactive; we additionally force loopback pinentry and
# no-tty so a stale or headless agent can never block on a pinentry prompt. This is
# what makes the suite safe to run headlessly in CI.

setup() {
    setup_signed_gpg
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
    teardown_signed_gpg
}

setup_signed_gpg() {
    export GNUPGHOME="$BATS_TMPDIR/gnupg-$$"
    rm -rf "$GNUPGHOME"
    mkdir -p "$GNUPGHOME"
    chmod 700 "$GNUPGHOME"
    echo "allow-loopback-pinentry" > "$GNUPGHOME/gpg-agent.conf"
    printf "no-tty\npinentry-mode loopback\n" > "$GNUPGHOME/gpg.conf"
    gpg --batch --import "$BATS_TEST_DIRNAME/private.pgp"
    gpg --batch --list-secret-keys "573DA8C6366D04E35CDB1A44E09A0B208F666373"
}

teardown_signed_gpg() {
    gpgconf --kill all || true
    rm -rf "$GNUPGHOME"
}

# bats test_tags=no_lambda
@test "signed: dolt commit with key specified on command line" {
    run dolt sql -q "CREATE TABLE t (pk INT primary key);"
    [ "$status" -eq 0 ]

    run dolt add .
    [ "$status" -eq 0 ]

    run dolt commit -m "initial commit"
    [ "$status" -eq 0 ]

    run dolt sql -q "INSERT INTO t VALUES (1);"
    [ "$status" -eq 0 ]

    run dolt add .
    [ "$status" -eq 0 ]

    run dolt commit -S "573DA8C6366D04E35CDB1A44E09A0B208F666373" -m "signed commit"
    echo $output
    [ "$status" -eq 0 ]

    run dolt log --show-signature
    echo $output
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'gpg: Good signature from "Test User <test@dolthub.com>"' ]] || false
}

# bats test_tags=no_lambda
@test "signed: dolt commit with key specified in config" {
    skip_if_remote

    run dolt config --global --add sqlserver.global.signingkey "573DA8C6366D04E35CDB1A44E09A0B208F666373"
    [ "$status" -eq 0 ]

    run dolt sql -q "CREATE TABLE t (pk INT primary key);"
    [ "$status" -eq 0 ]

    run dolt add .
    [ "$status" -eq 0 ]

    run dolt commit -m "initial commit"
    [ "$status" -eq 0 ]

    run dolt sql -q "INSERT INTO t VALUES (1);"
    [ "$status" -eq 0 ]

    run dolt add .
    [ "$status" -eq 0 ]

    run dolt commit -S -m "signed commit"
    [ "$status" -eq 0 ]

    run dolt log --show-signature
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'gpg: Good signature from "Test User <test@dolthub.com>"' ]] || false
}

# bats test_tags=no_lambda
@test "signed: signing by default using the config" {
    skip_if_remote

    run dolt config --global --add sqlserver.global.signingkey "573DA8C6366D04E35CDB1A44E09A0B208F666373"
    [ "$status" -eq 0 ]

    run dolt config --global --add sqlserver.global.gpgsign true
    [ "$status" -eq 0 ]

    run dolt sql -q "CREATE TABLE t (pk INT primary key);"
    [ "$status" -eq 0 ]

    run dolt add .
    [ "$status" -eq 0 ]

    run dolt commit -m "initial commit"
    echo $output
    [ "$status" -eq 0 ]

    run dolt sql -q "INSERT INTO t VALUES (1);"
    [ "$status" -eq 0 ]

    run dolt add .
    [ "$status" -eq 0 ]

    run dolt commit -m "signed commit without being specified on the command line"
    [ "$status" -eq 0 ]

    run dolt log --show-signature
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'gpg: Good signature from "Test User <test@dolthub.com>"' ]] || false
}

# bats test_tags=no_lambda
@test "signed: using stored procedure" {
    skip_if_remote

    run dolt config --global --add sqlserver.global.signingkey "573DA8C6366D04E35CDB1A44E09A0B208F666373"
    [ "$status" -eq 0 ]

    run dolt config --global --add sqlserver.global.gpgsign true
    [ "$status" -eq 0 ]

    run dolt sql -q "CREATE TABLE t (pk INT primary key);"
    [ "$status" -eq 0 ]

    run dolt add .
    [ "$status" -eq 0 ]

    run dolt commit -m "initial commit"
    echo $output
    [ "$status" -eq 0 ]

    run dolt sql -q "INSERT INTO t VALUES (1);"
    [ "$status" -eq 0 ]

    run dolt add .
    [ "$status" -eq 0 ]

    run dolt sql -q "CALL dolt_commit('-m', 'signed commit');"
    [ "$status" -eq 0 ]

    run dolt log --show-signature
    [ "$status" -eq 0 ]
    [[ "$output" =~ 'gpg: Good signature from "Test User <test@dolthub.com>"' ]] || false
}
