#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "creds: ls empty creds" {
    run dolt creds ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

@test "creds: ls new cred" {
    dolt creds new
    run dolt creds ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    [[ "${lines[0]}" =~ (^\*\ ) ]] || false
    cred=`echo "${lines[0]}" | awk '{print $2}'`
    dolt creds new
    run dolt creds ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    # Initially chosen credentials is still the chosen one.
    [[ "`echo "$output"|grep "$cred"`" =~ (^\*\ ) ]] || false
}

@test "creds: ls -v new creds" {
    dolt creds new
    dolt creds new
    run dolt creds ls -v
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 4 ]
    [[ "${lines[0]}" =~ (public\ key) ]] || false
    [[ "${lines[0]}" =~ (key\ id) ]] || false
}

@test "creds: rm removes a cred" {
    dolt creds new
    run dolt creds ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 1 ]
    cred=`echo "${lines[0]}" | awk '{print $2}'`
    dolt creds rm $cred
    run dolt creds ls
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 0 ]
}

@test "creds: use can use a new credential by pub key" {
    dolt creds new
    dolt creds new
    run dolt creds ls -v
    [ "$status" -eq 0 ]
    unusedpk=`echo "$output"|sed -n '3,$p'|grep '^  '|awk '{print $1}'`
    dolt creds use "$unusedpk"
    run dolt creds ls
    [ "$status" -eq 0 ]
    [[ "`echo "$output"|grep "$unusedpk"`" =~ (^\*\ ) ]] || false
}

@test "creds: use can use a new credential by key id" {
    dolt creds new
    dolt creds new
    run dolt creds ls -v
    [ "$status" -eq 0 ]
    unusedpk=`echo "$output"|sed -n '3,$p'|grep '^  '|awk '{print $1}'`
    unusedkid=`echo "$output"|sed -n '3,$p'|grep '^  '|awk '{print $2}'`
    dolt creds use "$unusedkid"
    run dolt creds ls
    [ "$status" -eq 0 ]
    [[ "`echo "$output"|grep "$unusedpk"`" =~ (^\*\ ) ]] || false
}

@test "creds: use fails with bad arguments" {
    run dolt creds use qv7bnud1t4fo9qo6nq8l44cbrjlh33hn6h22a2c4thr0m454lp4g
    [ "$status" -eq 1 ]
    run dolt creds use ir3vamrck6e6e8gl4s51t94k0i7eo92ccr0st3mc6keau
    [ "$status" -eq 1 ]
    run dolt creds use invalid-format-for-parameter
    [ "$status" -eq 1 ]
    run dolt creds use
    [ "$status" -eq 1 ]
}

@test "creds: creds file created with right permissions" {
    skiponwindows "Need to investigate the permissions results on windows."
    dolt creds new
    file=$(echo ${DOLT_ROOT_PATH}/.dolt/creds/`dolt creds ls -v | grep '*' | awk '{print $3}'`.jwk)
    perms=$(ls -l "$file" | awk '{print $1}')
    [ "$perms" == "-rw-------" ]
}

@test "creds: can import cred from good jwk file" {
    dolt creds import `batshelper known-good.jwk`
}

@test "creds: can import cred from good jwk stdin" {
    dolt creds import <"$BATS_TEST_DIRNAME/helper/known-good.jwk"
}

@test "creds: import cred of corrupted jwk from file fails" {
    run dolt creds import `batshelper known-truncated.jwk`
    [ "$status" -eq 1 ]
    run dolt creds import `batshelper known-decapitated.jwk`
    [ "$status" -eq 1 ]
    run dolt creds import does-not-exist
    [ "$status" -eq 1 ]
}

@test "creds: import cred of corrupted jwk from stdin fails" {
    run dolt creds import <"$BATS_TEST_DIRNAME/helper/known-truncated.jwk"
    [ "$status" -eq 1 ]
    run dolt creds import <"$BATS_TEST_DIRNAME/helper/known-decapitated.jwk"
    [ "$status" -eq 1 ]
    run dolt creds import </dev/null
    [ "$status" -eq 1 ]
}

@test "creds: import cred with already used cred does not replace used cred" {
    pubkey=`dolt creds new | grep 'pub key:' | awk '{print $3}'`
    dolt creds import `batshelper known-good.jwk`
    dolt creds ls -v | grep '*' | grep "$pubkey"
}
