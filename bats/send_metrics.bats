#!/usr/bin/env bats

setup() {
    load $BATS_TEST_DIRNAME/helper/common.bash
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir "dolt-repo-$$"
    cd "dolt-repo-$$"
    dolt init
    dolt config --global --unset metrics.disabled
}

teardown() {
    rm -rf "$BATS_TMPDIR/dolt-repo-$$"
    rm -rf "$BATS_TMPDIR/config-$$/.dolt/eventsData/"
}

# Create events data then flush data to stdout non-concurrently
@test "create events data then flush data to stdout concurrently" {
    cp -a $BATS_TEST_DIRNAME/helper/testEvents/ $BATS_TMPDIR/config-$$/.dolt/eventsData/
    dolt send-metrics -output >file1.txt &
    pid1=$!
    dolt send-metrics -output >file2.txt &
    pid2=$!
    wait $pid1
    exit_code1=$?
    wait $pid2
    echo exit code 1
    echo $exit_code1
    exit_code2=$?
    echo exit code 2
    echo $exit_code2
    echo file 1 contents
    cat file1.txt
    echo file 2 contents
    cat file2.txt
    if $exit_code1 = 0; then
      if $exit_code2 = 0; then
        # test file1 and file2 for some text like "0 events processed" and make sure the process writes that out
        echo inside block 1
        exit 1
      elif exit_code2 != 2; then
        # fail test
        echo inside block 2
        exit 1
      fi
    elif $exit_code2 != 0; then
      # fail test
        echo inside block 1
        exit 1
    else
      if exit_code1 != 2; then
        # fail
        echo inside block 3
        exit 1
      fi
    fi
    exit 1
}

