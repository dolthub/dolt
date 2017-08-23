#!/bin/sh

test_description="Test ipfs cli cmd suggest"

. lib/test-lib.sh

test_suggest() {


    test_expect_success "test command fails" '
        test_must_fail ipfs kog 2>actual
    '

    test_expect_success "test one command is suggested" '
        grep "Did you mean this?" actual &&
        grep "log" actual ||
        test_fsh cat actual
    '

    test_expect_success "test command fails" '
        test_must_fail ipfs lis 2>actual
    '

    test_expect_success "test multiple commands are suggested" '
        grep "Did you mean any of these?" actual &&
        grep "ls" actual &&
        grep "id" actual ||
        test_fsh cat actual
    '

}

test_init_ipfs

test_suggest

test_launch_ipfs_daemon

test_suggest

test_kill_ipfs_daemon

test_done
