#!/bin/sh
#
# Copyright (c) 2017 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test keystore commands"

. lib/test-lib.sh

test_init_ipfs

test_key_cmd() {
  test_expect_success "create a new rsa key" '
    rsahash=$(ipfs key gen foobarsa --type=rsa --size=2048)
  '

  test_expect_success "create a new ed25519 key" '
    edhash=$(ipfs key gen bazed --type=ed25519)
  '

  test_expect_success "both keys show up in list output" '
    echo bazed > list_exp &&
    echo foobarsa >> list_exp &&
    echo self >> list_exp
    ipfs key list | sort > list_out &&
    test_cmp list_exp list_out
  '

  test_expect_success "key hashes show up in long list output" '
    ipfs key list -l | grep $edhash > /dev/null &&
    ipfs key list -l | grep $rsahash > /dev/null
  '

  test_expect_success "key list -l contains self key with peerID" '
    PeerID="$(ipfs config Identity.PeerID)"
    ipfs key list -l | grep "$PeerID self"
  '

  test_expect_success "key rm remove a key" '
    ipfs key rm foobarsa
    echo bazed > list_exp &&
    echo self >> list_exp
    ipfs key list | sort > list_out &&
    test_cmp list_exp list_out
  '

  test_expect_success "key rm can't remove self" '
    test_must_fail ipfs key rm self 2>&1 | tee key_rm_out &&
    grep -q "Error: cannot remove key with name" key_rm_out
  '

  test_expect_success "key rename rename a key" '
    ipfs key rename bazed fooed
    echo fooed > list_exp &&
    echo self >> list_exp
    ipfs key list | sort > list_out &&
    test_cmp list_exp list_out
  '

  test_expect_success "key rename can't rename self" '
    test_must_fail ipfs key rename self bar 2>&1 | tee key_rename_out &&
    grep -q "Error: cannot rename key with name" key_rename_out
  '

  test_expect_success "key rename can't overwrite self, even with force" '
    test_must_fail ipfs key rename -f fooed self 2>&1 | tee key_rename_out &&
    grep -q "Error: cannot overwrite key with name" key_rename_out
  '
}

test_key_cmd

test_done
