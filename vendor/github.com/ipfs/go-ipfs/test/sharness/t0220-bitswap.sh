#!/bin/sh
#
# Copyright (c) 2015 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="test bitswap commands"

. lib/test-lib.sh

test_init_ipfs
test_launch_ipfs_daemon

test_expect_success "'ipfs bitswap stat' succeeds" '
	ipfs bitswap stat >stat_out
'

test_expect_success "'ipfs bitswap stat' output looks good" '
	cat >expected <<EOF &&
bitswap status
	provides buffer: 0 / 256
	blocks received: 0
	blocks sent: 0
	data received: 0
	data sent: 0
	dup blocks received: 0
	dup data received: 0 B
	wantlist [0 keys]
	partners [0]
EOF
	test_cmp expected stat_out
'

test_expect_success "ipfs peer id looks good" '
	PEERID=$(ipfs config Identity.PeerID) &&
	test_check_peerid "$PEERID"
'

test_expect_success "'ipfs bitswap wantlist -p' works" '
	ipfs bitswap wantlist -p "$PEERID" >wantlist_p_out
'

test_expect_success "'ipfs bitswap wantlist -p' output looks good" '
	test_must_be_empty wantlist_p_out
'

test_expect_success "hash was removed from wantlist" '
	ipfs bitswap wantlist > wantlist_out &&
	test_must_be_empty wantlist_out
'

test_expect_success "'ipfs bitswap stat' succeeds" '
	ipfs bitswap stat >stat_out
'

test_expect_success "'ipfs bitswap stat' output looks good" '
	cat >expected <<EOF &&
bitswap status
	provides buffer: 0 / 256
	blocks received: 0
	blocks sent: 0
	data received: 0
	data sent: 0
	dup blocks received: 0
	dup data received: 0 B
	wantlist [0 keys]
	partners [0]
EOF
	test_cmp expected stat_out
'

test_expect_success "'ipfs bitswap wantlist -p' works" '
	ipfs bitswap wantlist -p "$PEERID" >wantlist_p_out
'

test_expect_success "'ipfs bitswap wantlist -p' output looks good" '
	test_cmp wantlist_out wantlist_p_out
'

test_kill_ipfs_daemon

test_done
