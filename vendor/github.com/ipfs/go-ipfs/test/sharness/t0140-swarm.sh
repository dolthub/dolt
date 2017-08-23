#!/bin/sh
#
# Copyright (c) 2014 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test ipfs swarm command"

. lib/test-lib.sh

test_init_ipfs

test_launch_ipfs_daemon

test_expect_success 'disconnected: peers is empty' '
	ipfs swarm peers >actual &&
	test_must_be_empty actual
'

test_expect_success 'disconnected: addrs local has localhost' '
	ipfs swarm addrs local >actual &&
	grep "/ip4/127.0.0.1" actual
'

test_expect_success 'disconnected: addrs local matches ipfs id' '
	ipfs id -f="<addrs>\\n" | sort >expected &&
	ipfs swarm addrs local --id | sort >actual &&
	test_cmp expected actual
'

test_expect_success "ipfs id self works" '
	myid=$(ipfs id -f="<id>") &&
	ipfs id --timeout=1s $myid > output
'

test_expect_success "output looks good" '
	grep $myid output &&
	grep PublicKey output
'

addr="/ip4/127.0.0.1/tcp/9898/ipfs/QmUWKoHbjsqsSMesRC2Zoscs8edyFz6F77auBB1YBBhgpX"

test_expect_success "cant trigger a dial backoff with swarm connect" '
	test_expect_code 1 ipfs swarm connect $addr 2> connect_out
	test_expect_code 1 ipfs swarm connect $addr 2>> connect_out
	test_expect_code 1 ipfs swarm connect $addr 2>> connect_out
	test_expect_code 1 grep "backoff" connect_out
'

test_kill_ipfs_daemon

test_done
