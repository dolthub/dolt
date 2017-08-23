#!/bin/sh
#
# Copyright (c) 2016 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

# changing the bootstrap peers will require changing it in two places :)
test_description="test node bootstrapping"

. lib/test-lib.sh

test_init_ipfs

test_expect_success "disable mdns" '
	ipfs config Discovery.MDNS.Enabled false --json
'

test_launch_ipfs_daemon

test_expect_success "setup iptb nodes" '
	iptb init -n 5 -f --bootstrap=none --port=0
'

test_expect_success "start up iptb nodes" '
    iptb start
'

test_expect_success "check peers works" '
    ipfs swarm peers >peers_out
'

test_expect_success "correct number of peers" '
    test -z "`cat peers_out`"
'

betterwait() {
	while kill -0 $1; do true; done
}

test_expect_success "bring down iptb nodes" '
	PID0=$(cat "$IPTB_ROOT/0/daemon.pid") &&
	PID1=$(cat "$IPTB_ROOT/1/daemon.pid") &&
	PID2=$(cat "$IPTB_ROOT/2/daemon.pid") &&
	PID3=$(cat "$IPTB_ROOT/3/daemon.pid") &&
	PID4=$(cat "$IPTB_ROOT/4/daemon.pid") &&
	iptb stop && # TODO: add --wait flag to iptb stop
	betterwait $PID0
	betterwait $PID1
	betterwait $PID2
	betterwait $PID3
	betterwait $PID4
'

test_expect_success "reset iptb nodes" '
	# the api doesnt seem to get cleaned up in sharness tests for some reason
	iptb init -n 5 -f --bootstrap=none --port=0
'

test_expect_success "set bootstrap addrs" '
	bsn_peer_id=$(ipfs id -f "<id>") &&
	BADDR="/ip4/127.0.0.1/tcp/$SWARM_PORT/ipfs/$bsn_peer_id" &&
	ipfsi 0 bootstrap add $BADDR &&
	ipfsi 1 bootstrap add $BADDR &&
	ipfsi 2 bootstrap add $BADDR &&
	ipfsi 3 bootstrap add $BADDR &&
	ipfsi 4 bootstrap add $BADDR
'

test_expect_success "start up iptb nodes" '
	iptb start --wait
'

test_expect_success "check peers works" '
	ipfs swarm peers > peers_out
'

test_expect_success "correct number of peers" '
	test `cat peers_out | wc -l` = 5
'

test_kill_ipfs_daemon

test_expect_success "bring down iptb nodes" '
	iptb stop
'

test_done
