#!/bin/sh
#
# Copyright (c) 2015 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test client mode dht"

. lib/test-lib.sh

check_file_fetch() {
	node=$1
	fhash=$2
	fname=$3

	test_expect_success "can fetch file" '
		ipfsi $node cat $fhash > fetch_out
	'

	test_expect_success "file looks good" '
		test_cmp $fname fetch_out
	'
}

run_single_file_test() {
	test_expect_success "add a file on node1" '
		random 1000000 > filea &&
		FILEA_HASH=$(ipfsi 1 add -q filea)
	'

	check_file_fetch 9 $FILEA_HASH filea
	check_file_fetch 8 $FILEA_HASH filea
	check_file_fetch 7 $FILEA_HASH filea
	check_file_fetch 6 $FILEA_HASH filea
	check_file_fetch 5 $FILEA_HASH filea
	check_file_fetch 4 $FILEA_HASH filea
	check_file_fetch 3 $FILEA_HASH filea
	check_file_fetch 2 $FILEA_HASH filea
	check_file_fetch 1 $FILEA_HASH filea
	check_file_fetch 0 $FILEA_HASH filea
}

NNODES=10

test_expect_success "set up testbed" '
	iptb init -n $NNODES -p 0 -f --bootstrap=none
'

test_expect_success "start up nodes" '
	iptb start [0-7] &&
	iptb start [8-9] --args="--routing=dhtclient"
'

test_expect_success "connect up nodes" '
	iptb connect [1-9] 0
'

test_expect_success "add a file on a node in client mode" '
	random 1000000 > filea &&
	FILE_HASH=$(ipfsi 8 add -q filea)
'

test_expect_success "retrieve that file on a client mode node" '
	check_file_fetch 9 $FILE_HASH filea
'

run_single_file_test

test_expect_success "shut down nodes" '
	iptb stop
'

test_done
