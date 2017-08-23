#!/bin/sh
#
# Copyright (c) 2015 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test multiple ipfs nodes"

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

check_dir_fetch() {
	node=$1
	ref=$2

	test_expect_success "node can fetch all refs for dir" '
		ipfsi $node refs -r $ref > /dev/null
	'
}

run_single_file_test() {
	test_expect_success "add a file on node1" '
		random 1000000 > filea &&
		FILEA_HASH=$(ipfsi 1 add -q filea)
	'

	check_file_fetch 4 $FILEA_HASH filea
	check_file_fetch 3 $FILEA_HASH filea
	check_file_fetch 2 $FILEA_HASH filea
	check_file_fetch 1 $FILEA_HASH filea
	check_file_fetch 0 $FILEA_HASH filea
}

run_random_dir_test() {
	test_expect_success "create a bunch of random files" '
		random-files -depth=4 -dirs=5 -files=8 foobar > /dev/null
	'

	test_expect_success "add those on node 2" '
		DIR_HASH=$(ipfsi 2 add -r -q foobar | tail -n1)
	'

	check_dir_fetch 0 $DIR_HASH
	check_dir_fetch 1 $DIR_HASH
	check_dir_fetch 2 $DIR_HASH
	check_dir_fetch 3 $DIR_HASH
	check_dir_fetch 4 $DIR_HASH
}


run_basic_test() {
	startup_cluster 5

	run_single_file_test

	test_expect_success "shut down nodes" '
		iptb stop
	'
}

run_advanced_test() {
	startup_cluster 5 "$@"

	run_single_file_test

	run_random_dir_test

	test_expect_success "shut down nodes" '
		iptb stop ||
			test_fsh tail -n +1 .iptb/*/daemon.std*
	'
}

test_expect_success "set up tcp testbed" '
	iptb init -n 5 -p 0 -f --bootstrap=none
'

# test multiplex muxer
export LIBP2P_MUX_PREFS="/mplex/6.7.0"
run_advanced_test
unset LIBP2P_MUX_PREFS

# test default configuration
run_advanced_test


test_done
