#!/bin/sh
#
# Copyright (c) 2015 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test output of streaming json commands"

. lib/test-lib.sh

test_init_ipfs

get_api_port() {
	cat "$IPFS_PATH/api" | awk -F/ '{ print $5 }'
}

test_ls_cmd() {
	test_expect_success "make a file with multiple refs" '
		HASH=$(random 1000000 | ipfs add -q)
	'

	test_expect_success "can get refs through curl" '
		PORT=$(get_api_port) &&
		curl http://localhost:$PORT/api/v0/refs/$HASH > output
	'

	# make sure newlines are printed between each object
	test_expect_success "output looks good" '
		test_expect_code 1 grep "}{" output > /dev/null
	'
}

# should work online (only)
test_launch_ipfs_daemon
test_ls_cmd
test_kill_ipfs_daemon

test_done
