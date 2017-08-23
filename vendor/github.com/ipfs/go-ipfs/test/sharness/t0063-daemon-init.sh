#!/bin/sh
#
# Copyright (c) 2014 Juan Batiz-Benet
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test daemon --init command"

. lib/test-lib.sh

# We don't want the normal test_init_ipfs but we need to make sure the
# IPFS_PATH is set correctly.
export IPFS_PATH="$(pwd)/.ipfs"

# safety check since we will be removing the directory
if [ -e "$IPFS_PATH" ]; then
	echo "$IPFS_PATH exists"
	exit 1
fi

test_ipfs_daemon_init() {
	# Doing it manually since we want to launch the daemon with an
	# empty or non-existent repo; the normal
	# test_launch_ipfs_daemon does not work since it assumes the
	# repo was created a particular way with regard to the API
	# server.

	test_expect_success "'ipfs daemon --init' succeeds" '
		ipfs daemon --init >actual_daemon 2>daemon_err &
		IPFS_PID=$!
		sleep 2 &&
		if ! kill -0 $IPFS_PID; then cat daemon_err; return 1; fi
	'

	test_expect_success "'ipfs daemon' can be killed" '
		test_kill_repeat_10_sec $IPFS_PID
	'
}

test_expect_success "remove \$IPFS_PATH dir" '
	rm -rf "$IPFS_PATH"
'
test_ipfs_daemon_init

test_expect_success "create empty \$IPFS_PATH dir" '
	rm -rf "$IPFS_PATH" &&
	mkdir "$IPFS_PATH"
'

test_ipfs_daemon_init

test_done
