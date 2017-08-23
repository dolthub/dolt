#!/bin/sh
#
# Copyright (c) 2015 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="test output of sysdiag command"

. lib/test-lib.sh

test_init_ipfs

test_expect_success "ipfs diag sys succeeds" '
	ipfs diag sys > output
'

test_expect_success "output contains some expected keys" '
	grep "virt" output &&
	grep "interface_addresses" output &&
	grep "arch" output &&
	grep "online" output
'

test_expect_success "uname succeeds" '
	UOUT=$(uname)
'

test_expect_success "output is similar to uname" '
	case $UOUT in
	Linux)
		grep linux output > /dev/null
		;;
	Darwin)
		grep darwin output > /dev/null
		;;
	FreeBSD)
		grep freebsd output > /dev/null
		;;
	CYGWIN*)
		grep windows output > /dev/null
		;;
	*)
		test_fsh echo system check for $UOUT failed, unsupported system?
		;;
	esac
'

test_done
