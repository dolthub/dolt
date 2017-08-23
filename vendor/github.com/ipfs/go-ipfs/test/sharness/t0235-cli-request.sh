#!/bin/sh
#
# Copyright (c) 2015 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="test http requests made by cli"

. lib/test-lib.sh

test_init_ipfs

test_expect_success "can make http request against nc server" '
	nc -ld 5005 > nc_out &
	NCPID=$!
	go-sleep 0.5s && kill "$NCPID" &
	ipfs cat /ipfs/Qmabcdef --api /ip4/127.0.0.1/tcp/5005 || true
'

test_expect_success "output does not contain multipart info" '
	test_expect_code 1 grep multipart nc_out
'

test_expect_success "request looks good" '
	grep "POST /api/v0/cat" nc_out
'

test_expect_success "api flag does not appear in request" '
	test_expect_code 1 grep "api=/ip4" nc_out
'

test_done
