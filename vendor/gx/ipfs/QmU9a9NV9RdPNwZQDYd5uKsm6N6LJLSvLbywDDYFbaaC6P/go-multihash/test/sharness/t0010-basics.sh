#!/bin/sh
#
# Copyright (c) 2015 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Basic tests"

. lib/test-lib.sh

test_expect_success "current dir is writable" '
	echo "It works!" >test.txt
'

test_expect_success "multihash is available" '
	type multihash
'

test_expect_success "multihash help output looks good" '
	multihash -h 2>help.txt &&
	egrep -i "^usage:" help.txt >/dev/null &&
	egrep -i "multihash .*options.*file" help.txt >/dev/null
'

test_done
