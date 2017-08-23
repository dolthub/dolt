#!/bin/sh
#
# Copyright (c) 2015 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="sha1 tests"

. lib/test-lib.sh

test_expect_success "setup sha1 tests" '
	echo "Hash me!" >hash_me.txt &&
	SHA1=bc6f2c3cd945bc754789e50b2f68deee2f421810 &&
	echo "1114$SHA1" >actual
'

test_expect_success "'multihash -a=sha1 -e=hex' works" '
	multihash -a=sha1 -e=hex hash_me.txt >expected
'

test_expect_success "'multihash -a=sha1 -e=hex' output looks good" '
	test_cmp expected actual
'

test_expect_success SHASUM "check hash using shasum" '
	echo "$SHA1  hash_me.txt" >actual &&
	$SHASUMBIN hash_me.txt >expected &&
	test_cmp expected actual
'

test_done
