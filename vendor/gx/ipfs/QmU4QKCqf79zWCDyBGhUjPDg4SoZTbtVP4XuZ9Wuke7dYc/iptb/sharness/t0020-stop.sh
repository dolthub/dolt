#!/bin/sh

test_description="iptb stop tests"

. lib/test-lib.sh

IPTB_ROOT=.

test_expect_success "iptb init works" '
	../bin/iptb init -n 3
'

test_expect_success "iptb start works" '
	../bin/iptb start --args --debug
'

test_expect_success "iptb stop works" '
	../bin/iptb stop
'

for i in {0..2}; do
	test_expect_success "daemon '$i' was shut down gracefully" '
		cat testbed/'$i'/daemon.stderr | tail -1 | grep "Gracefully shut down daemon"
	'
done

test_done
