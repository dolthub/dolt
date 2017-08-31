#!/bin/sh

test_description="iptb --version and --help tests"

. lib/test-lib.sh

test_expect_success "iptb binary is here" '
	test -f ../bin/iptb
'

test_expect_success "'iptb --version' works" '
	iptb --version >actual
'

test_expect_success "'iptb --version' output looks good" '
	egrep "^iptb version [0-9]+.[0-9]+.[0-9]+$" actual
'

test_expect_success "'iptb --help' works" '
	iptb --help >actual
'

test_expect_success "'iptb --help' output looks good" '
	grep "COMMANDS" actual &&
	grep "USAGE" actual
'

test_done
