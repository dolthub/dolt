#!/bin/sh
#
# Copyright (c) 2016 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test object diff command"

. lib/test-lib.sh

test_init_ipfs

test_expect_success "create some objects for testing diffs" '
	mkdir foo &&
	echo "stuff" > foo/bar &&
	mkdir foo/baz &&
	A=$(ipfs add -r -q foo | tail -n1) &&
	echo "more things" > foo/cat &&
	B=$(ipfs add -r -q foo | tail -n1) &&
	echo "nested" > foo/baz/dog &&
	C=$(ipfs add -r -q foo | tail -n1)
	echo "changed" > foo/bar &&
	D=$(ipfs add -r -q foo | tail -n1)
'

test_expect_success "diff against self is empty" '
	ipfs object diff $A $A > diff_out
'

test_expect_success "identity diff output looks good" '
	printf "" > diff_exp &&
	test_cmp diff_exp diff_out
'

test_expect_success "diff added link works" '
	ipfs object diff $A $B > diff_out
'

test_expect_success "diff added link looks right" '
	echo + QmUSvcqzhdfYM1KLDbM76eLPdS9ANFtkJvFuPYeZt73d7A \"cat\" > diff_exp &&
	test_cmp diff_exp diff_out
'

test_expect_success "verbose diff added link works" '
	ipfs object diff -v $A $B > diff_out
'

test_expect_success "verbose diff added link looks right" '
	echo Added new link \"cat\" pointing to QmUSvcqzhdfYM1KLDbM76eLPdS9ANFtkJvFuPYeZt73d7A. > diff_exp &&
	test_cmp diff_exp diff_out
'

test_expect_success "diff removed link works" '
	ipfs object diff -v $B $A > diff_out
'

test_expect_success "diff removed link looks right" '
	echo Removed link \"cat\" \(was QmUSvcqzhdfYM1KLDbM76eLPdS9ANFtkJvFuPYeZt73d7A\). > diff_exp &&
	test_cmp diff_exp diff_out
'

test_expect_success "diff nested add works" '
	ipfs object diff -v $B $C > diff_out
'

test_expect_success "diff looks right" '
	echo Added new link \"baz/dog\" pointing to QmdNJQUTZuDpsUcec7YDuCfRfvw1w4J13DCm7YcU4VMZdS. > diff_exp &&
	test_cmp diff_exp diff_out
'

test_expect_success "diff changed link works" '
	ipfs object diff -v $C $D > diff_out
'

test_expect_success "diff looks right" '
	echo Changed \"bar\" from QmNgd5cz2jNftnAHBhcRUGdtiaMzb5Rhjqd4etondHHST8 to QmRfFVsjSXkhFxrfWnLpMae2M4GBVsry6VAuYYcji5MiZb. > diff_exp &&
	test_cmp diff_exp diff_out
'

test_done
