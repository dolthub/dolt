#!/bin/sh
#
# Copyright (c) 2015 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test tar commands"

. lib/test-lib.sh

test_init_ipfs

test_expect_success "create some random files" '
	mkdir foo &&
	random 10000 > foo/a &&
	random 12345 > foo/b &&
	mkdir foo/bar &&
	random 5432 > foo/bar/baz &&
	ln -s ../a foo/bar/link &&
	echo "exit" > foo/script &&
	chmod +x foo/script
'

test_expect_success "tar those random files up" '
	tar cf files.tar foo/
'

test_expect_success "'ipfs tar add' succeeds" '
	TAR_HASH=$(ipfs tar add files.tar)
'

test_expect_success "'ipfs tar cat' succeeds" '
	mkdir output &&
	ipfs tar cat $TAR_HASH > output/out.tar
'

test_expect_success "can extract tar" '
	tar xf output/out.tar -C output/
'

test_expect_success "files look right" '
	diff foo/a output/foo/a &&
	diff foo/b output/foo/b &&
	diff foo/bar/baz output/foo/bar/baz &&
	[ -L output/foo/bar/link ] &&
	[ -x foo/script ]
'

test_done
