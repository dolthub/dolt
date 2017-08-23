#!/bin/sh
#
# Copyright (c) Jakub Sztandera
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test ipfs blockstore repo read check."

. lib/test-lib.sh

rm -rf "$IPF_PATH/*"

test_init_ipfs


H_BLOCK1=$(echo "Block 1" | ipfs add -q)
H_BLOCK2=$(echo "Block 2" | ipfs add -q)

BS_BLOCK1="XZ/CIQPDDQH5PDJTF4QSNMPFC45FQZH5MBSWCX2W254P7L7HGNHW5MQXZA.data"
BS_BLOCK2="CK/CIQNYWBOKHY7TCY7FUOBXKVJ66YRMARDT3KC7PPY6UWWPZR4YA67CKQ.data"


test_expect_success 'blocks are swapped' '
	ipfs cat $H_BLOCK2 > noswap &&
	cp -f "$IPFS_PATH/blocks/$BS_BLOCK1" "$IPFS_PATH/blocks/$BS_BLOCK2" &&
	ipfs cat $H_BLOCK2 > swap &&
	test_must_fail test_cmp noswap swap
'

ipfs config --bool Datastore.HashOnRead true

test_check_bad_blocks() {
	test_expect_success 'getting modified block fails' '
		(test_must_fail ipfs cat $H_BLOCK2 2> err_msg) &&
		grep "block in storage has different hash than requested" err_msg
	'

	test_expect_success "block shows up in repo verify" '
		test_expect_code 1 ipfs repo verify > verify_out &&
		grep "$H_BLOCK2" verify_out
	'
}

test_check_bad_blocks

test_expect_success "can add and cat a raw-leaf file" '
	HASH=$(echo "stuff" | ipfs add -q --raw-leaves) &&
	ipfs cat $HASH > /dev/null
'

test_launch_ipfs_daemon
test_check_bad_blocks
test_kill_ipfs_daemon

test_done
