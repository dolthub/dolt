#!/bin/sh
#
# Copyright (c) 2014 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test ipfs repo operations"

. lib/test-lib.sh

test_init_ipfs

# test publishing a hash

test_expect_success "'ipfs name publish' succeeds" '
	PEERID=`ipfs id --format="<id>"` &&
	test_check_peerid "${PEERID}" &&
	ipfs name publish "/ipfs/$HASH_WELCOME_DOCS" >publish_out
'

test_expect_success "publish output looks good" '
	echo "Published to ${PEERID}: /ipfs/$HASH_WELCOME_DOCS" >expected1 &&
	test_cmp expected1 publish_out
'

test_expect_success "'ipfs name resolve' succeeds" '
	ipfs name resolve "$PEERID" >output
'

test_expect_success "resolve output looks good" '
	printf "/ipfs/%s\n" "$HASH_WELCOME_DOCS" >expected2 &&
	test_cmp expected2 output
'

# now test with a path

test_expect_success "'ipfs name publish' succeeds" '
	PEERID=`ipfs id --format="<id>"` &&
	test_check_peerid "${PEERID}" &&
	ipfs name publish "/ipfs/$HASH_WELCOME_DOCS/help" >publish_out
'

test_expect_success "publish a path looks good" '
	echo "Published to ${PEERID}: /ipfs/$HASH_WELCOME_DOCS/help" >expected3 &&
	test_cmp expected3 publish_out
'

test_expect_success "'ipfs name resolve' succeeds" '
	ipfs name resolve "$PEERID" >output
'

test_expect_success "resolve output looks good" '
	printf "/ipfs/%s/help\n" "$HASH_WELCOME_DOCS" >expected4 &&
	test_cmp expected4 output
'

test_expect_success "ipfs cat on published content succeeds" '
    ipfs cat "/ipfs/$HASH_WELCOME_DOCS/help" >expected &&
    ipfs cat "/ipns/$PEERID" >actual &&
    test_cmp expected actual
'

# publish with an explicit node ID

test_expect_failure "'ipfs name publish <local-id> <hash>' succeeds" '
	PEERID=`ipfs id --format="<id>"` &&
	test_check_peerid "${PEERID}" &&
	echo ipfs name publish "${PEERID}" "/ipfs/$HASH_WELCOME_DOCS" &&
	ipfs name publish "${PEERID}" "/ipfs/$HASH_WELCOME_DOCS" >actual_node_id_publish
'

test_expect_failure "publish with our explicit node ID looks good" '
	echo "Published to ${PEERID}: /ipfs/$HASH_WELCOME_DOCS" >expected_node_id_publish &&
	test_cmp expected_node_id_publish actual_node_id_publish
'

# publish with an explicit node ID as key name

test_expect_success "generate and verify a new key" '
	NEWID=`ipfs key gen --type=rsa --size=2048 keyname` &&
	test_check_peerid "${NEWID}"
'

test_expect_success "'ipfs name publish --key=<peer-id> <hash>' succeeds" '
	ipfs name publish --key=${NEWID} "/ipfs/$HASH_WELCOME_DOCS" >actual_node_id_publish
'

test_expect_success "publish an explicit node ID as key name looks good" '
	echo "Published to ${NEWID}: /ipfs/$HASH_WELCOME_DOCS" >expected_node_id_publish &&
	test_cmp expected_node_id_publish actual_node_id_publish
'

test_done
