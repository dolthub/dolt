#!/bin/sh
#
# Copyright (c) 2014 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test init command with default config"

. lib/test-lib.sh

cfg_key="Addresses.API"
cfg_val="/ip4/0.0.0.0/tcp/5001"

# test that init succeeds
test_expect_success "ipfs init succeeds" '
	export IPFS_PATH="$(pwd)/.ipfs" &&
	echo "IPFS_PATH: \"$IPFS_PATH\"" &&
	BITS="2048" &&
	ipfs init --bits="$BITS" >actual_init ||
	test_fsh cat actual_init
'

test_expect_success ".ipfs/config has been created" '
	test -f "$IPFS_PATH"/config ||
	test_fsh ls -al .ipfs
'

test_expect_success "ipfs config succeeds" '
	ipfs config $cfg_flags "$cfg_key" "$cfg_val"
'

test_expect_success "ipfs read config succeeds" '
    IPFS_DEFAULT_CONFIG=$(cat "$IPFS_PATH"/config)
'

test_expect_success "clean up ipfs dir" '
	rm -rf "$IPFS_PATH"
'

test_expect_success "ipfs init default config succeeds" '
	echo $IPFS_DEFAULT_CONFIG | ipfs init - >actual_init ||
	test_fsh cat actual_init
'

test_expect_success "ipfs config output looks good" '
	echo "$cfg_val" >expected &&
	ipfs config "$cfg_key" >actual &&
	test_cmp expected actual
'

test_done
