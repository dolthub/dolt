#!/bin/sh
#
# Copyright (c) 2014 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test init command"

. lib/test-lib.sh

# test that ipfs fails to init if IPFS_PATH isnt writeable
test_expect_success "create dir and change perms succeeds" '
	export IPFS_PATH="$(pwd)/.badipfs" &&
	mkdir "$IPFS_PATH" &&
	chmod 000 "$IPFS_PATH"
'

test_expect_success "ipfs init fails" '
	test_must_fail ipfs init 2> init_fail_out
'

# Under Windows/Cygwin the error message is different,
# so we use the STD_ERR_MSG prereq.
if test_have_prereq STD_ERR_MSG; then
	init_err_msg="Error: failed to take lock at $IPFS_PATH: permission denied"
else
	init_err_msg="Error: mkdir $IPFS_PATH: The system cannot find the path specified."
fi

test_expect_success "ipfs init output looks good" '
	echo "$init_err_msg" >init_fail_exp &&
	test_cmp init_fail_exp init_fail_out
'

test_expect_success "cleanup dir with bad perms" '
	chmod 775 "$IPFS_PATH" &&
	rmdir "$IPFS_PATH"
'

# test no repo error message
# this applies to `ipfs add sth`, `ipfs refs <hash>`
test_expect_success "ipfs cat fails" '
    export IPFS_PATH="$(pwd)/.ipfs" &&
    test_must_fail ipfs cat Qmaa4Rw81a3a1VEx4LxB7HADUAXvZFhCoRdBzsMZyZmqHD 2> cat_fail_out
'

test_expect_success "ipfs cat no repo message looks good" '
    echo "Error: no IPFS repo found in $IPFS_PATH." > cat_fail_exp &&
    echo "please run: 'ipfs init'" >> cat_fail_exp &&
    test_path_cmp cat_fail_exp cat_fail_out
'

# test that init succeeds
test_expect_success "ipfs init succeeds" '
	export IPFS_PATH="$(pwd)/.ipfs" &&
	echo "IPFS_PATH: \"$IPFS_PATH\"" &&
	BITS="2048" &&
	ipfs init --bits="$BITS" >actual_init ||
	test_fsh cat actual_init
'

test_expect_success ".ipfs/ has been created" '
	test -d ".ipfs" &&
	test -f ".ipfs/config" &&
	test -d ".ipfs/datastore" &&
	test -d ".ipfs/blocks" &&
	test ! -f ._check_writeable ||
	test_fsh ls -al .ipfs
'

test_expect_success "ipfs config succeeds" '
	echo /ipfs >expected_config &&
	ipfs config Mounts.IPFS >actual_config &&
	test_cmp expected_config actual_config
'

test_expect_success "ipfs peer id looks good" '
	PEERID=$(ipfs config Identity.PeerID) &&
	test_check_peerid "$PEERID"
'

test_expect_success "ipfs init output looks good" '
	STARTFILE="ipfs cat /ipfs/$HASH_WELCOME_DOCS/readme" &&
	echo "initializing IPFS node at $IPFS_PATH" >expected &&
	echo "generating $BITS-bit RSA keypair...done" >>expected &&
	echo "peer identity: $PEERID" >>expected &&
	echo "to get started, enter:" >>expected &&
	printf "\\n\\t$STARTFILE\\n\\n" >>expected &&
	test_cmp expected actual_init
'

test_expect_success "Welcome readme exists" '
	ipfs cat /ipfs/$HASH_WELCOME_DOCS/readme
'

test_expect_success "clean up ipfs dir" '
	rm -rf "$IPFS_PATH"
'

test_expect_success "'ipfs init --empty-repo' succeeds" '
	BITS="1024" &&
	ipfs init --bits="$BITS" --empty-repo >actual_init
'

test_expect_success "ipfs peer id looks good" '
	PEERID=$(ipfs config Identity.PeerID) &&
	test_check_peerid "$PEERID"
'

test_expect_success "'ipfs init --empty-repo' output looks good" '
	echo "initializing IPFS node at $IPFS_PATH" >expected &&
	echo "generating $BITS-bit RSA keypair...done" >>expected &&
	echo "peer identity: $PEERID" >>expected &&
	test_cmp expected actual_init
'

test_expect_success "Welcome readme doesn't exists" '
	test_must_fail ipfs cat /ipfs/$HASH_WELCOME_DOCS/readme
'

test_expect_success "ipfs id agent string contains correct version" '
	ipfs id -f "<aver>" | grep $(ipfs version -n)
'

test_expect_success "clean up ipfs dir" '
	rm -rf "$IPFS_PATH"
'

# test init profiles
test_expect_success "'ipfs init --profile' with invalid profile fails" '
	BITS="1024" &&
	test_must_fail ipfs init --bits="$BITS" --profile=nonexistent_profile 2> invalid_profile_out
	EXPECT="Error: invalid configuration profile: nonexistent_profile" &&
	grep "$EXPECT" invalid_profile_out
'

test_expect_success "'ipfs init --profile' succeeds" '
	BITS="1024" &&
	ipfs init --bits="$BITS" --profile=server
'

test_expect_success "'ipfs config Swarm.AddrFilters' looks good" '
	ipfs config Swarm.AddrFilters > actual_config &&
	test $(cat actual_config | wc -l) = 17
'

test_expect_success "clean up ipfs dir" '
	rm -rf "$IPFS_PATH"
'

test_expect_success "'ipfs init --profile=test' succeeds" '
	BITS="1024" &&
	ipfs init --bits="$BITS" --profile=test
'

test_expect_success "'ipfs config Bootstrap' looks good" '
	ipfs config Bootstrap > actual_config &&
	test $(cat actual_config) = "[]"
'

test_expect_success "'ipfs config Addresses.API' looks good" '
	ipfs config Addresses.API > actual_config &&
	test $(cat actual_config) = "/ip4/127.0.0.1/tcp/0"
'

test_expect_success "clean up ipfs dir" '
	rm -rf "$IPFS_PATH"
'

test_init_ipfs

test_launch_ipfs_daemon

test_expect_success "ipfs init should not run while daemon is running" '
	test_must_fail ipfs init 2> daemon_running_err &&
	EXPECT="Error: ipfs daemon is running. please stop it to run this command" &&
	grep "$EXPECT" daemon_running_err
'

test_kill_ipfs_daemon

test_done
