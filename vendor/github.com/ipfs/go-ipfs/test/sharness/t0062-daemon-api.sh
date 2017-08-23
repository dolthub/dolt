#!/bin/sh
#
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test daemon command"

. lib/test-lib.sh

test_init_ipfs

differentport=$((API_PORT + 1))
api_other="/ip4/127.0.0.1/tcp/$differentport"
api_unreachable="/ip4/127.0.0.1/tcp/1"

test_expect_success "config setup" '
	peerid=$(ipfs config Identity.PeerID) &&
	test_check_peerid "$peerid"
'

test_client() {
	opts="$@"
	echo "OPTS = " $opts
	test_expect_success "client must work properly $state" '
		printf "$peerid" >expected &&
		ipfs id -f="<id>" $opts >actual &&
		test_cmp expected actual
	'
}

test_client_must_fail() {
	opts="$@"
	echo "OPTS = " $opts
	test_expect_success "client should fail $state" '
		echo "Error: api not running" >expected_err &&
		test_must_fail ipfs id -f="<id>" $opts >actual 2>actual_err &&
		test_cmp expected_err actual_err
	'
}

test_client_suite() {
	state="$1"
	cfg_success="$2"
	diff_success="$3"
	api_fromcfg="$4"
	api_different="$5"

	# must always work
	test_client

	# must always err
	test_client_must_fail --api "$api_unreachable"

	if [ "$cfg_success" = true ]; then
		test_client --api "$api_fromcfg"
	else
		test_client_must_fail --api "$api_fromcfg"
	fi

	if [ "$diff_success" = true ]; then
		test_client --api "$api_different"
	else
		test_client_must_fail --api "$api_different"
	fi
}

# first, test things without daemon, without /api file
# with no daemon, everything should fail 
# (using unreachable because API_MADDR doesnt get set until daemon start)
test_client_suite "(daemon off, no --api, no /api file)" false false "$api_unreachable" "$api_other"


# then, test things with daemon, with /api file

test_launch_ipfs_daemon

test_expect_success "'ipfs daemon' creates api file" '
	test -f ".ipfs/api"
'

test_client_suite "(daemon on, no --api, /api file from cfg)" true false "$API_MADDR" "$api_other"

# then, test things without daemon, with /api file

test_kill_ipfs_daemon

# again, both should fail
test_client_suite "(daemon off, no --api, /api file from cfg)" false false "$API_MADDR" "$api_other"

test_done
