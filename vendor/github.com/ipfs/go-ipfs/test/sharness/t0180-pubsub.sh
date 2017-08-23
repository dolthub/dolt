#!/bin/sh

test_description="Test dht command"

. lib/test-lib.sh

# start iptb + wait for peering
NUM_NODES=5
test_expect_success 'init iptb' '
  iptb init -n $NUM_NODES --bootstrap=none --port=0
'

startup_cluster $NUM_NODES --enable-pubsub-experiment

test_expect_success 'peer ids' '
  PEERID_0=$(iptb get id 0) &&
  PEERID_2=$(iptb get id 2)
'

# ipfs pubsub sub
test_expect_success 'pubsub' '
	echo "testOK" > expected &&
	touch empty &&
	mkfifo wait ||
	test_fsh echo init fail

	# ipfs pubsub sub is long-running so we need to start it in the background and
	# wait put its output somewhere where we can access it
	(
		ipfsi 0 pubsub sub --enc=ndpayload testTopic | if read line; then
				echo $line > actual &&
				echo > wait
			fi
	) &
'

test_expect_success "wait until ipfs pubsub sub is ready to do work" '
	sleep 1
'

test_expect_success "can see peer subscribed to testTopic" '
	ipfsi 1 pubsub peers testTopic > peers_out
'

test_expect_success "output looks good" '
	echo $PEERID_0 > peers_exp &&
	test_cmp peers_exp peers_out
'

test_expect_success "publish something" '
	ipfsi 1 pubsub pub testTopic "testOK" &> pubErr
'

test_expect_success "wait until echo > wait executed" '
	cat wait &&
	test_cmp pubErr empty &&
	test_cmp expected actual
'

test_expect_success 'stop iptb' '
  iptb stop
'

test_done
