#!/bin/sh

test_description="Test circuit relay"

. lib/test-lib.sh

# start iptb + wait for peering
NUM_NODES=3
test_expect_success 'init iptb' '
  iptb init -n $NUM_NODES --bootstrap=none --port=0
'

# Network toplogy: A <-> Relay <-> B
test_expect_success 'start up nodes for configuration' '
  iptb start --args --routing=none
'

test_expect_success 'configure EnableRelayHop in relay node' '
  ipfsi 1 config --json Swarm.EnableRelayHop true
'

test_expect_success 'restart nodes' '
  iptb stop &&
  iptb start --args --routing=none
'

test_expect_success 'connect A <-> Relay' '
  iptb connect 0 1
'

test_expect_success 'connect B <-> Relay' '
  iptb connect 2 1
'

test_expect_success 'wait until relay is ready to do work' '
  sleep 1
'

test_expect_success 'peer ids' '
  PEERID_0=$(iptb get id 0) &&
  PEERID_1=$(iptb get id 1) &&
  PEERID_2=$(iptb get id 2)
'

test_expect_success 'connect A <-Relay-> B' '
  ipfsi 0 swarm connect /p2p-circuit/ipfs/$PEERID_2 > peers_out
'

test_expect_success 'output looks good' '
  echo "connect $PEERID_2 success" > peers_exp &&
  test_cmp peers_exp peers_out
'

test_expect_success 'peers for A look good' '
  ipfsi 0 swarm peers | grep p2p-circuit > peers_out &&
  echo "/ipfs/$PEERID_1/p2p-circuit/ipfs/$PEERID_2" > peers_exp &&
  test_cmp peers_exp peers_out
'

test_expect_success 'peers for B look good' '
  ipfsi 2 swarm peers | grep p2p-circuit > peers_out &&
  echo "/ipfs/$PEERID_1/p2p-circuit/ipfs/$PEERID_0" > peers_exp &&
  test_cmp peers_exp peers_out
'

test_expect_success 'add an object in A' '
  echo "hello relay" | ipfsi 0 add > peers_out
'

test_expect_success 'object ID' '
  OBJID=$(cut -f3 -d " " peers_out)
'

test_expect_success 'cat the object in B' '
  ipfsi 2 cat $OBJID > peers_out
'

test_expect_success 'output looks good' '
  echo "hello relay" > peers_exp &&
  test_cmp peers_exp peers_out
'

test_expect_success 'stop iptb' '
  iptb stop
'

test_done
