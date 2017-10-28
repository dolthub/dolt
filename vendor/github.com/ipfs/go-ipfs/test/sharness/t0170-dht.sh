#!/bin/sh

test_description="Test dht command"

. lib/test-lib.sh

# start iptb + wait for peering
NUM_NODES=5
test_expect_success 'init iptb' '
  iptb init -n $NUM_NODES --bootstrap=none --port=0
'

startup_cluster $NUM_NODES

test_expect_success 'peer ids' '
  PEERID_0=$(iptb get id 0) &&
  PEERID_2=$(iptb get id 2)
'

# ipfs dht findpeer <peerID>
test_expect_success 'findpeer' '
  ipfsi 1 dht findpeer $PEERID_0 | sort >actual &&
  ipfsi 0 id -f "<addrs>" | cut -d / -f 1-5 | sort >expected &&
  test_cmp actual expected
'

# ipfs dht put <key> <value>
test_expect_success 'put' '
  ipfsi 1 dht put planet pluto | sort >putted &&
  [ -s putted ] ||
  test_fsh cat putted
'

test_expect_success "add a ref so we can find providers for it" '
  echo "some stuff" > afile &&
  HASH=$(ipfsi 3 add -q afile)
'

# ipfs dht findprovs <key>
test_expect_success 'findprovs' '
  ipfsi 4 dht findprovs $HASH > provs &&
  iptb get id 3 > expected &&
  test_cmp provs expected
'

# ipfs dht get <key>
test_expect_success 'get' '
  ipfsi 0 dht put bar foo >actual &&
  ipfsi 4 dht get -v bar >actual &&
  egrep "error: record key does not have selectorfunc" actual > /dev//null ||
  test_fsh cat actual
'

# ipfs dht query <peerID>
## We query 3 different keys, to statisically lower the chance that the queryer
## turns out to be the closest to what a key hashes to.
# TODO: flaky. tracked by https://github.com/ipfs/go-ipfs/issues/2620
test_expect_success 'query' '
  ipfsi 3 dht query banana >actual &&
  ipfsi 3 dht query apple >>actual &&
  ipfsi 3 dht query pear >>actual &&
  PEERS=$(wc -l actual | cut -d '"'"' '"'"' -f 1) &&
  [ -s actual ] ||
  test_might_fail test_fsh cat actual
'

test_expect_success 'stop iptb' '
  iptb stop
'

test_done
