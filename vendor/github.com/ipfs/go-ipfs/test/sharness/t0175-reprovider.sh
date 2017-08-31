#!/bin/sh

test_description="Test reprovider"

. lib/test-lib.sh

NUM_NODES=6

init_strategy() {
  test_expect_success 'init iptb' '
    iptb init -f -n $NUM_NODES --bootstrap=none --port=0
  '

  test_expect_success 'peer ids' '
    PEERID_0=$(iptb get id 0) &&
    PEERID_1=$(iptb get id 1)
  '

  test_expect_success 'use pinning startegy for reprovider' '
    ipfsi 0 config Reprovider.Strategy '$1'
  '

  startup_cluster ${NUM_NODES}
}

findprovs_empty() {
  test_expect_success 'findprovs '$1' succeeds' '
    ipfsi 1 dht findprovs -n 1 '$1' > findprovsOut
  '

  test_expect_success "findprovs $1 output is empty" '
    test_must_be_empty findprovsOut
  '
}

findprovs_expect() {
  test_expect_success 'findprovs '$1' succeeds' '
    ipfsi 1 dht findprovs -n 1 '$1' > findprovsOut &&
    echo '$2' > expected
  '

  test_expect_success "findprovs $1 output looks good" '
    test_cmp findprovsOut expected
  '
}

reprovide() {
  test_expect_success 'reprovide' '
    # TODO: this hangs, though only after reprovision was done
    ipfsi 0 bitswap reprovide
  '
}

test_expect_success 'stop peer 1' '
  iptb stop 1
'

# Test 'all' strategy
init_strategy 'all'

test_expect_success 'add test object' '
  HASH_0=$(echo "foo" | ipfsi 0 add -q --local)
'

findprovs_empty '$HASH_0'
reprovide
findprovs_expect '$HASH_0' '$PEERID_0'

# Test 'pinned' strategy
init_strategy 'pinned'

test_expect_success 'prepare test files' '
  echo foo > f1 &&
  echo bar > f2
'

test_expect_success 'add test objects' '
  HASH_FOO=$(ipfsi 0 add -q --local --pin=false f1) &&
  HASH_BAR=$(ipfsi 0 add -q --local --pin=false f2) &&
  HASH_BAR_DIR=$(ipfsi 0 add -q --local -w f2)
'

findprovs_empty '$HASH_FOO'
findprovs_empty '$HASH_BAR'
findprovs_empty '$HASH_BAR_DIR'

reprovide

findprovs_empty '$HASH_FOO'
findprovs_expect '$HASH_BAR' '$PEERID_0'
findprovs_expect '$HASH_BAR_DIR' '$PEERID_0'

test_expect_success 'stop peer 1' '
  iptb stop 1
'

# Test 'roots' strategy
init_strategy 'roots'

test_expect_success 'prepare test files' '
  echo foo > f1 &&
  echo bar > f2 &&
  echo baz > f3
'

test_expect_success 'add test objects' '
  HASH_FOO=$(ipfsi 0 add -q --local --pin=false f1) &&
  HASH_BAR=$(ipfsi 0 add -q --local --pin=false f2) &&
  HASH_BAZ=$(ipfsi 0 add -q --local f3) &&
  HASH_BAR_DIR=$(ipfsi 0 add -q --local -w f2 | tail -1)
'

findprovs_empty '$HASH_FOO'
findprovs_empty '$HASH_BAR'
findprovs_empty '$HASH_BAR_DIR'

reprovide

findprovs_empty '$HASH_FOO'
findprovs_empty '$HASH_BAR'
findprovs_expect '$HASH_BAZ' '$PEERID_0'
findprovs_expect '$HASH_BAR_DIR' '$PEERID_0'

test_expect_success 'stop peer 1' '
  iptb stop 1
'

# Test reprovider working with ticking disabled
test_expect_success 'init iptb' '
  iptb init -f -n $NUM_NODES --bootstrap=none --port=0
'

test_expect_success 'peer ids' '
  PEERID_0=$(iptb get id 0) &&
  PEERID_1=$(iptb get id 1)
'

test_expect_success 'Disable reprovider ticking' '
  ipfsi 0 config Reprovider.Interval 0
'

startup_cluster ${NUM_NODES}

test_expect_success 'add test object' '
  HASH_0=$(echo "foo" | ipfsi 0 add -q --local)
'

findprovs_empty '$HASH_0'
reprovide
findprovs_expect '$HASH_0' '$PEERID_0'


test_done
