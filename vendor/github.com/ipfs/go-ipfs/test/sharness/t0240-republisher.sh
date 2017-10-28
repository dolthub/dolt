#!/bin/sh
#
# Copyright (c) 2014 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test ipfs repo operations"

. lib/test-lib.sh

export DEBUG=true

setup_iptb() {
  num_nodes="$1"
  bound=$(expr "$num_nodes" - 1)

  test_expect_success "iptb init" '
    iptb init -n $num_nodes --bootstrap none --port 0
  '

  for i in $(test_seq 0 "$bound")
  do
    test_expect_success "set configs up for node $i" '
      ipfsi "$i" config Ipns.RepublishPeriod 20s &&
      ipfsi "$i" config --json Ipns.ResolveCacheSize 0
    '
  done

  startup_cluster "$num_nodes"
}

teardown_iptb() {
  test_expect_success "shut down nodes" '
    iptb kill
  '
}

verify_can_resolve() {
  num_nodes="$1"
  bound=$(expr "$num_nodes" - 1)
  name="$2"
  expected="$3"
  msg="$4"

  for node in $(test_seq 0 "$bound")
  do
    test_expect_success "$msg: node $node can resolve entry" '
      ipfsi "$node" name resolve "$name" > resolve
    '

    test_expect_success "$msg: output for node $node looks right" '
      printf "/ipfs/$expected\n" > expected &&
      test_cmp expected resolve
    '
  done
}

verify_cannot_resolve() {
  num_nodes="$1"
  bound=$(expr "$num_nodes" - 1)
  name="$2"
  msg="$3"

  for node in $(test_seq 0 "$bound")
  do
    test_expect_success "$msg: resolution fails on node $node" '
      # TODO: this should work without the timeout option
      # but it currently hangs for some reason every so often
      test_expect_code 1 ipfsi "$node" name resolve --timeout=300ms "$name"
    '
  done
}

num_test_nodes=4

setup_iptb "$num_test_nodes"

test_expect_success "publish succeeds" '
  HASH=$(echo "foobar" | ipfsi 1 add -q) &&
  ipfsi 1 name publish -t 5s $HASH
'

test_expect_success "get id succeeds" '
  id=$(ipfsi 1 id -f "<id>")
'

verify_can_resolve "$num_test_nodes" "$id" "$HASH" "just after publishing"

go-sleep 5s

verify_cannot_resolve "$num_test_nodes" "$id" "after five seconds, records are invalid"

go-sleep 15s

verify_can_resolve "$num_test_nodes" "$id" "$HASH" "republisher fires after twenty seconds"

#

test_expect_success "generate new key" '
KEY2=`ipfsi 1 key gen beepboop --type ed25519`
'

test_expect_success "publish with new key succeeds" '
  HASH=$(echo "barfoo" | ipfsi 1 add -q) &&
  ipfsi 1 name publish -t 5s -k "$KEY2" $HASH
'

verify_can_resolve "$num_test_nodes" "$KEY2" "$HASH" "new key just after publishing"

go-sleep 5s

verify_cannot_resolve "$num_test_nodes" "$KEY2" "new key cannot resolve after 5 seconds"

go-sleep 15s

verify_can_resolve "$num_test_nodes" "$KEY2" "$HASH" "new key can resolve again after republish"

#

teardown_iptb

test_done
