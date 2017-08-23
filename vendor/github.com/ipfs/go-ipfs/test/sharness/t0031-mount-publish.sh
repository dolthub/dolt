#!/bin/sh

test_description="Test mount command in conjunction with publishing"

# imports
. lib/test-lib.sh

# if in travis CI, dont test mount (no fuse)
if ! test_have_prereq FUSE; then
	skip_all='skipping mount tests, fuse not available'

	test_done
fi

test_init_ipfs

# start iptb + wait for peering
NUM_NODES=3
test_expect_success 'init iptb' '
  iptb init -n $NUM_NODES -f --bootstrap=none --port=0 &&
  startup_cluster $NUM_NODES
'

# pre-mount publish
HASH=$(echo 'hello warld' | ipfsi 0 add -q)
test_expect_success "can publish before mounting /ipns" '
  ipfsi 0 name publish '$HASH'
'

# mount
IPFS_MOUNT_DIR="$PWD/ipfs"
IPNS_MOUNT_DIR="$PWD/ipns"
test_expect_success FUSE "'ipfs mount' succeeds" '
  ipfsi 0 mount -f "'"$IPFS_MOUNT_DIR"'" -n "'"$IPNS_MOUNT_DIR"'" >actual
'
test_expect_success FUSE "'ipfs mount' output looks good" '
  echo "IPFS mounted at: $PWD/ipfs" >expected &&
  echo "IPNS mounted at: $PWD/ipns" >>expected &&
  test_cmp expected actual
'

test_expect_success "cannot publish after mounting /ipns" '
  echo "Error: cannot manually publish while IPNS is mounted" >expected &&
  test_must_fail ipfsi 0 name publish '$HASH' 2>actual &&
  test_cmp expected actual
'

test_expect_success "unmount /ipns out-of-band" '
  fusermount -u "'"$IPNS_MOUNT_DIR"'"
'

test_expect_success "can publish after unmounting /ipns" '
  ipfsi 0 name publish '$HASH'
'

# clean-up ipfs
test_expect_success "unmount /ipfs" '
  fusermount -u "'"$IPFS_MOUNT_DIR"'"
'
iptb stop

test_done
