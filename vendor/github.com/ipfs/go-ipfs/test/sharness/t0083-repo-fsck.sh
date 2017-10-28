#!/bin/sh
#
# Copyright (c) 2016 Mike Pfister 
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test ipfs repo fsck operations"

. lib/test-lib.sh

test_init_ipfs

#############################
# Test without daemon running
############################# 
# NOTE: if api file isn't present we can assume the daemon isn't running

# Try with all lock files present: repo.lock, api, and datastore/LOCK with
# repo.lock and datastore/LOCK being empty
test_expect_success "'ipfs repo fsck' succeeds with no daemon running empty
repo.lock" '
  mkdir -p $IPFS_PATH &&
  mkdir -p $IPFS_PATH/datastore &&
  touch $IPFS_PATH/datastore/LOCK &&
  touch $IPFS_PATH/repo.lock &&
  printf "/ip4/127.0.0.1/tcp/5001" > $IPFS_PATH/api &&
  ipfs repo fsck > fsck_out_actual1
'
test_expect_success "'ipfs repo fsck' output looks good with no daemon" '
  grep "Lockfiles have been removed." fsck_out_actual1
'

# Make sure the files are actually removed
test_expect_success "'ipfs repo fsck' confirm file deletion" '
  test ! -e "$IPFS_PATH/repo.lock" &&
  test ! -e "$IPFS_PATH/datastore/LOCK" &&
  test ! -e "$IPFS_PATH/api"
'

# Try with all lock files present: repo.lock, api, and datastore/LOCK with
# repo.lock is non-zero TODO: this test is broken until we find consensus on the
# non-zero repo.lock issue
test_expect_success "'ipfs repo fsck' succeeds with no daemon running non-zero
repo.lock" '
  mkdir -p $IPFS_PATH &&
  printf ":D" > $IPFS_PATH/repo.lock &&
  touch $IPFS_PATH/datastore/LOCK &&
  ipfs repo fsck > fsck_out_actual1b
'
test_expect_success "'ipfs repo fsck' output looks good with no daemon" '
  grep "Lockfiles have been removed." fsck_out_actual1b
'

# Make sure the files are actually removed
test_expect_success "'ipfs repo fsck' confirm file deletion" '
  test ! -e "$IPFS_PATH/repo.lock" &&
  test ! -e "$IPFS_PATH/datastore/LOCK" &&
  test ! -e "$IPFS_PATH/api"
'

########################
# Test for partial locks
######################## 

# Try with locks api and datastore/LOCK
test_expect_success "'ipfs repo fsck' succeeds partial lock" '
  printf  "/ip4/127.0.0.1/tcp/5001" > $IPFS_PATH/api &&
  touch $IPFS_PATH/datastore/LOCK &&
  ipfs repo fsck > fsck_out_actual2
'

test_expect_success "'ipfs repo fsck' output looks good with no daemon" '
  grep "Lockfiles have been removed." fsck_out_actual2
'

# Make sure the files are actually removed
test_expect_success "'ipfs repo fsck' confirm file deletion" '
  test ! -e "$IPFS_PATH/repo.lock" &&
  test ! -e "$IPFS_PATH/datastore/LOCK" &&
  test ! -e "$IPFS_PATH/api"
'

# Try with locks api and repo.lock
test_expect_success "'ipfs repo fsck' succeeds partial lock" '
  printf  "/ip4/127.0.0.1/tcp/5001" > $IPFS_PATH/api &&
  touch $IPFS_PATH/repo.lock &&
  ipfs repo fsck > fsck_out_actual3
'

test_expect_success "'ipfs repo fsck' output looks good with no daemon" '
  grep "Lockfiles have been removed." fsck_out_actual3
'

# Make sure the files are actually removed
test_expect_success "'ipfs repo fsck' confirm file deletion" '
  test ! -e "$IPFS_PATH/repo.lock" &&
  test ! -e "$IPFS_PATH/datastore/LOCK" &&
  test ! -e "$IPFS_PATH/api"
'

# Try with locks repo.lock and datastore
test_expect_success "'ipfs repo fsck' succeeds partial lock" '
  touch $IPFS_PATH/repo.lock &&
  touch $IPFS_PATH/datastore/LOCK &&
  ipfs repo fsck > fsck_out_actual4
'

test_expect_success "'ipfs repo fsck' output looks good with no daemon" '
  grep "Lockfiles have been removed." fsck_out_actual4
'

# Make sure the files are actually removed
test_expect_success "'ipfs repo fsck' confirm file deletion" '
  test ! -e "$IPFS_PATH/repo.lock" &&
  test ! -e "$IPFS_PATH/datastore/LOCK" &&
  test ! -e "$IPFS_PATH/api"
'

#######################
# Test for single locks
#######################

# Try with single locks repo.lock
test_expect_success "'ipfs repo fsck' succeeds partial lock" '
  touch $IPFS_PATH/repo.lock &&
  ipfs repo fsck > fsck_out_actual5
'
test_expect_success "'ipfs repo fsck' output looks good with no daemon" '
  grep "Lockfiles have been removed." fsck_out_actual5
'

# Make sure the files are actually removed
test_expect_success "'ipfs repo fsck' confirm file deletion" '
  test ! -e "$IPFS_PATH/repo.lock" &&
  test ! -e "$IPFS_PATH/datastore/LOCK" &&
  test ! -e "$IPFS_PATH/api"
'

# Try with single locks datastore/LOCK
test_expect_success "'ipfs repo fsck' succeeds partial lock" '
  touch $IPFS_PATH/datastore/LOCK &&
  ipfs repo fsck > fsck_out_actual6
'
test_expect_success "'ipfs repo fsck' output looks good with no daemon" '
  grep "Lockfiles have been removed." fsck_out_actual6
'

# Make sure the files are actually removed
test_expect_success "'ipfs repo fsck' confirm file deletion" '
  test ! -e "$IPFS_PATH/repo.lock" &&
  test ! -e "$IPFS_PATH/datastore/LOCK" &&
  test ! -e "$IPFS_PATH/api"
'

# Try with single lock api
test_expect_success "'ipfs repo fsck' succeeds partial lock" '
  printf "/ip4/127.0.0.1/tcp/5001" > $IPFS_PATH/api &&
  ipfs repo fsck > fsck_out_actual7
'

test_expect_success "'ipfs repo fsck' output looks good with no daemon" '
  grep "Lockfiles have been removed." fsck_out_actual7
'

# Make sure the files are actually removed
test_expect_success "'ipfs repo fsck' confirm file deletion" '
  test ! -e "$IPFS_PATH/repo.lock" &&
  test ! -e "$IPFS_PATH/datastore/LOCK" &&
  test ! -e "$IPFS_PATH/api"
'

##########################
# Test with daemon running
########################## 

test_launch_ipfs_daemon

# Daemon is running -> command doesn't run
test_expect_success "'ipfs repo fsck' fails with daemon running" '
  ! (ipfs repo fsck 2>fsck_out_actual8 )

'

test_expect_success "'ipfs repo fsck' output looks good with daemon" '
  grep "Error: ipfs daemon is running" fsck_out_actual8
'

test_kill_ipfs_daemon

test_done
