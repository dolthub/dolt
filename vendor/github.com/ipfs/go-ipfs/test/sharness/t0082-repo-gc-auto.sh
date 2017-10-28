#!/bin/sh
#
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test ipfs repo auto gc"

. lib/test-lib.sh

skip_all="skipping auto repo gc tests until they can be fixed"

test_done

check_ipfs_storage() {
  ipfs config Datastore.StorageMax
}

test_init_ipfs

test_expect_success "generate 2 600 kB files and 2 MB file using go-random" '
  random 600k 41 >600k1 &&
  random 600k 42 >600k2 &&
  random 2M 43 >2M
'

test_expect_success "set ipfs gc watermark, storage max, and gc timeout" '
  test_config_set Datastore.StorageMax "2MB" &&
  test_config_set --json Datastore.StorageGCWatermark 60 &&
  test_config_set Datastore.GCPeriod "20ms"
'

test_launch_ipfs_daemon --enable-gc

test_gc() {
  test_expect_success "adding data below watermark doesn't trigger auto gc" '
    ipfs add 600k1 >/dev/null &&
    disk_usage "$IPFS_PATH/blocks" >expected &&
    go-sleep 40ms &&
    disk_usage "$IPFS_PATH/blocks" >actual &&
    test_cmp expected actual
  '

  test_expect_success "adding data beyond watermark triggers auto gc" '
    HASH=`ipfs add -q 600k2` &&
    ipfs pin rm -r $HASH &&
    go-sleep 40ms &&
    DU=$(disk_usage "$IPFS_PATH/blocks") &&
    if test $(uname -s) = "Darwin"; then
      test "$DU" -lt 1400  # 60% of 2MB
    else
      test "$DU" -lt 1000000
    fi
  '
}

#TODO: conditional GC test is disabled due to files size bug in ipfs add
#test_expect_success "adding data beyond storageMax fails" '
#  test_must_fail ipfs add 2M 2>add_fail_out
#'
#test_expect_success "ipfs add not enough space message looks good" '
#  echo "Error: file size exceeds slack space allowed by storageMax. Maybe unpin some files?" >add_fail_exp &&
#  test_cmp add_fail_exp add_fail_out
#'

test_expect_success "periodic auto gc stress test" '
  for i in $(test_seq 1 20)
  do
    test_gc
  done
'

test_kill_ipfs_daemon

test_done
