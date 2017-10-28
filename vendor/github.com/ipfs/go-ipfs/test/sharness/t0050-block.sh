#!/bin/sh
#
# Copyright (c) 2014 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test block command"

. lib/test-lib.sh

test_init_ipfs

HASH="QmRKqGMAM6EZngbpjSqrvYzq5Qd8b1bSWymjSUY9zQSNDk"

#
# "block put tests"
#

test_expect_success "'ipfs block put' succeeds" '
  echo "Hello Mars!" >expected_in &&
  ipfs block put <expected_in >actual_out
'

test_expect_success "'ipfs block put' output looks good" '
  echo "$HASH" >expected_out &&
  test_cmp expected_out actual_out
'

#
# "block get" tests
#

test_expect_success "'ipfs block get' succeeds" '
  ipfs block get $HASH >actual_in
'

test_expect_success "'ipfs block get' output looks good" '
  test_cmp expected_in actual_in
'

#
# "block stat" tests
#

test_expect_success "'ipfs block stat' succeeds" '
  ipfs block stat $HASH >actual_stat
'

test_expect_success "'ipfs block stat' output looks good" '
  echo "Key: $HASH" >expected_stat &&
  echo "Size: 12" >>expected_stat &&
  test_cmp expected_stat actual_stat
'

#
# "block rm" tests
#

test_expect_success "'ipfs block rm' succeeds" '
  ipfs block rm $HASH >actual_rm
'

test_expect_success "'ipfs block rm' output looks good" '
  echo "removed $HASH" > expected_rm &&
  test_cmp expected_rm actual_rm
'

test_expect_success "'ipfs block rm' block actually removed" '
  test_must_fail ipfs block stat $HASH
'

DIRHASH=QmdWmVmM6W2abTgkEfpbtA1CJyTWS2rhuUB9uP1xV8Uwtf
FILE1HASH=Qmae3RedM7SNkWGsdzYzsr6svmsFdsva4WoTvYYsWhUSVz
FILE2HASH=QmUtkGLvPf63NwVzLPKPUYgwhn8ZYPWF6vKWN3fZ2amfJF
FILE3HASH=Qmesmmf1EEG1orJb6XdK6DabxexsseJnCfw8pqWgonbkoj

test_expect_success "add and pin directory" '
  mkdir adir &&
  echo "file1" > adir/file1 &&
  echo "file2" > adir/file2 &&
  echo "file3" > adir/file3 &&
  ipfs add -r adir
  ipfs pin add -r $DIRHASH
'

test_expect_success "can't remove pinned block" '
  test_must_fail ipfs block rm $DIRHASH 2> block_rm_err
'

test_expect_success "can't remove pinned block: output looks good" '
  grep -q "$DIRHASH: pinned: recursive" block_rm_err
'

test_expect_success "can't remove indirectly pinned block" '
  test_must_fail ipfs block rm $FILE1HASH 2> block_rm_err
'

test_expect_success "can't remove indirectly pinned block: output looks good" '
  grep -q "$FILE1HASH: pinned via $DIRHASH" block_rm_err
'

test_expect_success "remove pin" '
  ipfs pin rm -r $DIRHASH
'

test_expect_success "multi-block 'ipfs block rm' succeeds" '
  ipfs block rm $FILE1HASH $FILE2HASH $FILE3HASH > actual_rm
'

test_expect_success "multi-block 'ipfs block rm' output looks good" '
  grep -F -q "removed $FILE1HASH" actual_rm &&
  grep -F -q "removed $FILE2HASH" actual_rm &&
  grep -F -q "removed $FILE3HASH" actual_rm
'

test_expect_success "'add some blocks' succeeds" '
  echo "Hello Mars!" | ipfs block put &&
  echo "Hello Venus!" | ipfs block put
'

test_expect_success "add and pin directory" '
  ipfs add -r adir
  ipfs pin add -r $DIRHASH
'

HASH=QmRKqGMAM6EZngbpjSqrvYzq5Qd8b1bSWymjSUY9zQSNDk
HASH2=QmdnpnsaEj69isdw5sNzp3h3HkaDz7xKq7BmvFFBzNr5e7
RANDOMHASH=QmRKqGMAM6EbngbZjSqrvYzq5Qd8b1bSWymjSUY9zQSNDq

test_expect_success "multi-block 'ipfs block rm' mixed" '
  test_must_fail ipfs block rm $FILE1HASH $DIRHASH $HASH $FILE3HASH $RANDOMHASH $HASH2 2> block_rm_err
'

test_expect_success "pinned block not removed" '
  ipfs block stat $FILE1HASH &&
  ipfs block stat $FILE3HASH
'

test_expect_success "non-pinned blocks removed" '
  test_must_fail ipfs block stat $HASH &&
  test_must_fail ipfs block stat $HASH2
'

test_expect_success "error reported on removing non-existent block" '
  grep -q "cannot remove $RANDOMHASH" block_rm_err
'

test_expect_success "'add some blocks' succeeds" '
  echo "Hello Mars!" | ipfs block put &&
  echo "Hello Venus!" | ipfs block put
'

test_expect_success "multi-block 'ipfs block rm -f' with non existent blocks succeed" '
  ipfs block rm -f $HASH $RANDOMHASH $HASH2
'

test_expect_success "existent blocks removed" '
  test_must_fail ipfs block stat $HASH &&
  test_must_fail ipfs block stat $HASH2
'

test_expect_success "'add some blocks' succeeds" '
  echo "Hello Mars!" | ipfs block put &&
  echo "Hello Venus!" | ipfs block put
'

test_expect_success "multi-block 'ipfs block rm -q' produces no output" '
  ipfs block rm -q $HASH $HASH2 > block_rm_out &&
  test ! -s block_rm_out
'

test_expect_success "can set cid format on block put" '
  HASH=$(ipfs block put --format=protobuf ../t0051-object-data/testPut.pb)
'

test_expect_success "created an object correctly!" '
  ipfs object get $HASH > obj_out &&
  echo "{\"Links\":[],\"Data\":\"test json for sharness test\"}" > obj_exp &&
  test_cmp obj_out obj_exp
'

test_expect_success "block get output looks right" '
  ipfs block get $HASH > pb_block_out &&
  test_cmp pb_block_out ../t0051-object-data/testPut.pb
'

test_expect_success "can set multihash type and length on block put" '
  HASH=$(echo "foooo" | ipfs block put --format=raw --mhtype=sha3 --mhlen=16)
'

test_expect_success "output looks good" '
  test "z25ScPysKoxJBcPxczn9NvuHiZU5" = "$HASH"
'

test_expect_success "can read block with different hash" '
  ipfs block get $HASH > blk_get_out &&
  echo "foooo" > blk_get_exp &&
  test_cmp blk_get_exp blk_get_out
'
#
# Misc tests
#

test_expect_success "'ipfs block stat' with nothing from stdin doesnt crash" '
  test_expect_code 1 ipfs block stat < /dev/null 2> stat_out
'

test_expect_success "no panic in output" '
  test_expect_code 1 grep "panic" stat_out
'

test_done
