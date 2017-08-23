#!/bin/sh
#
# Copyright (c) 2016 Kevin Atkinson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="test how the unix files api interacts with the gc"

. lib/test-lib.sh

test_init_ipfs

test_expect_success "object not removed after gc" '
  echo "hello world" > hello.txt &&
  cat hello.txt | ipfs files write --create /hello.txt &&
  ipfs repo gc &&
  ipfs cat QmVib14uvPnCP73XaCDpwugRuwfTsVbGyWbatHAmLSdZUS
'

test_expect_success "/hello.txt still accessible after gc" '
  ipfs files read /hello.txt > hello-actual &&
  test_cmp hello.txt hello-actual
'

ADIR_HASH=QmbCgoMYVuZq8m1vK31JQx9DorwQdLMF1M3sJ7kygLLqnW
FILE1_HASH=QmX4eaSJz39mNhdu5ACUwTDpyA6y24HmrQNnAape6u3buS

test_expect_success "gc okay after adding incomplete node -- prep" '
  ipfs files mkdir /adir &&
  echo "file1" |  ipfs files write --create /adir/file1 &&
  echo "file2" |  ipfs files write --create /adir/file2 &&
  ipfs pin add --recursive=false $ADIR_HASH &&
  ipfs files rm -r /adir &&
  ipfs repo gc && # will remove /adir/file1 and /adir/file2 but not /adir
  test_must_fail ipfs cat $FILE1_HASH &&
  ipfs files cp /ipfs/$ADIR_HASH /adir &&
  ipfs pin rm $ADIR_HASH
'

test_expect_success "gc okay after adding incomplete node" '
  ipfs refs $ADIR_HASH &&
  ipfs repo gc &&
  ipfs refs $ADIR_HASH
'

test_expect_success "add directory with direct pin" '
  mkdir mydir/ &&
  echo "hello world!" > mydir/hello.txt &&
  FILE_UNPINNED=$(ipfs add --pin=false -q -r mydir/hello.txt) &&
  DIR_PINNED=$(ipfs add --pin=false -q -r mydir | tail -n1) &&
  ipfs add --pin=false -r mydir &&
  ipfs pin add --recursive=false $DIR_PINNED &&
  ipfs cat $FILE_UNPINNED
'

test_expect_success "run gc and make sure directory contents are removed" '
  ipfs repo gc &&
  test_must_fail ipfs cat $FILE_UNPINNED
'

test_expect_success "add incomplete directory and make sure gc is okay" '
  ipfs files cp /ipfs/$DIR_PINNED /mydir &&
  ipfs repo gc &&
  test_must_fail ipfs cat $FILE_UNPINNED
'

test_expect_success "add back directory contents and run gc" '
  ipfs add --pin=false mydir/hello.txt &&
  ipfs repo gc
'

test_expect_success "make sure directory contents are not removed" '
  ipfs cat $FILE_UNPINNED
'

test_done
