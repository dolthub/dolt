#!/bin/sh
#
# Copyright (c) 2014 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test ipfs repo pinning"

. lib/test-lib.sh

test_pin_flag() {
  object=$1
  ptype=$2
  expect=$3

  echo "test_pin_flag" "$@"

  if ipfs pin ls --type="$ptype" "$object" >actual
  then
    test "$expect" = "true" && return
    test_fsh cat actual
    return
  else
    test "$expect" = "false" && return
    test_fsh cat actual
    return
  fi
}

test_pin() {
  object=$1
  shift

  test_str_contains "recursive" $@
  [ "$?" = "0" ] && r="true" || r="false"

  test_str_contains "indirect" $@
  [ "$?" = "0" ] && i="true" || i="false"

  test_str_contains "direct" $@
  [ "$?" = "0" ] && d="true" || d="false"

  test_pin_flag "$object" "recursive" $r || return 1
  test_pin_flag "$object" "indirect"  $i || return 1
  test_pin_flag "$object" "direct"    $d || return 1
  return 0
}


test_init_ipfs

# test runs much faster without daemon.
# TODO: turn this back on after:
# https://github.com/ipfs/go-ipfs/issues/1075
# test_launch_ipfs_daemon

HASH_FILE6="QmRsBC3Y2G6VRPYGAVpZczx1W7Xw54MtM1NcLKTkn6rx3U"
HASH_FILE5="QmaN3PtyP8DcVGHi3Q2Fcp7CfAFVcVXKddWbHoNvaA41zf"
HASH_FILE4="QmV1aiVgpDknKQugrK59uBUbMrPnsQM1F9FXbFcfgEvUvH"
HASH_FILE3="QmZrr4Pzqp3NnMzMfbMhNe7LghfoUFHVx7c9Po9GZrhKZ7"
HASH_FILE2="QmSkjTornLY72QhmK9NvAz26815pTaoAL42rF8Qi3w2WBP"
HASH_FILE1="QmbgX4aXhSSY88GHmPQ4roizD8wFwPX8jzTLjc8VAp89x4"
HASH_DIR4="QmW98gV71Ns4bX7QbgWAqLiGF3SDC1JpveZSgBh4ExaSAd"
HASH_DIR3="QmRsCaNBMkweZ9vHT5PJRd2TT9rtNKEKyuognCEVxZxF1H"
HASH_DIR2="QmTUTQAgeVfughDSFukMZLbfGvetDJY7Ef5cDXkKK4abKC"
HASH_DIR1="QmNyZVFbgvmzguS2jVMRb8PQMNcCMJrn9E3doDhBbcPNTY"
HASH_NOPINDIR="QmWHjrRJYSfYKz5V9dWWSKu47GdY7NewyRhyTiroXgWcDU"
HASH_NOPIN_FILE1="QmUJT3GQi1dxQyTZbkaWeer9GkCn1d3W3HHRLSDr6PTcpx"
HASH_NOPIN_FILE2="QmarR7m9JT7qHEGhuFNZUEMAnoZ8E9QAfsthHCQ9Y2GfoT"

DIR1="dir1"
DIR2="dir1/dir2"
DIR4="dir1/dir2/dir4"
DIR3="dir1/dir3"
FILE1="dir1/file1"
FILE2="dir1/file2"
FILE3="dir1/file3"
FILE4="dir1/dir2/file4"
FILE6="dir1/dir2/dir4/file6"
FILE5="dir1/dir3/file5"

test_expect_success "'ipfs add dir' succeeds" '
  mkdir dir1 &&
  mkdir dir1/dir2 &&
  mkdir dir1/dir2/dir4 &&
  mkdir dir1/dir3 &&
  echo "some text 1" >dir1/file1 &&
  echo "some text 2" >dir1/file2 &&
  echo "some text 3" >dir1/file3 &&
  echo "some text 1" >dir1/dir2/file1 &&
  echo "some text 4" >dir1/dir2/file4 &&
  echo "some text 1" >dir1/dir2/dir4/file1 &&
  echo "some text 2" >dir1/dir2/dir4/file2 &&
  echo "some text 6" >dir1/dir2/dir4/file6 &&
  echo "some text 2" >dir1/dir3/file2 &&
  echo "some text 5" >dir1/dir3/file5 &&
  ipfs add -q -r dir1 >actualall &&
  tail -n1 actualall >actual1 &&
  echo "$HASH_DIR1" >expected1 &&
  ipfs repo gc && # remove the patch chaff
  test_cmp expected1 actual1
'

test_expect_success "objects are there" '
  ipfs cat "$HASH_FILE6" >FILE6_a &&
  ipfs cat "$HASH_FILE5" >FILE5_a &&
  ipfs cat "$HASH_FILE4" >FILE4_a &&
  ipfs cat "$HASH_FILE3" >FILE3_a &&
  ipfs cat "$HASH_FILE2" >FILE2_a &&
  ipfs cat "$HASH_FILE1" >FILE1_a &&
  ipfs ls "$HASH_DIR3"   >DIR3_a &&
  ipfs ls "$HASH_DIR4"   >DIR4_a &&
  ipfs ls "$HASH_DIR2"   >DIR2_a &&
  ipfs ls "$HASH_DIR1"   >DIR1_a
'

# saving this output for later
test_expect_success "ipfs object links $HASH_DIR1 works" '
  ipfs object links $HASH_DIR1 > DIR1_objlink
'


test_expect_success "added dir was pinned recursively" '
  test_pin_flag $HASH_DIR1 recursive true
'

test_expect_success "rest were pinned indirectly" '
  test_pin_flag "$HASH_FILE6" indirect true
  test_pin_flag "$HASH_FILE5" indirect true
  test_pin_flag "$HASH_FILE4" indirect true
  test_pin_flag "$HASH_FILE3" indirect true
  test_pin_flag "$HASH_FILE2" indirect true
  test_pin_flag "$HASH_FILE1" indirect true
  test_pin_flag "$HASH_DIR3" indirect true
  test_pin_flag "$HASH_DIR4" indirect true
  test_pin_flag "$HASH_DIR2" indirect true
'

test_expect_success "added dir was NOT pinned indirectly" '
  test_pin_flag "$HASH_DIR1" indirect false
'

test_expect_success "nothing is pinned directly" '
  ipfs pin ls --type=direct >actual4 &&
  test_must_be_empty actual4
'

test_expect_success "'ipfs repo gc' succeeds" '
  ipfs repo gc >gc_out_actual
'

test_expect_success "objects are still there" '
  cat FILE6_a FILE5_a FILE4_a FILE3_a FILE2_a FILE1_a >expected45 &&
  cat DIR3_a DIR4_a DIR2_a DIR1_a >>expected45 &&
  ipfs cat "$HASH_FILE6"  >actual45 &&
  ipfs cat "$HASH_FILE5" >>actual45 &&
  ipfs cat "$HASH_FILE4" >>actual45 &&
  ipfs cat "$HASH_FILE3" >>actual45 &&
  ipfs cat "$HASH_FILE2" >>actual45 &&
  ipfs cat "$HASH_FILE1" >>actual45 &&
  ipfs ls "$HASH_DIR3"   >>actual45 &&
  ipfs ls "$HASH_DIR4"   >>actual45 &&
  ipfs ls "$HASH_DIR2"   >>actual45 &&
  ipfs ls "$HASH_DIR1"   >>actual45 &&
  test_cmp expected45 actual45
'

test_expect_success "remove dir recursive pin succeeds" '
  echo "unpinned $HASH_DIR1" >expected5 &&
  ipfs pin rm -r "$HASH_DIR1" >actual5 &&
  test_cmp expected5 actual5
'

test_expect_success "none are pinned any more" '
  test_pin "$HASH_FILE6" &&
  test_pin "$HASH_FILE5" &&
  test_pin "$HASH_FILE4" &&
  test_pin "$HASH_FILE3" &&
  test_pin "$HASH_FILE2" &&
  test_pin "$HASH_FILE1" &&
  test_pin "$HASH_DIR3"  &&
  test_pin "$HASH_DIR4"  &&
  test_pin "$HASH_DIR2"  &&
  test_pin "$HASH_DIR1"
'

test_expect_success "pin some directly and indirectly" '
  ipfs pin add -r=false  "$HASH_DIR1"  >actual7 &&
  ipfs pin add -r=true  "$HASH_DIR2"  >>actual7 &&
  ipfs pin add -r=false  "$HASH_FILE1" >>actual7 &&
  echo "pinned $HASH_DIR1 directly"     >expected7 &&
  echo "pinned $HASH_DIR2 recursively" >>expected7 &&
  echo "pinned $HASH_FILE1 directly"   >>expected7 &&
  test_cmp expected7 actual7
'

test_expect_success "pin lists look good" '
  test_pin $HASH_DIR1  direct &&
  test_pin $HASH_DIR2  recursive &&
  test_pin $HASH_DIR3  &&
  test_pin $HASH_DIR4  indirect &&
  test_pin $HASH_FILE1 indirect direct &&
  test_pin $HASH_FILE2 indirect &&
  test_pin $HASH_FILE3 &&
  test_pin $HASH_FILE4 indirect &&
  test_pin $HASH_FILE5 &&
  test_pin $HASH_FILE6 indirect
'

test_expect_success "'ipfs repo gc' succeeds" '
  ipfs repo gc >gc_out_actual2 &&
  echo "removed $HASH_FILE3" > gc_out_exp2 &&
  echo "removed $HASH_FILE5" >> gc_out_exp2 &&
  echo "removed $HASH_DIR3" >> gc_out_exp2 &&
  test_includes_lines gc_out_exp2 gc_out_actual2
'

# use object links for HASH_DIR1 here because its children
# no longer exist
test_expect_success "some objects are still there" '
  cat FILE6_a FILE4_a FILE2_a FILE1_a >expected8 &&
  cat DIR4_a DIR2_a DIR1_objlink >>expected8 &&
  ipfs cat "$HASH_FILE6"  >actual8 &&
  ipfs cat "$HASH_FILE4" >>actual8 &&
  ipfs cat "$HASH_FILE2" >>actual8 &&
  ipfs cat "$HASH_FILE1" >>actual8 &&
  ipfs ls "$HASH_DIR4"   >>actual8 &&
  ipfs ls "$HASH_DIR2"   >>actual8 &&
  ipfs object links "$HASH_DIR1" >>actual8 &&
  test_cmp expected8 actual8
'

# todo: make this faster somehow.
test_expect_success "some are no longer there" '
  test_must_fail ipfs cat "$HASH_FILE5" &&
  test_must_fail ipfs cat "$HASH_FILE3" &&
  test_must_fail ipfs ls "$HASH_DIR3"
'

test_expect_success "recursive pin fails without objects" '
  ipfs pin rm -r=false "$HASH_DIR1" &&
  test_must_fail ipfs pin add -r "$HASH_DIR1" 2>err_expected8 &&
  grep "pin: merkledag: not found" err_expected8 ||
  test_fsh cat err_expected8
'

test_expect_success "test add nopin file" '
  echo "test nopin data" > test_nopin_data &&
  NOPINHASH=$(ipfs add -q --pin=false test_nopin_data) &&
  test_pin_flag "$NOPINHASH" direct false &&
  test_pin_flag "$NOPINHASH" indirect false &&
  test_pin_flag "$NOPINHASH" recursive false
'


test_expect_success "test add nopin dir" '
  mkdir nopin_dir1 &&
  echo "some nopin text 1" >nopin_dir1/file1 &&
  echo "some nopin text 2" >nopin_dir1/file2 &&
  ipfs add -q -r --pin=false nopin_dir1 | tail -n1 >actual1 &&
  echo "$HASH_NOPINDIR" >expected1 &&
  test_cmp actual1 expected1 &&
  test_pin_flag "$HASH_NOPINDIR" direct false &&
  test_pin_flag "$HASH_NOPINDIR" indirect false &&
  test_pin_flag "$HASH_NOPINDIR" recursive false &&
  test_pin_flag "$HASH_NOPIN_FILE1" direct false &&
  test_pin_flag "$HASH_NOPIN_FILE1" indirect false &&
  test_pin_flag "$HASH_NOPIN_FILE1" recursive false &&
  test_pin_flag "$HASH_NOPIN_FILE2" direct false &&
  test_pin_flag "$HASH_NOPIN_FILE2" indirect false &&
  test_pin_flag "$HASH_NOPIN_FILE2" recursive false

'

FICTIONAL_HASH="QmXV4f9v8a56MxWKBhP3ETsz4EaafudU1cKfPaaJnenc48"
test_launch_ipfs_daemon
test_expect_success "test unpinning a hash that's not pinned" "
  test_expect_code 1 ipfs pin rm $FICTIONAL_HASH --timeout=2s
  test_expect_code 1 ipfs pin rm $FICTIONAL_HASH/a --timeout=2s
  test_expect_code 1 ipfs pin rm $FICTIONAL_HASH/a/b --timeout=2s
"
test_kill_ipfs_daemon

# test_kill_ipfs_daemon

test_done
