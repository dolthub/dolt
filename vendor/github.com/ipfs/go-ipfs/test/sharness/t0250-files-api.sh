#!/bin/sh
#
# Copyright (c) 2015 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="test the unix files api"

. lib/test-lib.sh

test_init_ipfs

create_files() {
  FILE1=$(echo foo | ipfs add "$@" -q) &&
  FILE2=$(echo bar | ipfs add "$@" -q) &&
  FILE3=$(echo baz | ipfs add "$@" -q) &&
  mkdir -p stuff_test &&
  echo cats > stuff_test/a &&
  echo dogs > stuff_test/b &&
  echo giraffes > stuff_test/c &&
  DIR1=$(ipfs add -r "$@" -q stuff_test | tail -n1)
}

verify_path_exists() {
  # simply running ls on a file should be a good 'check'
  ipfs files ls $1
}

verify_dir_contents() {
  dir=$1
  shift
  rm -f expected
  touch expected
  for e in $@
  do
    echo $e >> expected
  done

  test_expect_success "can list dir" '
    ipfs files ls $dir > output
  '

  test_expect_success "dir entries look good" '
    test_sort_cmp output expected
  '
}

test_sharding() {
  local EXTRA ARGS
  EXTRA=$1
  ARGS=$2 # only applied to the initial directory

  test_expect_success "make a directory $EXTRA" '
    ipfs files mkdir $ARGS /foo
  '

  test_expect_success "can make 100 files in a directory $EXTRA" '
    printf "" > list_exp_raw
    for i in `seq 100`
    do
      echo $i | ipfs files write --create /foo/file$i
      echo file$i >> list_exp_raw
    done
  '

  test_expect_success "listing works $EXTRA" '
    ipfs files ls /foo |sort > list_out &&
    sort list_exp_raw > list_exp &&
    test_cmp list_exp list_out
  '

  test_expect_success "can read a file from sharded directory $EXTRA" '
    ipfs files read /foo/file65 > file_out &&
    echo "65" > file_exp &&
    test_cmp file_out file_exp
  '

  test_expect_success "can pin a file from sharded directory $EXTRA" '
    ipfs files stat --hash /foo/file42 > pin_file_hash &&
    ipfs pin add < pin_file_hash > pin_hash
  '

  test_expect_success "can unpin a file from sharded directory $EXTRA" '
    read -r _ HASH _ < pin_hash &&
    ipfs pin rm $HASH
  '

  test_expect_success "output object was really sharded and has correct hash $EXTRA" '
    ipfs files stat --hash /foo > expected_foo_hash &&
    echo $SHARD_HASH > actual_foo_hash &&
    test_cmp expected_foo_hash actual_foo_hash
  '

  test_expect_success "clean up $EXTRA" '
    ipfs files rm -r /foo
  '
}

test_files_api() {
  local EXTRA ARGS RAW_LEAVES
  EXTRA=$1
  ARGS=$2
  RAW_LEAVES=$3

  test_expect_success "can mkdir in root $EXTRA" '
    ipfs files mkdir $ARGS /cats
  '

  test_expect_success "'files ls' lists root by default $EXTRA" '
    ipfs files ls >actual &&
    echo "cats" >expected &&
    test_cmp expected actual
  '

  test_expect_success "directory was created $EXTRA" '
    verify_path_exists /cats
  '

  test_expect_success "directory is empty $EXTRA" '
    verify_dir_contents /cats
  '
  # we do verification of stat formatting now as we depend on it

  test_expect_success "stat works $EXTRA" '
    ipfs files stat / >stat
  '

  test_expect_success "hash is first line of stat $EXTRA" '
    ipfs ls $(head -1 stat) | grep "cats"
  '

  test_expect_success "stat --hash gives only hash $EXTRA" '
    ipfs files stat --hash / >actual &&
    head -n1 stat >expected &&
    test_cmp expected actual
  '

  test_expect_success "stat with multiple format options should fail $EXTRA" '
    test_must_fail ipfs files stat --hash --size /
  '

  test_expect_success "compare hash option with format $EXTRA" '
    ipfs files stat --hash / >expected &&
    ipfs files stat --format='"'"'<hash>'"'"' / >actual &&
    test_cmp expected actual
  '
  test_expect_success "compare size option with format $EXTRA" '
    ipfs files stat --size / >expected &&
    ipfs files stat --format='"'"'<cumulsize>'"'"' / >actual &&
    test_cmp expected actual
  '

  test_expect_success "check root hash $EXTRA" '
    ipfs files stat --hash / > roothash
  '

  test_expect_success "cannot mkdir / $EXTRA" '
    test_expect_code 1 ipfs files mkdir $ARGS /
  '

  test_expect_success "check root hash was not changed $EXTRA" '
    ipfs files stat --hash / > roothashafter &&
    test_cmp roothash roothashafter
  '

  test_expect_success "can put files into directory $EXTRA" '
    ipfs files cp /ipfs/$FILE1 /cats/file1
  '

  test_expect_success "file shows up in directory $EXTRA" '
    verify_dir_contents /cats file1
  '

  test_expect_success "file has correct hash and size in directory $EXTRA" '
    echo "file1	$FILE1	4" > ls_l_expected &&
    ipfs files ls -l /cats > ls_l_actual &&
    test_cmp ls_l_expected ls_l_actual
  '

  test_expect_success "can stat file $EXTRA" '
    ipfs files stat /cats/file1 > file1stat_orig
  '

  test_expect_success "stat output looks good" '
    grep -v CumulativeSize: file1stat_orig > file1stat_actual &&
    echo "$FILE1" > file1stat_expect &&
    echo "Size: 4" >> file1stat_expect &&
    echo "ChildBlocks: 0" >> file1stat_expect &&
    echo "Type: file" >> file1stat_expect &&
    test_cmp file1stat_expect file1stat_actual
  '

  test_expect_success "can read file $EXTRA" '
    ipfs files read /cats/file1 > file1out
  '

  test_expect_success "output looks good $EXTRA" '
    echo foo > expected &&
    test_cmp expected file1out
  '

  test_expect_success "can put another file into root $EXTRA" '
    ipfs files cp /ipfs/$FILE2 /file2
  '

  test_expect_success "file shows up in root $EXTRA" '
    verify_dir_contents / file2 cats
  '

  test_expect_success "can read file $EXTRA" '
    ipfs files read /file2 > file2out
  '

  test_expect_success "output looks good $EXTRA" '
    echo bar > expected &&
    test_cmp expected file2out
  '

  test_expect_success "can make deep directory $EXTRA" '
    ipfs files mkdir $ARGS -p /cats/this/is/a/dir
  '

  test_expect_success "directory was created correctly $EXTRA" '
    verify_path_exists /cats/this/is/a/dir &&
    verify_dir_contents /cats this file1 &&
    verify_dir_contents /cats/this is &&
    verify_dir_contents /cats/this/is a &&
    verify_dir_contents /cats/this/is/a dir &&
    verify_dir_contents /cats/this/is/a/dir
  '

  test_expect_success "can copy file into new dir $EXTRA" '
    ipfs files cp /ipfs/$FILE3 /cats/this/is/a/dir/file3
  '

  test_expect_success "can read file $EXTRA" '
    ipfs files read /cats/this/is/a/dir/file3 > output
  '

  test_expect_success "output looks good $EXTRA" '
    echo baz > expected &&
    test_cmp expected output
  '

  test_expect_success "file shows up in dir $EXTRA" '
    verify_dir_contents /cats/this/is/a/dir file3
  '

  test_expect_success "can remove file $EXTRA" '
    ipfs files rm /cats/this/is/a/dir/file3
  '

  test_expect_success "file no longer appears $EXTRA" '
    verify_dir_contents /cats/this/is/a/dir
  '

  test_expect_success "can remove dir $EXTRA" '
    ipfs files rm -r /cats/this/is/a/dir
  '

  test_expect_success "dir no longer appears $EXTRA" '
    verify_dir_contents /cats/this/is/a
  '

  test_expect_success "can remove file from root $EXTRA" '
    ipfs files rm /file2
  '

  test_expect_success "file no longer appears $EXTRA" '
    verify_dir_contents / cats
  '

  test_expect_success "check root hash $EXTRA" '
    ipfs files stat --hash / > roothash
  '

  test_expect_success "cannot remove root $EXTRA" '
    test_expect_code 1 ipfs files rm -r /
  '

  test_expect_success "check root hash was not changed $EXTRA" '
    ipfs files stat --hash / > roothashafter &&
    test_cmp roothash roothashafter
  '

  # test read options

  test_expect_success "read from offset works $EXTRA" '
    ipfs files read -o 1 /cats/file1 > output
  '

  test_expect_success "output looks good $EXTRA" '
    echo oo > expected &&
    test_cmp expected output
  '

  test_expect_success "read with size works $EXTRA" '
    ipfs files read -n 2 /cats/file1 > output
  '

  test_expect_success "output looks good $EXTRA" '
    printf fo > expected &&
    test_cmp expected output
  '

  test_expect_success "cannot read from negative offset $EXTRA" '
    test_expect_code 1 ipfs files read --offset -3 /cats/file1
  '

  test_expect_success "read from offset 0 works $EXTRA" '
    ipfs files read --offset 0 /cats/file1 > output
  '

  test_expect_success "output looks good $EXTRA" '
    echo foo > expected &&
    test_cmp expected output
  '

  test_expect_success "read last byte works $EXTRA" '
    ipfs files read --offset 2 /cats/file1 > output
  '

  test_expect_success "output looks good $EXTRA" '
    echo o > expected &&
    test_cmp expected output
  '

  test_expect_success "offset past end of file fails $EXTRA" '
    test_expect_code 1 ipfs files read --offset 5 /cats/file1
  '

  test_expect_success "cannot read negative count bytes $EXTRA" '
    test_expect_code 1 ipfs read --count -1 /cats/file1
  '

  test_expect_success "reading zero bytes prints nothing $EXTRA" '
    ipfs files read --count 0 /cats/file1 > output
  '

  test_expect_success "output looks good $EXTRA" '
    printf "" > expected &&
    test_cmp expected output
  '

  test_expect_success "count > len(file) prints entire file $EXTRA" '
    ipfs files read --count 200 /cats/file1 > output
  '

  test_expect_success "output looks good $EXTRA" '
    echo foo > expected &&
    test_cmp expected output
  '

  # test write

  test_expect_success "can write file $EXTRA" '
    echo "ipfs rocks" > tmpfile &&
    cat tmpfile | ipfs files write $ARGS $RAW_LEAVES --create /cats/ipfs
  '

  test_expect_success "file was created $EXTRA" '
    verify_dir_contents /cats ipfs file1 this
  '

  test_expect_success "can read file we just wrote $EXTRA" '
    ipfs files read /cats/ipfs > output
  '

  test_expect_success "can write to offset $EXTRA" '
    echo "is super cool" | ipfs files write $ARGS $RAW_LEAVES -o 5 /cats/ipfs
  '

  test_expect_success "file looks correct $EXTRA" '
    echo "ipfs is super cool" > expected &&
    ipfs files read /cats/ipfs > output &&
    test_cmp expected output
  '

  test_expect_success "file hash correct $EXTRA" '
    echo $FILE_HASH > filehash_expected &&
    ipfs files stat --hash /cats/ipfs > filehash &&
    test_cmp filehash_expected filehash 
  '

  test_expect_success "cant write to negative offset $EXTRA" '
    test_expect_code 1 ipfs files write $ARGS $RAW_LEAVES --offset -1 /cats/ipfs < output
  '

  test_expect_success "verify file was not changed $EXTRA" '
    ipfs files stat --hash /cats/ipfs > afterhash &&
    test_cmp filehash afterhash
  '

  test_expect_success "write new file for testing $EXTRA" '
    echo foobar | ipfs files write $ARGS $RAW_LEAVES --create /fun
  '

  test_expect_success "write to offset past end works $EXTRA" '
    echo blah | ipfs files write $ARGS $RAW_LEAVES --offset 50 /fun
  '

  test_expect_success "can read file $EXTRA" '
    ipfs files read /fun > sparse_output
  '

  test_expect_success "output looks good $EXTRA" '
    echo foobar > sparse_expected &&
    echo blah | dd of=sparse_expected bs=50 seek=1 &&
    test_cmp sparse_expected sparse_output
  '

  test_expect_success "cleanup $EXTRA" '
    ipfs files rm /fun
  '

  test_expect_success "cannot write to directory $EXTRA" '
    ipfs files stat --hash /cats > dirhash &&
    test_expect_code 1 ipfs files write $ARGS $RAW_LEAVES /cats < output
  '

  test_expect_success "verify dir was not changed $EXTRA" '
    ipfs files stat --hash /cats > afterdirhash &&
    test_cmp dirhash afterdirhash
  '

  test_expect_success "cannot write to nonexistant path $EXTRA" '
    test_expect_code 1 ipfs files write $ARGS $RAW_LEAVES /cats/bar/ < output
  '

  test_expect_success "no new paths were created $EXTRA" '
    verify_dir_contents /cats file1 ipfs this
  '

  test_expect_success "write 'no-flush' succeeds $EXTRA" '
    echo "testing" | ipfs files write $ARGS $RAW_LEAVES -f=false -e /cats/walrus
  '

  test_expect_success "root hash not bubbled up yet $EXTRA" '
    test -z "$ONLINE" ||
    (ipfs refs local > refsout &&
    test_expect_code 1 grep $ROOT_HASH refsout)
  '

  test_expect_success "changes bubbled up to root on inspection $EXTRA" '
    ipfs files stat --hash / > root_hash
  '

  test_expect_success "root hash looks good $EXTRA" '
    export EXP_ROOT_HASH="$ROOT_HASH" &&
    echo $EXP_ROOT_HASH > root_hash_exp &&
    test_cmp root_hash_exp root_hash
  '

  test_expect_success "/cats hash looks good $EXTRA" '
    export EXP_CATS_HASH="$CATS_HASH" &&
    echo $EXP_CATS_HASH > cats_hash_exp &&
    ipfs files stat --hash /cats > cats_hash
    test_cmp cats_hash_exp cats_hash
  '

  test_expect_success "flush root succeeds $EXTRA" '
    ipfs files flush /
  '

  # test mv
  test_expect_success "can mv dir $EXTRA" '
    ipfs files mv /cats/this/is /cats/
  '

  test_expect_success "mv worked $EXTRA" '
    verify_dir_contents /cats file1 ipfs this is walrus &&
    verify_dir_contents /cats/this
  '

  test_expect_success "cleanup, remove 'cats' $EXTRA" '
    ipfs files rm -r /cats
  '

  test_expect_success "cleanup looks good $EXTRA" '
    verify_dir_contents /
  '

  # test truncating
  test_expect_success "create a new file $EXTRA" '
    echo "some content" | ipfs files write $ARGS $RAW_LEAVES --create /cats
  '

  test_expect_success "truncate and write over that file $EXTRA" '
    echo "fish" | ipfs files write $ARGS $RAW_LEAVES --truncate /cats
  '

  test_expect_success "output looks good $EXTRA" '
    ipfs files read /cats > file_out &&
    echo "fish" > file_exp &&
    test_cmp file_out file_exp
  '

  test_expect_success "file hash correct $EXTRA" '
    echo $TRUNC_HASH > filehash_expected &&
    ipfs files stat --hash /cats > filehash &&
    test_cmp filehash_expected filehash
  '

  test_expect_success "cleanup $EXTRA" '
    ipfs files rm /cats
  '

  # test flush flags
  test_expect_success "mkdir --flush works $EXTRA" '
    ipfs files mkdir $ARGS --flush --parents /flushed/deep
  '

  test_expect_success "mkdir --flush works a second time $EXTRA" '
    ipfs files mkdir $ARGS --flush --parents /flushed/deep
  '

  test_expect_success "dir looks right $EXTRA" '
    verify_dir_contents / flushed
  '

  test_expect_success "child dir looks right $EXTRA" '
    verify_dir_contents /flushed deep
  '

  test_expect_success "cleanup $EXTRA" '
    ipfs files rm -r /flushed
  '

  test_expect_success "child dir looks right $EXTRA" '
    verify_dir_contents /
  '

  # test for https://github.com/ipfs/go-ipfs/issues/2654
  test_expect_success "create and remove dir $EXTRA" '
    ipfs files mkdir $ARGS /test_dir &&
    ipfs files rm -r "/test_dir"
  '

  test_expect_success "create test file $EXTRA" '
    echo "content" | ipfs files write $ARGS $RAW_LEAVES -e "/test_file"
  '

  test_expect_success "copy test file onto test dir $EXTRA" '
    ipfs files cp "/test_file" "/test_dir"
  '

  test_expect_success "test /test_dir $EXTRA" '
    ipfs files stat "/test_dir" | grep -q "^Type: file"
  '

  test_expect_success "clean up /test_dir and /test_file $EXTRA" '
    ipfs files rm -r /test_dir &&
    ipfs files rm -r /test_file
  '

  test_expect_success "make a directory and a file $EXTRA" '
    ipfs files mkdir $ARGS /adir &&
    echo "blah" | ipfs files write $ARGS $RAW_LEAVES --create /foobar
  '

  test_expect_success "copy a file into a directory $EXTRA" '
    ipfs files cp /foobar /adir/
  '

  test_expect_success "file made it into directory $EXTRA" '
    ipfs files ls /adir | grep foobar
  '

  test_expect_success "clean up $EXTRA" '
    ipfs files rm -r /foobar &&
    ipfs files rm -r /adir
  '

  test_expect_success "root mfs entry is empty $EXTRA" '
    verify_dir_contents /
  '

  test_expect_success "repo gc $EXTRA" '
    ipfs repo gc
  '
}

# test offline and online

tests_for_files_api() {
  local EXTRA
  EXTRA=$1

  test_expect_success "can create some files for testing ($EXTRA)" '
    create_files
  '
  ROOT_HASH=QmcwKfTMCT7AaeiD92hWjnZn9b6eh9NxnhfSzN5x2vnDpt
  CATS_HASH=Qma88m8ErTGkZHbBWGqy1C7VmEmX8wwNDWNpGyCaNmEgwC
  FILE_HASH=QmQdQt9qooenjeaNhiKHF3hBvmNteB4MQBtgu3jxgf9c7i
  TRUNC_HASH=QmdaQZbLwK5ykweGdCVovNnvBom7QhikovDUVqTPHQG4L8
  test_files_api "($EXTRA)"

  test_expect_success "can create some files for testing with raw-leaves ($EXTRA)" '
    create_files --raw-leaves
  '

  if [ "$EXTRA" = "offline" ]; then
    ROOT_HASH=QmTpKiKcAj4sbeesN6vrs5w3QeVmd4QmGpxRL81hHut4dZ
    CATS_HASH=QmPhPkmtUGGi8ySPHoPu1qbfryLJKKq1GYxpgLyyCruvGe
    test_files_api "($EXTRA, partial raw-leaves)"
  fi

  ROOT_HASH=QmW3dMSU6VNd1mEdpk9S3ZYRuR1YwwoXjGaZhkyK6ru9YU
  CATS_HASH=QmPqWDEg7NoWRX8Y4vvYjZtmdg5umbfsTQ9zwNr12JoLmt
  FILE_HASH=QmRCgHeoKxCqK2Es6M6nPUDVWz19yNQPnsXGsXeuTkSKpN
  TRUNC_HASH=QmRFJEKWF5A5FyFYZgNhusLw2UziW9zBKYr4huyHjzcB6o
  test_files_api "($EXTRA, raw-leaves)" '' --raw-leaves

  ROOT_HASH=QmageRWxC7wWjPv5p36NeAgBAiFdBHaNfxAehBSwzNech2
  CATS_HASH=zdj7WkEzPLNAr5TYJSQC8CFcBjLvWFfGdx6kaBrJXnBguwWeX
  FILE_HASH=zdj7WYHvf5sBRgSBjYnq64QFr449CCbgupXfBvoYL3aHC1DzJ
  TRUNC_HASH=zdj7WYLYbka6Ydg8gZUJRLKnFBVehCADhQKBsFbNiMxZSB5Gj
  if [ "$EXTRA" = "offline" ]; then
    test_files_api "($EXTRA, cidv1)" --cid-version=1
  fi

  test_expect_success "can update root hash to cidv1" '
    ipfs files chcid --cid-version=1 / &&
    echo zdj7WbTaiJT1fgatdet9Ei9iDB5hdCxkbVyhyh8YTUnXMiwYi > hash_expect &&
    ipfs files stat --hash / > hash_actual &&
    test_cmp hash_expect hash_actual
  '

  ROOT_HASH=zdj7Whmtnx23bR7c7E1Yn3zWYWjnvT4tpzWYGaBMyqcopDWrx
    test_files_api "($EXTRA, cidv1 root)"

  if [ "$EXTRA" = "offline" ]; then
    test_expect_success "can update root hash to blake2b-256" '
    ipfs files chcid --hash=blake2b-256 / &&
      echo zDMZof1kvswQMT8txrmnb3JGBuna6qXCTry6hSifrkZEd6VmHbBm > hash_expect &&
      ipfs files stat --hash / > hash_actual &&
      test_cmp hash_expect hash_actual
    '
    ROOT_HASH=zDMZof1kxEsAwSgCZsGQRVcHCMtHLjkUQoiZUbZ87erpPQJGUeW8
    CATS_HASH=zDMZof1kuAhr3zBkxq48V7o9HJZCTVyu1Wd9wnZtVcPJLW8xnGft
    FILE_HASH=zDMZof1kxbB9CvxgRioBzESbGnZUxtSCsZ18H1EUkxDdWt1DYEkK
    TRUNC_HASH=zDMZof1kxXqKdVsVo231qVdN3hCTF5a34UuQZpzmm5K7CbRJ4u2S
    test_files_api "($EXTRA, blake2b-256 root)"
  fi

  test_expect_success "can update root hash back to cidv0" '
    ipfs files chcid / --cid-version=0 &&
    echo QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn > hash_expect &&
    ipfs files stat --hash / > hash_actual &&
    test_cmp hash_expect hash_actual
  '
}

tests_for_files_api "online"

test_launch_ipfs_daemon --offline

ONLINE=1 # set online flag so tests can easily tell

tests_for_files_api "offline"

test_kill_ipfs_daemon --offline

test_expect_success "enable sharding in config" '
  ipfs config --json Experimental.ShardingEnabled true
'

test_launch_ipfs_daemon --offline

SHARD_HASH=QmPkwLJTYZRGPJ8Lazr9qPdrLmswPtUjaDbEpmR9jEh1se
test_sharding "(cidv0)"

SHARD_HASH=zdj7WZXr6vG2Ne7ZLHGEKrGyF3pHBfAViEnmH9CoyvjrFQM8E
test_sharding "(cidv1 root)" "--cid-version=1"

test_kill_ipfs_daemon

test_done
