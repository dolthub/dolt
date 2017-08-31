#!/bin/sh
#
# Copyright (c) 2015 Henry Bubert
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test object command"

. lib/test-lib.sh

test_init_ipfs

test_patch_create_path() {
	root=$1
	name=$2
	target=$3

	test_expect_success "object patch --create works" '
		PCOUT=$(ipfs object patch $root add-link --create $name $target)
	'

	test_expect_success "output looks good" '
		ipfs cat "$PCOUT/$name" >tpcp_out &&
		ipfs cat "$target" >tpcp_exp &&
		test_cmp tpcp_exp tpcp_out
	'
}

test_object_cmd() {

	test_expect_success "'ipfs add testData' succeeds" '
		printf "Hello Mars" >expected_in &&
		ipfs add expected_in >actual_Addout
	'

	test_expect_success "'ipfs add testData' output looks good" '
		HASH="QmWkHFpYBZ9mpPRreRbMhhYWXfUhBAue3JkbbpFqwowSRb" &&
		echo "added $HASH expected_in" >expected_Addout &&
		test_cmp expected_Addout actual_Addout
	'

	test_expect_success "'ipfs object get' succeeds" '
		ipfs object get $HASH >actual_getOut
	'

	test_expect_success "'ipfs object get' output looks good" '
		test_cmp ../t0051-object-data/expected_getOut actual_getOut
	'

	test_expect_success "'ipfs object stat' succeeds" '
		ipfs object stat $HASH >actual_stat
	'

	test_expect_success "'ipfs object get' output looks good" '
		echo "NumLinks: 0" > expected_stat &&
		echo "BlockSize: 18" >> expected_stat &&
		echo "LinksSize: 2" >> expected_stat &&
		echo "DataSize: 16" >> expected_stat &&
		echo "CumulativeSize: 18" >> expected_stat &&
		test_cmp expected_stat actual_stat
	'

	test_expect_success "'ipfs object put file.json' succeeds" '
		ipfs object put  ../t0051-object-data/testPut.json > actual_putOut
	'

	test_expect_success "'ipfs object put file.json' output looks good" '
		HASH="QmUTSAdDi2xsNkDtLqjFgQDMEn5di3Ab9eqbrt4gaiNbUD" &&
		printf "added $HASH\n" > expected_putOut &&
		test_cmp expected_putOut actual_putOut
	'

	test_expect_success "'ipfs object put file.xml' succeeds" '
		ipfs object put  ../t0051-object-data/testPut.xml --inputenc=xml > actual_putOut
	'

	test_expect_success "'ipfs object put file.xml' output looks good" '
		HASH="QmQzNKUHy4HyEUGkqKe3q3t796ffPLQXYCkHCcXUNT5JNK" &&
		printf "added $HASH\n" > expected_putOut &&
		test_cmp expected_putOut actual_putOut
	'

	test_expect_success "'ipfs object put' from stdin succeeds" '
		cat ../t0051-object-data/testPut.xml | ipfs object put --inputenc=xml > actual_putStdinOut
	'

	test_expect_success "'ipfs object put broken.xml' should fail" '
		test_expect_code 1 ipfs object put ../t0051-object-data/brokenPut.xml --inputenc=xml 2>actual_putBrokenErr >actual_putBroken
	'

	test_expect_success "'ipfs object put broken.hxml' output looks good" '
		touch expected_putBroken &&
		printf "Error: no data or links in this node\n" > expected_putBrokenErr &&
		test_cmp expected_putBroken actual_putBroken &&
		test_cmp expected_putBrokenErr actual_putBrokenErr
	'
	test_expect_success "'ipfs object get --enc=xml' succeeds" '
		ipfs object get --enc=xml $HASH >utf8_xml
	'

	test_expect_success "'ipfs object put --inputenc=xml' succeeds" '
		ipfs object put --inputenc=xml <utf8_xml >actual
	'

	test_expect_failure "'ipfs object put --inputenc=xml' output looks good" '
		echo "added $HASH\n" >expected &&
		test_cmp expected actual
	'

	test_expect_success "'ipfs object put file.pb' succeeds" '
		ipfs object put --inputenc=protobuf ../t0051-object-data/testPut.pb > actual_putOut
	'

	test_expect_success "'ipfs object put file.pb' output looks good" '
		HASH="QmUTSAdDi2xsNkDtLqjFgQDMEn5di3Ab9eqbrt4gaiNbUD" &&
		printf "added $HASH\n" > expected_putOut &&
		test_cmp expected_putOut actual_putOut
	'

	test_expect_success "'ipfs object put' from stdin succeeds" '
		cat ../t0051-object-data/testPut.json | ipfs object put > actual_putStdinOut
	'

	test_expect_success "'ipfs object put' from stdin output looks good" '
		HASH="QmUTSAdDi2xsNkDtLqjFgQDMEn5di3Ab9eqbrt4gaiNbUD" &&
		printf "added $HASH\n" > expected_putStdinOut &&
		test_cmp expected_putStdinOut actual_putStdinOut
	'

	test_expect_success "'ipfs object put' from stdin (pb) succeeds" '
		cat ../t0051-object-data/testPut.pb | ipfs object put --inputenc=protobuf > actual_putPbStdinOut
	'

	test_expect_success "'ipfs object put' from stdin (pb) output looks good" '
		HASH="QmUTSAdDi2xsNkDtLqjFgQDMEn5di3Ab9eqbrt4gaiNbUD" &&
		printf "added $HASH\n" > expected_putStdinOut &&
		test_cmp expected_putStdinOut actual_putPbStdinOut
	'

	test_expect_success "'ipfs object put broken.json' should fail" '
		test_expect_code 1 ipfs object put ../t0051-object-data/brokenPut.json 2>actual_putBrokenErr >actual_putBroken
	'

	test_expect_success "'ipfs object put broken.hjson' output looks good" '
		touch expected_putBroken &&
		printf "Error: no data or links in this node\n" > expected_putBrokenErr &&
		test_cmp expected_putBroken actual_putBroken &&
		test_cmp expected_putBrokenErr actual_putBrokenErr
	'

	test_expect_success "setup: add UTF-8 test file" '
		HASH="QmNY5sQeH9ttVCg24sizH71dNbcZTpGd7Yb3YwsKZ4jiFP" &&
		ipfs add ../t0051-object-data/UTF-8-test.txt >actual &&
		echo "added $HASH UTF-8-test.txt" >expected &&
		test_cmp expected actual
	'

	test_expect_success "'ipfs object get --enc=json' succeeds" '
		ipfs object get --enc=json $HASH >utf8_json
	'

	test_expect_success "'ipfs object put --inputenc=json' succeeds" '
		ipfs object put --inputenc=json <utf8_json >actual
	'

	test_expect_failure "'ipfs object put --inputenc=json' output looks good" '
		echo "added $HASH" >expected &&
		test_cmp expected actual
	'

	test_expect_success "'ipfs object put --pin' succeeds" '
		HASH="QmXg9Pp2ytZ14xgmQjYEiHjVjMFXzCVVEcRTWJBmLgR39V" &&
		echo "added $HASH" >expected &&
		echo "{ \"Data\": \"abc\" }" | ipfs object put --pin >actual
	'

	test_expect_success "'ipfs object put --pin' output looks good" '
		echo "added $HASH" >expected &&
		test_cmp expected actual
	'

	test_expect_success "after gc, objects still acessible" '
		ipfs repo gc > /dev/null &&
		ipfs refs -r --timeout=2s $HASH > /dev/null
	'

	test_expect_success "'ipfs object patch' should work (no unixfs-dir)" '
		EMPTY_DIR=$(ipfs object new) &&
		OUTPUT=$(ipfs object patch $EMPTY_DIR add-link foo $EMPTY_DIR) &&
		ipfs object stat $OUTPUT
	'

	test_expect_success "'ipfs object patch' should work" '
		EMPTY_DIR=$(ipfs object new unixfs-dir) &&
		OUTPUT=$(ipfs object patch $EMPTY_DIR add-link foo $EMPTY_DIR) &&
		ipfs object stat $OUTPUT
	'

	test_expect_success "'ipfs object patch add-link' should work with paths" '
		EMPTY_DIR=$(ipfs object new unixfs-dir) &&
		N1=$(ipfs object patch $EMPTY_DIR add-link baz $EMPTY_DIR) &&
		N2=$(ipfs object patch $EMPTY_DIR add-link bar $N1) &&
		N3=$(ipfs object patch $EMPTY_DIR add-link foo /ipfs/$N2/bar) &&
		ipfs object stat /ipfs/$N3 > /dev/null &&
		ipfs object stat $N3/foo > /dev/null &&
		ipfs object stat /ipfs/$N3/foo/baz > /dev/null
	'

	test_expect_success "object patch creation looks right" '
		echo "QmPc73aWK9dgFBXe86P4PvQizHo9e5Qt7n7DAMXWuigFuG" > hash_exp &&
		echo $N3 > hash_actual &&
		test_cmp hash_exp hash_actual
	'

	test_expect_success "multilayer ipfs patch works" '
		echo "hello world" > hwfile &&
		FILE=$(ipfs add -q hwfile) &&
		EMPTY=$(ipfs object new unixfs-dir) &&
		ONE=$(ipfs object patch $EMPTY add-link b $EMPTY) &&
		TWO=$(ipfs object patch $EMPTY add-link a $ONE) &&
		ipfs object patch $TWO add-link a/b/c $FILE > multi_patch
	'

	test_expect_success "output looks good" '
		ipfs cat $(cat multi_patch)/a/b/c > hwfile_out &&
		test_cmp hwfile hwfile_out
	'

	test_expect_success "ipfs object stat path succeeds" '
		ipfs object stat $(cat multi_patch)/a > obj_stat_out
	'

	test_expect_success "ipfs object stat output looks good" '
		echo NumLinks: 1 > obj_stat_exp &&
		echo BlockSize: 47 >> obj_stat_exp &&
		echo LinksSize: 45 >> obj_stat_exp &&
		echo DataSize: 2 >> obj_stat_exp &&
		echo CumulativeSize: 114 >> obj_stat_exp &&

		test_cmp obj_stat_exp obj_stat_out
	'

	test_expect_success "should have created dir within a dir" '
		ipfs ls $OUTPUT > patched_output
	'

	test_expect_success "output looks good" '
		echo "QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn 4 foo/" > patched_exp &&
		test_cmp patched_exp patched_output
	'

	test_expect_success "can remove the directory" '
		ipfs object patch $OUTPUT rm-link foo > rmlink_output
	'

	test_expect_success "output should be empty" '
		echo QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn > rmlink_exp &&
		test_cmp rmlink_exp rmlink_output
	'

	test_expect_success "multilayer rm-link should work" '
		ipfs object patch $(cat multi_patch) rm-link a/b/c > multi_link_rm_out
	'

	test_expect_success "output looks good" '
		echo "QmZD3r9cZjzU8huNY2JS9TC6n8daDfT8TmE8zBSqG31Wvq" > multi_link_rm_exp &&
		test_cmp multi_link_rm_exp multi_link_rm_out
	'

	test_patch_create_path $EMPTY a/b/c $FILE

	test_patch_create_path $EMPTY a $FILE

	test_patch_create_path $EMPTY a/b/b/b/b $FILE

	test_expect_success "can create blank object" '
		BLANK=$(ipfs object new)
	'

	test_patch_create_path $BLANK a $FILE

	test_expect_success "create bad path fails" '
		test_must_fail ipfs object patch $EMPTY add-link --create / $FILE
	'

	test_expect_success "patch set-data works" '
		EMPTY=$(ipfs object new) &&
		HASH=$(printf "foo" | ipfs object patch $EMPTY set-data)
	'

	test_expect_success "output looks good" '
		echo "{\"Links\":[],\"Data\":\"foo\"}" > exp_data_set &&
		ipfs object get $HASH > actual_data_set &&
		test_cmp exp_data_set actual_data_set
	'

	test_expect_success "patch append-data works" '
		HASH=$(printf "bar" | ipfs object patch $HASH append-data)
	'

	test_expect_success "output looks good" '
		echo "{\"Links\":[],\"Data\":\"foobar\"}" > exp_data_append &&
		ipfs object get $HASH > actual_data_append &&
		test_cmp exp_data_append actual_data_append
	'
}

test_object_content_type() {

	  test_expect_success "'ipfs object get --encoding=protobuf' returns the correct content type" '
    curl -sI "http://$API_ADDR/api/v0/object/get?arg=$HASH&encoding=protobuf" | grep -q "^Content-Type: application/protobuf"
  '

	  test_expect_success "'ipfs object get --encoding=json' returns the correct content type" '
    curl -sI "http://$API_ADDR/api/v0/object/get?arg=$HASH&encoding=json" | grep -q "^Content-Type: application/json"
  '

	  test_expect_success "'ipfs object get --encoding=text' returns the correct content type" '
    curl -sI "http://$API_ADDR/api/v0/object/get?arg=$HASH&encoding=text" | grep -q "^Content-Type: text/plain"
  '

	  test_expect_success "'ipfs object get --encoding=xml' returns the correct content type" '
  curl -sI "http://$API_ADDR/api/v0/object/get?arg=$HASH&encoding=xml" | grep -q "^Content-Type: application/xml"
  '
}

# should work offline
test_object_cmd

# should work online
test_launch_ipfs_daemon
test_object_cmd
test_object_content_type
test_kill_ipfs_daemon

test_done
