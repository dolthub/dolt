#!/bin/sh
#
# Copyright (c) 2016 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test dag command"

. lib/test-lib.sh

test_init_ipfs

test_expect_success "make a few test files" '
	echo "foo" > file1 &&
	echo "bar" > file2 &&
	echo "baz" > file3 &&
	echo "qux" > file4 &&
	HASH1=$(ipfs add --pin=false -q file1) &&
	HASH2=$(ipfs add --pin=false -q file2) &&
	HASH3=$(ipfs add --pin=false -q file3) &&
	HASH4=$(ipfs add --pin=false -q file4)
'

test_expect_success "make an ipld object in json" '
	printf "{\"hello\":\"world\",\"cats\":[{\"/\":\"%s\"},{\"water\":{\"/\":\"%s\"}}],\"magic\":{\"/\":\"%s\"},\"sub\":{\"dict\":\"ionary\",\"beep\":[0,\"bop\"]}}" $HASH1 $HASH2 $HASH3 > ipld_object
'

test_dag_cmd() {
	test_expect_success "can add an ipld object" '
		IPLDHASH=$(cat ipld_object | ipfs dag put)
	'

	test_expect_success "output looks correct" '
		EXPHASH="zdpuAsXfkHapxohc8LtsCzYiAsy84ESqKRD8eWuY64tt9r2CE"
		test $EXPHASH = $IPLDHASH
	'

	test_expect_success "various path traversals work" '
		ipfs cat $IPLDHASH/cats/0 > out1 &&
		ipfs cat $IPLDHASH/cats/1/water > out2 &&
		ipfs cat $IPLDHASH/magic > out3
	'

	test_expect_success "outputs look correct" '
		test_cmp file1 out1 &&
		test_cmp file2 out2 &&
		test_cmp file3 out3
	'

	test_expect_success "resolving sub-objects works" '
		ipfs dag get $IPLDHASH/hello > sub1 &&
		ipfs dag get $IPLDHASH/sub > sub2 &&
		ipfs dag get $IPLDHASH/sub/beep > sub3 &&
		ipfs dag get $IPLDHASH/sub/beep/0 > sub4 &&
		ipfs dag get $IPLDHASH/sub/beep/1 > sub5
	'

	test_expect_success "sub-objects look right" '
		echo "\"world\"" > sub1_exp &&
		test_cmp sub1_exp sub1 &&
		echo "{\"beep\":[0,\"bop\"],\"dict\":\"ionary\"}" > sub2_exp &&
		test_cmp sub2_exp sub2 &&
		echo "[0,\"bop\"]" > sub3_exp &&
		test_cmp sub3_exp sub3 &&
		echo "0" > sub4_exp &&
		test_cmp sub4_exp sub4 &&
		echo "\"bop\"" > sub5_exp &&
		test_cmp sub5_exp sub5
	'

	test_expect_success "can pin cbor object" '
		ipfs pin add $EXPHASH
	'

	test_expect_success "after gc, objects still acessible" '
		ipfs repo gc > /dev/null &&
		ipfs refs -r --timeout=2s $EXPHASH > /dev/null
	'

	test_expect_success "can get object" '
		ipfs dag get $IPLDHASH > ipld_obj_out
	'

	test_expect_success "object links look right" '
		grep "{\"/\":\"" ipld_obj_out > /dev/null
	'

	test_expect_success "retreived object hashes back correctly" '
		IPLDHASH2=$(cat ipld_obj_out | ipfs dag put) &&
		test "$IPLDHASH" = "$IPLDHASH2"
	'

	test_expect_success "add a normal file" '
		HASH=$(echo "foobar" | ipfs add -q)
	'

	test_expect_success "can view protobuf object with dag get" '
		ipfs dag get $HASH > dag_get_pb_out
	'

	test_expect_success "output looks correct" '
		echo "{\"data\":\"CAISB2Zvb2JhcgoYBw==\",\"links\":[]}" > dag_get_pb_exp &&
		test_cmp dag_get_pb_exp dag_get_pb_out
	'

	test_expect_success "can call dag get with a path" '
		ipfs dag get $IPLDHASH/cats/0 > cat_out
	'

	test_expect_success "output looks correct" '
		echo "{\"data\":\"CAISBGZvbwoYBA==\",\"links\":[]}" > cat_exp &&
		test_cmp cat_exp cat_out
	'

	test_expect_success "non-canonical cbor input is normalized" '
		HASH=$(cat ../t0053-dag-data/non-canon.cbor | ipfs dag put --format=cbor --input-enc=raw) &&
		test $HASH = "zdpuAmxF8q6iTUtkB3xtEYzmc5Sw762qwQJftt5iW8NTWLtjC" ||
		test_fsh echo $HASH
	'

	test_expect_success "non-canonical cbor input is normalized with input-enc cbor" '
		HASH=$(cat ../t0053-dag-data/non-canon.cbor | ipfs dag put --format=cbor --input-enc=cbor) &&
		test $HASH = "zdpuAmxF8q6iTUtkB3xtEYzmc5Sw762qwQJftt5iW8NTWLtjC" ||
		test_fsh echo $HASH
	'

	test_expect_success "add an ipld with pin" '
		PINHASH=$(printf {\"foo\":\"bar\"} | ipfs dag put --pin=true)
	'

	test_expect_success "after gc, objects still acessible" '
		ipfs repo gc > /dev/null &&
		ipfs refs -r --timeout=2s $PINHASH > /dev/null
	'

	test_expect_success "can add an ipld object with sha3 hash" '
		IPLDHASH=$(cat ipld_object | ipfs dag put --hash sha3)
	'

	test_expect_success "output looks correct" '
		EXPHASH="zBwWX8u9LYZdCWqaryJW8QsBstghHSPy41nfhhFLY9qw1Vu2BWqnMFtk1jL3qCtEdGd7Kqw1HNPZv5z8LxP2eHGGDCdRE"
		test $EXPHASH = $IPLDHASH
	'

	test_expect_success "prepare dag-pb object" '
		echo foo > test_file &&
		HASH=$(ipfs add -wq test_file | tail -n1)
	'

	test_expect_success "dag put with json dag-pb works" '
		ipfs dag get $HASH > pbjson &&
		cat pbjson | ipfs dag put --format=dag-pb --input-enc=json > dag_put_out
	'

	test_expect_success "dag put with dag-pb works output looks good" '
		printf $HASH > dag_put_exp &&
		test_cmp dag_put_exp dag_put_out
	'

	test_expect_success "dag put with raw dag-pb works" '
		ipfs block get $HASH > pbraw &&
		cat pbraw | ipfs dag put --format=dag-pb --input-enc=raw > dag_put_out
	'

	test_expect_success "dag put with dag-pb works output looks good" '
		printf $HASH > dag_put_exp &&
		test_cmp dag_put_exp dag_put_out
	'

	test_expect_success "prepare data for dag resolve" '
		NESTED_HASH=$(echo "{\"data\":123}" | ipfs dag put) &&
		HASH=$(echo "{\"obj\":{\"/\":\"${NESTED_HASH}\"}}" | ipfs dag put)
	'

	test_expect_success "dag resolve some things" '
		ipfs dag resolve $HASH > resolve_hash &&
		ipfs dag resolve ${HASH}/obj > resolve_obj &&
		ipfs dag resolve ${HASH}/obj/data > resolve_data
	'

	test_expect_success "dag resolve output looks good" '
		printf $HASH > resolve_hash_exp &&
		printf $NESTED_HASH > resolve_obj_exp &&
		printf $NESTED_HASH/data > resolve_data_exp &&

		test_cmp resolve_hash_exp resolve_hash &&
		test_cmp resolve_obj_exp resolve_obj &&
		test_cmp resolve_data_exp resolve_data
	'
}

# should work offline
test_dag_cmd

# should work online
test_launch_ipfs_daemon
test_dag_cmd
test_kill_ipfs_daemon

test_done
