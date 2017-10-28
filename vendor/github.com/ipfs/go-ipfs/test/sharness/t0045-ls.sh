#!/bin/sh
#
# Copyright (c) 2014 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test ls command"

. lib/test-lib.sh

test_init_ipfs

test_ls_cmd() {

  test_expect_success "'ipfs add -r testData' succeeds" '
    mkdir -p testData testData/d1 testData/d2 &&
    echo "test" >testData/f1 &&
    echo "data" >testData/f2 &&
    echo "hello" >testData/d1/a &&
    random 128 42 >testData/d1/128 &&
    echo "world" >testData/d2/a &&
    random 1024 42 >testData/d2/1024 &&
    ipfs add -r testData >actual_add
  '

  test_expect_success "'ipfs add' output looks good" '
    cat <<-\EOF >expected_add &&
added QmQNd6ubRXaNG6Prov8o6vk3bn6eWsj9FxLGrAVDUAGkGe testData/d1/128
added QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN testData/d1/a
added QmbQBUSRL9raZtNXfpTDeaxQapibJEG6qEY8WqAN22aUzd testData/d2/1024
added QmaRGe7bVmVaLmxbrMiVNXqW4pRNNp3xq7hFtyRKA3mtJL testData/d2/a
added QmeomffUNfmQy76CQGy9NdmqEnnHU9soCexBnGU3ezPHVH testData/f1
added QmNtocSs7MoDkJMc1RkyisCSKvLadujPsfJfSdJ3e1eA1M testData/f2
added QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss testData/d1
added QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy testData/d2
added QmfNy183bXiRVyrhyWtq3TwHn79yHEkiAGFr18P7YNzESj testData
EOF
    test_cmp expected_add actual_add
  '

  test_expect_success "'ipfs ls <three dir hashes>' succeeds" '
    ipfs ls QmfNy183bXiRVyrhyWtq3TwHn79yHEkiAGFr18P7YNzESj QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss >actual_ls
  '

  test_expect_success "'ipfs ls <three dir hashes>' output looks good" '
    cat <<-\EOF >expected_ls &&
QmfNy183bXiRVyrhyWtq3TwHn79yHEkiAGFr18P7YNzESj:
QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss 246  d1/
QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy 1143 d2/
QmeomffUNfmQy76CQGy9NdmqEnnHU9soCexBnGU3ezPHVH 13   f1
QmNtocSs7MoDkJMc1RkyisCSKvLadujPsfJfSdJ3e1eA1M 13   f2

QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy:
QmbQBUSRL9raZtNXfpTDeaxQapibJEG6qEY8WqAN22aUzd 1035 1024
QmaRGe7bVmVaLmxbrMiVNXqW4pRNNp3xq7hFtyRKA3mtJL 14   a

QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss:
QmQNd6ubRXaNG6Prov8o6vk3bn6eWsj9FxLGrAVDUAGkGe 139 128
QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN 14  a

EOF
    test_cmp expected_ls actual_ls
  '

  test_expect_success "'ipfs ls --headers <three dir hashes>' succeeds" '
    ipfs ls --headers QmfNy183bXiRVyrhyWtq3TwHn79yHEkiAGFr18P7YNzESj QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss >actual_ls_headers
  '

  test_expect_success "'ipfs ls --headers  <three dir hashes>' output looks good" '
    cat <<-\EOF >expected_ls_headers &&
QmfNy183bXiRVyrhyWtq3TwHn79yHEkiAGFr18P7YNzESj:
Hash                                           Size Name
QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss 246  d1/
QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy 1143 d2/
QmeomffUNfmQy76CQGy9NdmqEnnHU9soCexBnGU3ezPHVH 13   f1
QmNtocSs7MoDkJMc1RkyisCSKvLadujPsfJfSdJ3e1eA1M 13   f2

QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy:
Hash                                           Size Name
QmbQBUSRL9raZtNXfpTDeaxQapibJEG6qEY8WqAN22aUzd 1035 1024
QmaRGe7bVmVaLmxbrMiVNXqW4pRNNp3xq7hFtyRKA3mtJL 14   a

QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss:
Hash                                           Size Name
QmQNd6ubRXaNG6Prov8o6vk3bn6eWsj9FxLGrAVDUAGkGe 139  128
QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN 14   a

EOF
    test_cmp expected_ls_headers actual_ls_headers
  '
}

test_ls_cmd_raw_leaves() {
  test_expect_success "'ipfs add -r --raw-leaves' then 'ipfs ls' works as expected" '
    mkdir -p somedir &&
    echo bar > somedir/foo &&
    ipfs add --raw-leaves -r somedir/ > /dev/null &&
    ipfs ls QmThNTdtKaVoCVrYmM5EBS6U3S5vfKFue2TxbxxAxRcKKE > ls-actual
    echo "zb2rhf6GzX4ckKZtjy8yy8iyq1KttCrRyqDedD6xubhY3sw2F 4 foo" > ls-expect
    test_cmp ls-actual ls-expect
  '
}

test_ls_object() {
  test_expect_success "ipfs add medium size file then 'ipfs ls' works as expected" '
    random 500000 2 > somefile &&
    HASH=$(ipfs add somefile -q) &&
    echo "QmPrM8S5T7Q3M8DQvQMS7m41m3Aq4jBjzAzvky5fH3xfr4 262158 " > ls-expect &&
    echo "QmdaAntAzQqqVMo4B8V69nkQd5d918YjHXUe2oF6hr72ri 237870 " >> ls-expect &&
    ipfs ls $HASH > ls-actual &&
    test_cmp ls-actual ls-expect
  '
}

# should work offline
test_ls_cmd
test_ls_cmd_raw_leaves
test_ls_object

# should work online
test_launch_ipfs_daemon
test_ls_cmd
test_ls_cmd_raw_leaves
test_kill_ipfs_daemon
test_ls_object

#
# test for ls --resolve-type=false
#

test_expect_success "'ipfs add -r' succeeds" '
  mkdir adir &&
  # note: not using a seed as the files need to have truly random content
  random 1000 > adir/file1 &&
  random 1000 > adir/file2 &&
  ipfs add --pin=false -q -r adir > adir-hashes
'

test_expect_success "get hashes from add output" '
  FILE=`head -1 adir-hashes` &&
  DIR=`tail -1 adir-hashes` &&
  test "$FILE" -a "$DIR"
'

test_expect_success "remove a file in dir" '
  ipfs block rm $FILE
'

test_expect_success "'ipfs ls --resolve-type=false ' ok" '
  ipfs ls --resolve-type=false $DIR > /dev/null
'

test_expect_success "'ipfs ls' fails" '
  test_must_fail ipfs ls $DIR
'

test_launch_ipfs_daemon --offline

test_expect_success "'ipfs ls --resolve-type=false' ok" '
  ipfs ls --resolve-type=false $DIR > /dev/null
'

test_expect_success "'ipfs ls' fails" '
  test_must_fail ipfs ls $DIR
'

test_kill_ipfs_daemon

test_launch_ipfs_daemon

# now we try `ipfs ls --resolve-type=false` with the daemon online It
# should not even attempt to retrieve the file from the network.  If
# it does it should eventually fail as the content is random and
# should not exist on the network, but we don't want to wait for a
# timeout so we will kill the request after a few seconds
test_expect_success "'ipfs ls --resolve-type=false' ok and does not hang" '
  go-timeout 2 ipfs ls --resolve-type=false $DIR
'

test_kill_ipfs_daemon

test_done
