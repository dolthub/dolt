#!/bin/sh
#
# Copyright (c) 2014 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test file ls command"

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

  test_expect_success "'ipfs file ls <dir>' succeeds" '
    ipfs file ls QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy >actual_ls_one_directory
  '

  test_expect_success "'ipfs file ls <dir>' output looks good" '
    cat <<-\EOF >expected_ls_one_directory &&
1024
a
EOF
    test_cmp expected_ls_one_directory actual_ls_one_directory
  '

  test_expect_success "'ipfs file ls <three dir hashes>' succeeds" '
    ipfs file ls QmfNy183bXiRVyrhyWtq3TwHn79yHEkiAGFr18P7YNzESj QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss >actual_ls_three_directories
  '

  test_expect_success "'ipfs file ls <three dir hashes>' output looks good" '
    cat <<-\EOF >expected_ls_three_directories &&
QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy:
1024
a

QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss:
128
a

QmfNy183bXiRVyrhyWtq3TwHn79yHEkiAGFr18P7YNzESj:
d1
d2
f1
f2
EOF
    test_cmp expected_ls_three_directories actual_ls_three_directories
  '

  test_expect_success "'ipfs file ls <file hashes>' succeeds" '
    ipfs file ls /ipfs/QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy/1024 QmQNd6ubRXaNG6Prov8o6vk3bn6eWsj9FxLGrAVDUAGkGe >actual_ls_file
  '

  test_expect_success "'ipfs file ls <file hashes>' output looks good" '
    cat <<-\EOF >expected_ls_file &&
/ipfs/QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy/1024
QmQNd6ubRXaNG6Prov8o6vk3bn6eWsj9FxLGrAVDUAGkGe
EOF
    test_cmp expected_ls_file actual_ls_file
  '

  test_expect_success "'ipfs file ls <duplicates>' succeeds" '
    ipfs file ls /ipfs/QmfNy183bXiRVyrhyWtq3TwHn79yHEkiAGFr18P7YNzESj/d1 /ipfs/QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss /ipfs/QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy/1024 /ipfs/QmbQBUSRL9raZtNXfpTDeaxQapibJEG6qEY8WqAN22aUzd >actual_ls_duplicates_file
  '

  test_expect_success "'ipfs file ls <duplicates>' output looks good" '
    cat <<-\EOF >expected_ls_duplicates_file &&
/ipfs/QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy/1024
/ipfs/QmbQBUSRL9raZtNXfpTDeaxQapibJEG6qEY8WqAN22aUzd

/ipfs/QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss:
/ipfs/QmfNy183bXiRVyrhyWtq3TwHn79yHEkiAGFr18P7YNzESj/d1:
128
a
EOF
    test_cmp expected_ls_duplicates_file actual_ls_duplicates_file
  '

  test_expect_success "'ipfs --encoding=json file ls <file hashes>' succeeds" '
    ipfs --encoding=json file ls /ipfs/QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy/1024 >actual_json_ls_file
  '

  test_expect_success "'ipfs --encoding=json file ls <file hashes>' output looks good" '
    cat <<-\EOF >expected_json_ls_file_trailing_newline &&
{"Arguments":{"/ipfs/QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy/1024":"QmbQBUSRL9raZtNXfpTDeaxQapibJEG6qEY8WqAN22aUzd"},"Objects":{"QmbQBUSRL9raZtNXfpTDeaxQapibJEG6qEY8WqAN22aUzd":{"Hash":"QmbQBUSRL9raZtNXfpTDeaxQapibJEG6qEY8WqAN22aUzd","Size":1024,"Type":"File","Links":null}}}
EOF
    printf "%s\n" "$(cat expected_json_ls_file_trailing_newline)" >expected_json_ls_file &&
    test_cmp expected_json_ls_file actual_json_ls_file
  '

  test_expect_success "'ipfs --encoding=json file ls <duplicates>' succeeds" '
    ipfs --encoding=json file ls /ipfs/QmfNy183bXiRVyrhyWtq3TwHn79yHEkiAGFr18P7YNzESj/d1 /ipfs/QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss /ipfs/QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy/1024 /ipfs/QmbQBUSRL9raZtNXfpTDeaxQapibJEG6qEY8WqAN22aUzd >actual_json_ls_duplicates_file
  '

  test_expect_success "'ipfs --encoding=json file ls <duplicates>' output looks good" '
      cat <<-\EOF >expected_json_ls_duplicates_file_trailing_newline &&
{"Arguments":{"/ipfs/QmR3jhV4XpxxPjPT3Y8vNnWvWNvakdcT3H6vqpRBsX1MLy/1024":"QmbQBUSRL9raZtNXfpTDeaxQapibJEG6qEY8WqAN22aUzd","/ipfs/QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss":"QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss","/ipfs/QmbQBUSRL9raZtNXfpTDeaxQapibJEG6qEY8WqAN22aUzd":"QmbQBUSRL9raZtNXfpTDeaxQapibJEG6qEY8WqAN22aUzd","/ipfs/QmfNy183bXiRVyrhyWtq3TwHn79yHEkiAGFr18P7YNzESj/d1":"QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss"},"Objects":{"QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss":{"Hash":"QmSix55yz8CzWXf5ZVM9vgEvijnEeeXiTSarVtsqiiCJss","Size":0,"Type":"Directory","Links":[{"Name":"128","Hash":"QmQNd6ubRXaNG6Prov8o6vk3bn6eWsj9FxLGrAVDUAGkGe","Size":128,"Type":"File"},{"Name":"a","Hash":"QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN","Size":6,"Type":"File"}]},"QmbQBUSRL9raZtNXfpTDeaxQapibJEG6qEY8WqAN22aUzd":{"Hash":"QmbQBUSRL9raZtNXfpTDeaxQapibJEG6qEY8WqAN22aUzd","Size":1024,"Type":"File","Links":null}}}
EOF
      printf "%s\n" "$(cat expected_json_ls_duplicates_file_trailing_newline)" >expected_json_ls_duplicates_file &&
      test_cmp expected_json_ls_duplicates_file actual_json_ls_duplicates_file
  '

}


# should work offline
test_ls_cmd

# should work online
test_launch_ipfs_daemon
test_ls_cmd
test_kill_ipfs_daemon

test_done
