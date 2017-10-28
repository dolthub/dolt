#!/bin/sh
#
# Copyright (c) 2017 Jakub Sztandera
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test git plugin"

. lib/test-lib.sh

# if in travis CI, dont test mount (no fuse)
if ! test_have_prereq PLUGIN; then
  skip_all='skipping git plugin tests, plugins not available'

  test_done
fi

test_init_ipfs

test_expect_success "copy plugin" '
  mkdir -p "$IPFS_PATH/plugins" &&
  cp ../plugins/git.so "$IPFS_PATH/plugins/"
'

# from https://github.com/ipfs/go-ipld-git/blob/master/make-test-repo.sh
test_expect_success "prepare test data" '
  tar xzf ../t0280-plugin-git-data/git.tar.gz
'

test_dag_git() {
  test_expect_success "add objects via dag put" '
    find objects -type f -exec ipfs dag put --format=git --input-enc=zlib {} \; -exec echo \; > hashes
  '

  test_expect_success "successfully get added objects" '
    cat hashes | xargs -i ipfs dag get -- {} > /dev/null
  '

  test_expect_success "path traversals work" '
    echo \"YmxvYiA3ACcsLnB5Zgo=\" > file1 &&
    ipfs dag get z8mWaJh5RLq16Zwgtd8gZxd63P4hgwNNx/object/parents/0/tree/dir2/hash/f3/hash > out1
  '

  test_expect_success "outputs look correct" '
    test_cmp file1 out1
  '
}

# should work offline
#test_dag_git

# should work online
test_launch_ipfs_daemon
test_dag_git
test_kill_ipfs_daemon

test_done
