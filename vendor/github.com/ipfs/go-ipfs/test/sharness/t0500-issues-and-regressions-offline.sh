#!/bin/sh

test_description="Tests for various fixed issues and regressions."

. lib/test-lib.sh

# Tests go here

test_expect_success "ipfs init with occupied input works - #2748" '
  export IPFS_PATH="ipfs_path"
  echo "" | go-timeout 10 ipfs init &&
  rm -rf ipfs_path
'
test_init_ipfs

test_expect_success "ipfs cat --help succeeds when input remains open" '
  yes | go-timeout 1 ipfs cat --help
'

test_expect_success "ipfs pin ls --help succeeds when input remains open" '
  yes | go-timeout 1 ipfs pin ls --help
'

test_expect_success "ipfs add on 1MB from stdin woks" '
  random 1048576 42 | ipfs add -q > 1MB.hash
'

test_expect_success "'ipfs refs -r -e \$(cat 1MB.hash)' succeeds" '
  ipfs refs -r -e $(cat 1MB.hash) > refs-e.out
'

test_expect_success "output of 'ipfs refs -e' links to separate blocks" '
  grep "$(cat 1MB.hash) ->" refs-e.out
'

test_expect_success "output of 'ipfs refs -e' contains all first level links" '
  grep "$(cat 1MB.hash) ->" refs-e.out | sed -e '\''s/.* -> //'\'' | sort > refs-s.out &&
  ipfs refs "$(cat 1MB.hash)" | sort > refs-one.out &&
  test_cmp refs-s.out refs-one.out
'

test_done
