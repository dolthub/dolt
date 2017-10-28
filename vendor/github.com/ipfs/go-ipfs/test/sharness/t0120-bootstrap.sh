#!/bin/sh
#
# Copyright (c) 2014 Jeromy Johnson
# MIT Licensed; see the LICENSE file in this repository.
#

# changing the bootstrap peers will require changing it in two places :)
BP1="/dnsaddr/bootstrap.libp2p.io/ipfs/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"
BP2="/dnsaddr/bootstrap.libp2p.io/ipfs/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa"
BP3="/dnsaddr/bootstrap.libp2p.io/ipfs/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb"
BP4="/dnsaddr/bootstrap.libp2p.io/ipfs/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt"
BP5="/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
BP6="/ip4/104.236.179.241/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM"
BP7="/ip4/104.236.76.40/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64"
BP8="/ip4/128.199.219.111/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu"
BP9="/ip4/178.62.158.247/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd"
BP10="/ip6/2400:6180:0:d0::151:6001/tcp/4001/ipfs/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu"
BP11="/ip6/2604:a880:1:20::203:d001/tcp/4001/ipfs/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM"
BP12="/ip6/2604:a880:800:10::4a:5001/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64"
BP13="/ip6/2a03:b0c0:0:1010::23:1001/tcp/4001/ipfs/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd"

test_description="Test ipfs repo operations"

. lib/test-lib.sh

test_init_ipfs

# we use a function so that we can run it both offline + online
test_bootstrap_list_cmd() {
  printf "" >list_expected
  for BP in "$@"
  do
    echo "$BP" >>list_expected
  done

  test_expect_success "'ipfs bootstrap' succeeds" '
    ipfs bootstrap >list_actual
  '

  test_expect_success "'ipfs bootstrap' output looks good" '
    test_cmp list_expected list_actual
  '

  test_expect_success "'ipfs bootstrap list' succeeds" '
    ipfs bootstrap list >list2_actual
  '

  test_expect_success "'ipfs bootstrap list' output looks good" '
    test_cmp list_expected list2_actual
  '
}

# we use a function so that we can run it both offline + online
test_bootstrap_cmd() {

  # remove all peers just in case.
  # if this fails, the first listing may not be empty
  ipfs bootstrap rm --all

  test_bootstrap_list_cmd

  test_expect_success "'ipfs bootstrap add' succeeds" '
    ipfs bootstrap add "$BP1" "$BP2" "$BP3" >add_actual
  '

  test_expect_success "'ipfs bootstrap add' output looks good" '
    echo $BP1 >add_expected &&
    echo $BP2 >>add_expected &&
    echo $BP3 >>add_expected &&
    test_cmp add_expected add_actual
  '

  test_bootstrap_list_cmd $BP1 $BP2 $BP3

  test_expect_success "'ipfs bootstrap rm' succeeds" '
    ipfs bootstrap rm "$BP1" "$BP3" >rm_actual
  '

  test_expect_success "'ipfs bootstrap rm' output looks good" '
    echo $BP1 >rm_expected &&
    echo $BP3 >>rm_expected &&
    test_cmp rm_expected rm_actual
  '

  test_bootstrap_list_cmd $BP2

  test_expect_success "'ipfs bootstrap add --default' succeeds" '
    ipfs bootstrap add --default >add2_actual
  '

  test_expect_success "'ipfs bootstrap add --default' output has default BP" '
    echo $BP1 >add2_expected &&
    echo $BP2 >>add2_expected &&
    echo $BP3 >>add2_expected &&
    echo $BP4 >>add2_expected &&
    echo $BP5 >>add2_expected &&
    echo $BP6 >>add2_expected &&
    echo $BP7 >>add2_expected &&
    echo $BP8 >>add2_expected &&
    echo $BP9 >>add2_expected &&
    echo $BP10 >>add2_expected &&
    echo $BP11 >>add2_expected &&
    echo $BP12 >>add2_expected &&
    echo $BP13 >>add2_expected &&
    test_cmp add2_expected add2_actual
  '

  test_bootstrap_list_cmd $BP1 $BP2 $BP3 $BP4 $BP5 $BP6 $BP7 $BP8 $BP9 $BP10 $BP11 $BP12 $BP13 $BP14 $BP15 $BP16 $BP17

  test_expect_success "'ipfs bootstrap rm --all' succeeds" '
    ipfs bootstrap rm --all >rm2_actual
  '

  test_expect_success "'ipfs bootstrap rm' output looks good" '
    echo $BP1 >rm2_expected &&
    echo $BP2 >>rm2_expected &&
    echo $BP3 >>rm2_expected &&
    echo $BP4 >>rm2_expected &&
    echo $BP5 >>rm2_expected &&
    echo $BP6 >>rm2_expected &&
    echo $BP7 >>rm2_expected &&
    echo $BP8 >>rm2_expected &&
    echo $BP9 >>rm2_expected &&
    echo $BP10 >>rm2_expected &&
    echo $BP11 >>rm2_expected &&
    echo $BP12 >>rm2_expected &&
    echo $BP13 >>rm2_expected &&
    test_cmp rm2_expected rm2_actual
  '

  test_bootstrap_list_cmd

  test_expect_success "'ipfs bootstrap add' accepts args from stdin" '
  echo $BP1 > bpeers &&
  echo $BP2 >> bpeers &&
  echo $BP3 >> bpeers &&
  echo $BP4 >> bpeers &&
  cat bpeers | ipfs bootstrap add > add_stdin_actual
  '

  test_expect_success "output looks good" '
  test_cmp add_stdin_actual bpeers
  '

  test_bootstrap_list_cmd $BP1 $BP2 $BP3 $BP4

  test_expect_success "'ipfs bootstrap rm' accepts args from stdin" '
  cat bpeers | ipfs bootstrap rm > rm_stdin_actual
  '

  test_expect_success "output looks good" '
  test_cmp rm_stdin_actual bpeers
  '

  test_bootstrap_list_cmd
}

# should work offline
test_bootstrap_cmd

# should work online
test_launch_ipfs_daemon
test_bootstrap_cmd
test_kill_ipfs_daemon


test_done
