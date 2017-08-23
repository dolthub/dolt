#!/bin/sh
#
# Copyright (c) 2014 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test add -w"

add_w_m='QmazHkwx6mPmmCEi1jR5YzjjQd1g5XzKfYQLzRAg7x5uUk'

add_w_1='added Qme987pqNBhZZXy4ckeXiR7zaRQwBabB7fTgHurW2yJfNu 4r93
added Qmf82PSsMpUHcrqxa69KG6Qp5yeK7K9BTizXgG3nvzWcNG '

add_w_12='added Qme987pqNBhZZXy4ckeXiR7zaRQwBabB7fTgHurW2yJfNu 4r93
added QmVb4ntSZZnT2J2zvCmXKMJc52cmZYH6AB37MzeYewnkjs 4u6ead
added QmZPASVB6EsADrLN8S2sak34zEHL8mx4TAVsPJU9cNnQQJ '

add_w_21='added Qme987pqNBhZZXy4ckeXiR7zaRQwBabB7fTgHurW2yJfNu 4r93
added QmVb4ntSZZnT2J2zvCmXKMJc52cmZYH6AB37MzeYewnkjs 4u6ead
added QmZPASVB6EsADrLN8S2sak34zEHL8mx4TAVsPJU9cNnQQJ '

add_w_d1='added QmPcaX84tDiTfzdTn8GQxexodgeWH6mHjSss5Zfr5ojssb _jo7/-s782qgs
added QmaVBqquUuXKjkyWHXaXfsaQUxAnsCKS95VRDHU8PzGA4K _jo7/15totauzkak-
added QmaAHFG8cmhW3WLjofx5siSp44VV25ETN6ThzrU8iAqpkR _jo7/galecuirrj4r
added QmeuSfhJNKwBESp1W9H8cfoMdBfW3AeHQDWXbNXQJYWp53 _jo7/mzo50r-1xidf5zx
added QmYC3u5jGWuyFwvTxtvLYm2K3SpWZ31tg3NjpVVvh9cJaJ _jo7/wzvsihy
added QmQkib3f9XNX5sj6WEahLUPFpheTcwSRJwUCSvjcv8b9by _jo7
added QmNQoesMj1qp8ApE51NbtTjFYksyzkezPD4cat7V2kzbKN '

add_w_d1_v1='added zb2rhjXyHbbgwgtAUwHtpBd8iXLgK22ZjVmaiJSMNmqBTpXS3 _jo7/-s782qgs
added zb2rhi6PQqQFbS4QsvrV8sL9ue1fvFoqtLVqogNPCZri8rquN _jo7/15totauzkak-
added zb2rhjQthC6LgnNZztpsF9LcfPxznh3cJnmzUx8dnSqNqJ8Yz _jo7/galecuirrj4r
added zb2rhYh9hgDw1DpaZfLUU5MkKNezPWjPTkgGQPiTyLpZYu3jn _jo7/mzo50r-1xidf5zx
added zb2rhZK5xwEUhY4uskfj4sn979aCH27cnqseVVznYDn7NFWtt _jo7/wzvsihy
added zdj7WfNC8EZchqskczxsgrVEqwLVpksQ9B5kopf391jVbCGwv _jo7
added zdj7Wn5jf686mfYE8gUKWzY7aTjp5eAQcecD8q4ZtqLJbDNxe '

add_w_d2='added Qme987pqNBhZZXy4ckeXiR7zaRQwBabB7fTgHurW2yJfNu 4r93
added QmU9Jqks8TPu4vFr6t7EKkAKQrSJuEujNj1AkzoCeTEDFJ gnz66h/1k0xpx34
added QmSLYZycXAufRw3ePMVH2brbtYWCcWsmksGLbHcT8ia9Ke gnz66h/9cwudvacx
added QmfYmpCCAMU9nLe7xbrYsHf5z2R2GxeQnsm4zavUhX9vq2 gnz66h/9ximv51cbo8
added QmWgEE4e2kfx3b8HZcBk5cLrfhoi8kTMQP2MipgPhykuV3 gnz66h/b54ygh6gs
added QmcLbqEqhREGednc6mrVtanee4WHKp5JnUfiwTTHCJwuDf gnz66h/lbl5
added QmPcaX84tDiTfzdTn8GQxexodgeWH6mHjSss5Zfr5ojssb _jo7/-s782qgs
added QmaVBqquUuXKjkyWHXaXfsaQUxAnsCKS95VRDHU8PzGA4K _jo7/15totauzkak-
added QmaAHFG8cmhW3WLjofx5siSp44VV25ETN6ThzrU8iAqpkR _jo7/galecuirrj4r
added QmeuSfhJNKwBESp1W9H8cfoMdBfW3AeHQDWXbNXQJYWp53 _jo7/mzo50r-1xidf5zx
added QmYC3u5jGWuyFwvTxtvLYm2K3SpWZ31tg3NjpVVvh9cJaJ _jo7/wzvsihy
added QmVaKAt2eVftNKFfKhiBV7Mu5HjCugffuLqWqobSSFgiA7 h3qpecj0
added QmQkib3f9XNX5sj6WEahLUPFpheTcwSRJwUCSvjcv8b9by _jo7
added QmVPwNy8pZegpsNmsjjZvdTQn4uCeuZgtzhgWhRSQWjK9x gnz66h
added QmTmc46fhKC8Liuh5soy1VotdnHcqLu3r6HpPGwDZCnqL1 '

add_w_r='QmcCksBMDuuyuyfAMMNzEAx6Z7jTrdRy9a23WpufAhG9ji'

. lib/test-lib.sh

test_add_w() {

  test_expect_success "go-random-files is installed" '
    type random-files
  '

  test_expect_success "random-files generates test files" '
    random-files --seed 7547632 --files 5 --dirs 2 --depth 3 m &&
    echo "$add_w_m" >expected &&
    ipfs add -q -r m | tail -n1 >actual &&
    test_sort_cmp expected actual
  '

  # test single file
  test_expect_success "ipfs add -w (single file) succeeds" '
    ipfs add -w m/4r93 >actual
  '

  test_expect_success "ipfs add -w (single file) is correct" '
    echo "$add_w_1" >expected &&
    test_sort_cmp expected actual
  '

  # test two files together
  test_expect_success "ipfs add -w (multiple) succeeds" '
    ipfs add -w m/4r93 m/4u6ead >actual
  '

  test_expect_success "ipfs add -w (multiple) is correct" '
    echo "$add_w_12" >expected  &&
    test_sort_cmp expected actual
  '

  test_expect_success "ipfs add -w (multiple) succeeds" '
    ipfs add -w m/4u6ead m/4r93 >actual
  '

  test_expect_success "ipfs add -w (multiple) orders" '
    echo "$add_w_21" >expected  &&
    test_sort_cmp expected actual
  '

  # test a directory
  test_expect_success "ipfs add -w -r (dir) succeeds" '
    ipfs add -r -w m/t_1wp-8a2/_jo7 >actual
  '

  test_expect_success "ipfs add -w -r (dir) is correct" '
    echo "$add_w_d1" >expected &&
    test_sort_cmp expected actual
  '

  # test files and directory
  test_expect_success "ipfs add -w -r <many> succeeds" '
    ipfs add -w -r m/t_1wp-8a2/h3qpecj0 \
      m/ha6f0x7su6/gnz66h m/t_1wp-8a2/_jo7 m/4r93 >actual
  '

  test_expect_success "ipfs add -w -r <many> is correct" '
    echo "$add_w_d2" >expected &&
    test_sort_cmp expected actual
  '

  # test -w -r m/* == -r m
  test_expect_success "ipfs add -w -r m/* == add -r m  succeeds" '
    ipfs add -q -w -r m/* | tail -n1 >actual
  '

  test_expect_success "ipfs add -w -r m/* == add -r m  is correct" '
    echo "$add_w_m" >expected &&
    test_sort_cmp expected actual
  '

  # test repeats together
  test_expect_success "ipfs add -w (repeats) succeeds" '
    ipfs add -q -w -r m/t_1wp-8a2/h3qpecj0 m/ha6f0x7su6/gnz66h \
      m/t_1wp-8a2/_jo7 m/4r93 m/t_1wp-8a2 m/t_1wp-8a2 m/4r93 \
      m/4r93 m/ha6f0x7su6/_rwujlf3qh_g08 \
      m/ha6f0x7su6/gnz66h/9cwudvacx | tail -n1 >actual
  '

  test_expect_success "ipfs add -w (repeats) is correct" '
    echo "$add_w_r" >expected  &&
    test_sort_cmp expected actual
  '

  test_expect_success "ipfs add -w -r (dir) --cid-version=1 succeeds" '
    ipfs add -r -w --cid-version=1 m/t_1wp-8a2/_jo7 >actual
  '

  test_expect_success "ipfs add -w -r (dir) --cid-version=1 is correct" '
    echo "$add_w_d1_v1" >expected &&
    test_sort_cmp expected actual
  '
}

test_init_ipfs

test_add_w

test_launch_ipfs_daemon

test_add_w

test_kill_ipfs_daemon

test_done
