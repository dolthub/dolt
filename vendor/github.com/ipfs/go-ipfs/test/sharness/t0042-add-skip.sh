#!/bin/sh
#
# Copyright (c) 2014 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test add and cat commands"

. lib/test-lib.sh

test_add_skip() {

	test_expect_success "'ipfs add -r' with hidden file succeeds" '
		mkdir -p mountdir/planets/.asteroids &&
		echo "Hello Mars" >mountdir/planets/mars.txt &&
		echo "Hello Venus" >mountdir/planets/venus.txt &&
		echo "Hello Pluto" >mountdir/planets/.pluto.txt &&
		echo "Hello Charon" >mountdir/planets/.charon.txt &&
		echo "Hello Ceres" >mountdir/planets/.asteroids/ceres.txt &&
		echo "Hello Pallas" >mountdir/planets/.asteroids/pallas.txt &&
		ipfs add -r mountdir/planets >actual
	'

	test_expect_success "'ipfs add -r' did not include . files" '
		cat >expected <<-\EOF &&
			added QmZy3khu7qf696i5HtkgL2NotsCZ8wzvNZJ1eUdA5n8KaV planets/mars.txt
			added QmQnv4m3Q5512zgVtpbJ9z85osQrzZzGRn934AGh6iVEXz planets/venus.txt
			added QmR8nD1Vzk5twWVC6oShTHvv7mMYkVh6dApCByBJyV2oj3 planets
		EOF
		test_cmp expected actual
	'

	test_expect_success "'ipfs add -r --hidden' succeeds" '
		ipfs add -r --hidden mountdir/planets >actual
	'

	test_expect_success "'ipfs add -r --hidden' did include . files" '
		cat >expected <<-\EOF &&
			added QmcAREBcjgnUpKfyFmUGnfajA1NQS5ydqRp7WfqZ6JF8Dx planets/.asteroids/ceres.txt
			added QmZ5eaLybJ5GUZBNwy24AA9EEDTDpA4B8qXnuN3cGxu2uF planets/.asteroids/pallas.txt
			added QmaowqjedBkUrMUXgzt9c2ZnAJncM9jpJtkFfgdFstGr5a planets/.charon.txt
			added QmU4zFD5eJtRBsWC63AvpozM9Atiadg9kPVTuTrnCYJiNF planets/.pluto.txt
			added QmZy3khu7qf696i5HtkgL2NotsCZ8wzvNZJ1eUdA5n8KaV planets/mars.txt
			added QmQnv4m3Q5512zgVtpbJ9z85osQrzZzGRn934AGh6iVEXz planets/venus.txt
			added Qmf6rbs5GF85anDuoxpSAdtuZPM9D2Yt3HngzjUVSQ7kDV planets/.asteroids
			added QmetajtFdmzhWYodAsZoVZSiqpeJDAiaw2NwbM3xcWcpDj planets
		EOF
		test_cmp expected actual
	'

	test_expect_success "'ipfs add' includes hidden files given explicitly even without --hidden" '
    mkdir -p mountdir/dotfiles &&
    echo "set nocompatible" > mountdir/dotfiles/.vimrc
		cat >expected <<-\EOF &&
added QmT4uMRDCN7EMpFeqwvKkboszbqeW1kWVGrBxBuCGqZcQc .vimrc
		EOF
		ipfs add mountdir/dotfiles/.vimrc >actual
    cat actual
		test_cmp expected actual
	'

}

# should work offline
test_init_ipfs
test_add_skip

# should work online
test_launch_ipfs_daemon
test_add_skip
test_kill_ipfs_daemon

test_done
