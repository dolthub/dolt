# Test framework for go-ipfs
#
# Copyright (c) 2014 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#
# We are using sharness (https://github.com/mlafeldt/sharness)
# which was extracted from the Git test framework.

# use the ipfs tool to test against

# add current directory to path, for ipfs tool.
if test "$MAKE_SKIP_PATH" != "1"; then
	BIN=$(cd .. && echo `pwd`/bin)
	BIN2=$(cd ../.. && echo `pwd`/cmd/ipfs)
	PATH=${BIN2}:${BIN}:${PATH}

	# assert the `ipfs` we're using is the right one.
	if test `which ipfs` != ${BIN2}/ipfs; then
		echo >&2 "Cannot find the tests' local ipfs tool."
		echo >&2 "Please check test and ipfs tool installation."
		exit 1
	fi
fi

# set sharness verbosity. we set the env var directly as
# it's too late to pass in --verbose, and --verbose is harder
# to pass through in some cases.
test "$TEST_VERBOSE" = 1 && verbose=t
# source the common hashes first.
. lib/test-lib-hashes.sh


SHARNESS_LIB="lib/sharness/sharness.sh"

. "$SHARNESS_LIB" || {
	echo >&2 "Cannot source: $SHARNESS_LIB"
	echo >&2 "Please check Sharness installation."
	exit 1
}

# Please put go-ipfs specific shell functions below

TEST_OS="$(uname -s | tr '[a-z]' '[A-Z]')"

# grab + output options
test "$TEST_NO_FUSE" != 1 && test_set_prereq FUSE
test "$TEST_EXPENSIVE" = 1 && test_set_prereq EXPENSIVE
test "$TEST_NO_DOCKER" != 1 && type docker >/dev/null 2>&1 && test_set_prereq DOCKER
test "$TEST_NO_PLUGIN" != 1 && test "$TEST_OS" = "LINUX" && test_set_prereq PLUGIN

# Set a prereq as error messages are often different on Windows/Cygwin
expr "$TEST_OS" : "CYGWIN_NT" >/dev/null || test_set_prereq STD_ERR_MSG

if test "$TEST_VERBOSE" = 1; then
	echo '# TEST_VERBOSE='"$TEST_VERBOSE"
	echo '# TEST_NO_FUSE='"$TEST_NO_FUSE"
	echo '# TEST_NO_PLUGIN='"$TEST_NO_PLUGIN"
	echo '# TEST_EXPENSIVE='"$TEST_EXPENSIVE"
	echo '# TEST_OS='"$TEST_OS"
fi

# source our generic test lib
. ../../ipfs-test-lib.sh

# source iptb lib
. ../lib/iptb-lib.sh

test_cmp_repeat_10_sec() {
	for i in $(test_seq 1 100)
	do
		test_cmp "$1" "$2" >/dev/null && return
		go-sleep 100ms
	done
	test_cmp "$1" "$2"
}

test_run_repeat_60_sec() {
	for i in $(test_seq 1 600)
	do
		(test_eval_ "$1") && return
		go-sleep 100ms
	done
	return 1 # failed
}

test_wait_output_n_lines_60_sec() {
	for i in $(test_seq 1 600)
	do
		test $(cat "$1" | wc -l | tr -d " ") -ge $2 && return
		go-sleep 100ms
	done
	actual=$(cat "$1" | wc -l | tr -d " ")
	test_fsh "expected $2 lines of output. got $actual"
}

test_wait_open_tcp_port_10_sec() {
	for i in $(test_seq 1 100)
	do
		# this is not a perfect check, but it's portable.
		# cant count on ss. not installed everywhere.
		# cant count on netstat using : or . as port delim. differ across platforms.
		echo $(netstat -aln | egrep "^tcp.*LISTEN" | egrep "[.:]$1" | wc -l) -gt 0
		if [ $(netstat -aln | egrep "^tcp.*LISTEN" | egrep "[.:]$1" | wc -l) -gt 0 ]; then
			return 0
		fi
		go-sleep 100ms
	done
	return 1
}


# test_config_set helps us make sure _we really did set_ a config value.
# it sets it and then tests it. This became elaborate because ipfs config
# was setting really weird things and am not sure why.
test_config_set() {

	# grab flags (like --bool in "ipfs config --bool")
	test_cfg_flags="" # unset in case.
	test "$#" = 3 && { test_cfg_flags=$1; shift; }

	test_cfg_key=$1
	test_cfg_val=$2

	# when verbose, tell the user what config values are being set
	test_cfg_cmd="ipfs config $test_cfg_flags \"$test_cfg_key\" \"$test_cfg_val\""
	test "$TEST_VERBOSE" = 1 && echo "$test_cfg_cmd"

	# ok try setting the config key/val pair.
	ipfs config $test_cfg_flags "$test_cfg_key" "$test_cfg_val"
	echo "$test_cfg_val" >cfg_set_expected
	ipfs config "$test_cfg_key" >cfg_set_actual
	test_cmp cfg_set_expected cfg_set_actual
}

test_init_ipfs() {


	# we set the Addresses.API config variable.
	# the cli client knows to use it, so only need to set.
	# todo: in the future, use env?

	test_expect_success "ipfs init succeeds" '
		export IPFS_PATH="$(pwd)/.ipfs" &&
		ipfs init --profile=test -b=1024 > /dev/null
	'

	test_expect_success "prepare config -- mounting" '
		mkdir mountdir ipfs ipns &&
		test_config_set Mounts.IPFS "$(pwd)/ipfs" &&
		test_config_set Mounts.IPNS "$(pwd)/ipns" ||
		test_fsh cat "\"$IPFS_PATH/config\""
	'

}

test_config_ipfs_gateway_writable() {
	test_expect_success "prepare config -- gateway writable" '
		test_config_set --bool Gateway.Writable true ||
		test_fsh cat "\"$IPFS_PATH/config\""
	'
}

test_wait_for_file() {
	loops=$1
	delay=$2
	file=$3
	fwaitc=0
	while ! test -f "$file"
	do
		if test $fwaitc -ge $loops
		then
			echo "Error: timed out waiting for file: $file"
			return 1
		fi

		go-sleep $delay
		fwaitc=`expr $fwaitc + 1`
	done
}

test_set_address_vars() {
	daemon_output="$1"

	test_expect_success "set up address variables" '
		API_MADDR=$(cat "$IPFS_PATH/api") &&
		API_ADDR=$(convert_tcp_maddr $API_MADDR) &&
		API_PORT=$(port_from_maddr $API_MADDR) &&

		GWAY_MADDR=$(sed -n "s/^Gateway (.*) server listening on //p" "$daemon_output") &&
		GWAY_ADDR=$(convert_tcp_maddr $GWAY_MADDR) &&
		GWAY_PORT=$(port_from_maddr $GWAY_MADDR)
	'

	if ipfs swarm addrs local >/dev/null 2>&1; then
		test_expect_success "set swarm address vars" '
		ipfs swarm addrs local > addrs_out &&
			SWARM_MADDR=$(grep "127.0.0.1" addrs_out) &&
			SWARM_PORT=$(port_from_maddr $SWARM_MADDR)
		'
	fi
}

test_launch_ipfs_daemon() {

	args="$@"

	test "$TEST_ULIMIT_PRESET" != 1 && ulimit -n 2048

	test_expect_success "'ipfs daemon' succeeds" '
		ipfs daemon $args >actual_daemon 2>daemon_err &
	'

	# wait for api file to show up
	test_expect_success "api file shows up" '
		test_wait_for_file 20 100ms "$IPFS_PATH/api"
	'

	test_set_address_vars actual_daemon

	# we say the daemon is ready when the API server is ready.
	test_expect_success "'ipfs daemon' is ready" '
		IPFS_PID=$! &&
		pollEndpoint -ep=/version -host=$API_MADDR -v -tout=1s -tries=60 2>poll_apierr > poll_apiout ||
		test_fsh cat actual_daemon || test_fsh cat daemon_err || test_fsh cat poll_apierr || test_fsh cat poll_apiout
	'
}

do_umount() {
    if [ "$(uname -s)" = "Linux" ]; then
	fusermount -u "$1"
    else
	umount "$1"
    fi
}

test_mount_ipfs() {

	# make sure stuff is unmounted first.
	test_expect_success FUSE "'ipfs mount' succeeds" '
		do_umount "$(pwd)/ipfs" || true &&
		do_umount "$(pwd)/ipns" || true &&
		ipfs mount >actual
	'

	test_expect_success FUSE "'ipfs mount' output looks good" '
		echo "IPFS mounted at: $(pwd)/ipfs" >expected &&
		echo "IPNS mounted at: $(pwd)/ipns" >>expected &&
		test_cmp expected actual
	'

}

test_launch_ipfs_daemon_and_mount() {

	test_init_ipfs
	test_launch_ipfs_daemon
	test_mount_ipfs

}

test_kill_repeat_10_sec() {
	# try to shut down once + wait for graceful exit
	kill $1
	for i in $(test_seq 1 100)
	do
		go-sleep 100ms
		! kill -0 $1 2>/dev/null && return
	done

	# if not, try once more, which will skip graceful exit
	kill $1
	go-sleep 1s
	! kill -0 $1 2>/dev/null && return

	# ok, no hope. kill it to prevent it messing with other tests
	kill -9 $1 2>/dev/null
	return 1
}

test_kill_ipfs_daemon() {

	test_expect_success "'ipfs daemon' is still running" '
		kill -0 $IPFS_PID
	'

	test_expect_success "'ipfs daemon' can be killed" '
		test_kill_repeat_10_sec $IPFS_PID
	'
}

test_curl_resp_http_code() {
	curl -I "$1" >curl_output || {
		echo "curl error with url: '$1'"
		echo "curl output was:"
		cat curl_output
		return 1
	}
	shift &&
	RESP=$(head -1 curl_output) &&
	while test "$#" -gt 0
	do
		expr "$RESP" : "$1" >/dev/null && return
		shift
	done
	echo "curl response didn't match!"
	echo "curl response was: '$RESP'"
	echo "curl output was:"
	cat curl_output
	return 1
}

test_must_be_empty() {
	if test -s "$1"
	then
		echo "'$1' is not empty, it contains:"
		cat "$1"
		return 1
	fi
}

test_should_contain() {
	test "$#" = 2 || error "bug in the test script: not 2 parameters to test_should_contain"
	if ! grep -q "$1" "$2"
	then
		echo "'$2' does not contain '$1', it contains:"
		cat "$2"
		return 1
	fi
}

test_str_contains() {
	find=$1
	shift
	echo "$@" | egrep "\b$find\b" >/dev/null
}

disk_usage() {
    # normalize du across systems
    case $(uname -s) in
        Linux)
            DU="du -sb"
			M=1
            ;;
        FreeBSD)
            DU="du -s -A -B 1"
			M=512
            ;;
        Darwin | DragonFly | *)
            DU="du -s"
			M=512
            ;;
    esac
	expr $($DU "$1" | awk "{print \$1}") "*" "$M"
}

# output a file's permission in human readable format
generic_stat() {
    # normalize stat across systems
    case $(uname -s) in
        Linux)
            _STAT="stat -c %A"
            ;;
        FreeBSD | Darwin | DragonFly)
            _STAT="stat -f %Sp"
            ;;
    esac
    $_STAT "$1" || echo "failed" # Avoid returning nothing.
}

test_check_peerid() {
	peeridlen=$(echo "$1" | tr -dC "[:alnum:]" | wc -c | tr -d " ") &&
	test "$peeridlen" = "46" || {
		echo "Bad peerid '$1' with len '$peeridlen'"
		return 1
	}
}

convert_tcp_maddr() {
	echo $1 | awk -F'/' '{ printf "%s:%s", $3, $5 }'
}

port_from_maddr() {
	echo $1 | awk -F'/' '{ print $NF }'
}
