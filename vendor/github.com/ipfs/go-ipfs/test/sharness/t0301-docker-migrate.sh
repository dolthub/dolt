#!/bin/sh
#
# Copyright (c) 2017 Whyrusleeping
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test docker image migration"

. lib/test-lib.sh

# if in travis CI on OSX, docker is not available
if ! test_have_prereq DOCKER; then
	skip_all='skipping docker tests, docker not available'

	test_done
fi

TEST_TRASH_DIR=$(pwd)
TEST_SCRIPTS_DIR=$(dirname "$TEST_TRASH_DIR")
TEST_TESTS_DIR=$(dirname "$TEST_SCRIPTS_DIR")
APP_ROOT_DIR=$(dirname "$TEST_TESTS_DIR")

test_expect_success "docker image build succeeds" '
	docker_build "$TEST_TESTS_DIR/../Dockerfile.fast" "$APP_ROOT_DIR" >actual &&
	IMAGE_ID=$(tail -n1 actual | cut -d " " -f 3)
'

test_init_ipfs

test_expect_success "make repo be version 4" '
	echo 4 > "$IPFS_PATH/version"
'

test_expect_success "setup http response" '
	echo "HTTP/1.1 200 OK" > vers_resp &&
	echo "Content-Length: 7" >> vers_resp &&
	echo "" >> vers_resp &&
	echo "v1.1.1" >> vers_resp
'

pretend_server() {
	cat vers_resp | nc -l -i 1 -p 17233
}

test_expect_success "startup fake dists server" '
	pretend_server > dist_serv_out &
	echo $! > netcat_pid
'

test_expect_success "docker image runs" '
	DOC_ID=$(docker run -d -v "$IPFS_PATH":/data/ipfs --net=host -e IPFS_DIST_PATH="http://localhost:17233" "$IMAGE_ID" --migrate)
'

test_expect_success "docker container tries to pull migrations from netcat" '
	sleep 4 &&
	cat dist_serv_out
'

test_expect_success "see logs" '
	docker logs $DOC_ID
'

test_expect_success "stop docker container" '
	docker_stop "$DOC_ID"
'

test_expect_success "kill the net cat" '
	kill $(cat netcat_pid) || true
'

test_expect_success "correct version was requested" '
	grep "/fs-repo-migrations/v1.1.1/fs-repo-migrations_v1.1.1_linux-musl-amd64.tar.gz" dist_serv_out > /dev/null
'

test_done

