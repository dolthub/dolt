#!/bin/sh
#
# Copyright (c) 2016 Tom O'Donnell
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test API add command"

. lib/test-lib.sh

test_init_ipfs

# Verify that that API add command returns size

test_launch_ipfs_daemon
test_expect_success "API Add response includes size field" '
  echo "hi" | curl -s -F file=@- "http://localhost:$API_PORT/api/v0/add" | grep "\"Size\": *\"11\""
'
test_kill_ipfs_daemon

test_done
