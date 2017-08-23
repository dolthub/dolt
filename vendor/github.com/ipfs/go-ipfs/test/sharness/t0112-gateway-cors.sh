#!/bin/sh
#
# Copyright (c) 2016 Marcin Rataj
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test HTTP Gateway CORS Support"

test_config_ipfs_cors_headers() {
    ipfs config --json API.HTTPHeaders.Access-Control-Allow-Origin '["*"]'
    ipfs config --json API.HTTPHeaders.Access-Control-Allow-Methods '["PUT", "GET", "POST"]'
    ipfs config --json API.HTTPHeaders.Access-Control-Allow-Headers '["X-Requested-With"]'
}

. lib/test-lib.sh

test_init_ipfs
test_config_ipfs_cors_headers
test_launch_ipfs_daemon

gwport=$GWAY_PORT
apiport=$API_PORT
thash='QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn'

# Gateway

# HTTP GET Request
test_expect_success "GET to Gateway succeeds" '
    curl -svX GET "http://127.0.0.1:$gwport/ipfs/$thash" 2>curl_output
'

cat curl_output
# GET Response from Gateway should contain CORS headers
test_expect_success "GET response for Gateway resource looks good" '
    grep "Access-Control-Allow-Origin:" curl_output | grep "\*"  &&
    grep "Access-Control-Allow-Methods:" curl_output | grep " GET\b" &&
    grep "Access-Control-Allow-Headers:" curl_output
'

# HTTP OPTIONS Request
test_expect_success "OPTIONS to Gateway succeeds" '
    curl -svX OPTIONS "http://127.0.0.1:$gwport/ipfs/$thash" 2>curl_output
'
# OPTION Response from Gateway should contain CORS headers
test_expect_success "OPTIONS response for Gateway resource looks good" '
    grep "Access-Control-Allow-Origin:" curl_output | grep "\*"  &&
    grep "Access-Control-Allow-Methods:" curl_output | grep " GET\b" &&
    grep "Access-Control-Allow-Headers:" curl_output
'

# Read-Only API (at the Gateway Port)

# HTTP GET Request
test_expect_success "GET to API succeeds" '
    curl -svX GET "http://127.0.0.1:$gwport/api/v0/cat?arg=$thash" 2>curl_output
'
# GET Response from the API should NOT contain CORS headers
# Blacklisting: https://git.io/vzaj2
# Rationale: https://git.io/vzajX
test_expect_success "OPTIONS response for API looks good" '
    grep -q "Access-Control-Allow-" curl_output && false || true
'

# HTTP OPTIONS Request
test_expect_success "OPTIONS to API succeeds" '
    curl -svX OPTIONS "http://127.0.0.1:$gwport/api/v0/cat?arg=$thash" 2>curl_output
'
# OPTIONS Response from the API should NOT contain CORS headers
test_expect_success "OPTIONS response for API looks good" '
    grep -q "Access-Control-Allow-" curl_output && false || true
'

test_kill_ipfs_daemon

test_done
