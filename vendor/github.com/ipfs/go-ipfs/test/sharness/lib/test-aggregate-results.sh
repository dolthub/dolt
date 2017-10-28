#!/bin/sh
#
# Script to aggregate results using Sharness
#
# Copyright (c) 2014 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#

SHARNESS_AGGREGATE="lib/sharness/aggregate-results.sh"

test -f "$SHARNESS_AGGREGATE" || {
  echo >&2 "Cannot find: $SHARNESS_AGGREGATE"
  echo >&2 "Please check Sharness installation."
  exit 1
}

ls test-results/t*-*.sh.*.counts | "$SHARNESS_AGGREGATE"
