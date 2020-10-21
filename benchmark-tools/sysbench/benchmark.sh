#!/bin/bash
set -e
set -o pipefail

if [ -n "$DOLT_COMMITTISH" ]; then
echo "Running sysbench tests $SYSBENCH_TESTS for test user $TEST_USERNAME"
python /python/sysbench_wrapper.py \
  --db-host="$DB_HOST" \
  --committish="$DOLT_COMMITTISH" \
  --tests="$SYSBENCH_TESTS" \
  --username="$TEST_USERNAME"
else
  echo "Running sysbench tests $SYSBENCH_TESTS for test user $TEST_USERNAME"
  python /python/sysbench_wrapper.py \
    --db-host="$DB_HOST" \
    --tests="$SYSBENCH_TESTS" \
    --username="$TEST_USERNAME"
fi
