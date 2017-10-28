#!/bin/sh

test_description="Test sharness test indent"

. lib/test-lib.sh

for file in $(find .. -name 't*.sh' -type f); do
  test_expect_success "indent in $file is not using tabs" '
    test_must_fail grep -P "^ *\t" $file
  '
done

test_done
