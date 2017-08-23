#!/usr/bin/env bash
# the CircleCI build line got a bit out of hands
# thus we have sparate file for it

curl -s https://codecov.io/bash > codecov

case $CIRCLE_NODE_INDEX in
	0) make -j 1 coverage/unit_tests.coverprofile &&
		bash codecov -cF unittests -X search -f coverage/unit_tests.coverprofile
	;;
	1) make -j 1 coverage/sharness_tests.coverprofile &&
		bash codecov -cF sharness -X search -f coverage/sharness_tests.coverprofile
	;;
esac
