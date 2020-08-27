#!/bin/sh

results=$(bats mysql-client-tests.bats)
echo "::set-output name=results::$results"