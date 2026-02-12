#!/bin/sh

echo "Updating dolt config for tests:"
dolt config --global --add metrics.disabled true
dolt config --global --add metrics.host localhost
dolt config --global --add user.name orm-test-runner
dolt config --global --add user.email orm-test-runner@liquidata.co

echo "Running orm-tests:"
bats /orm-tests/orm-tests.bats
