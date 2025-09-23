#!/bin/sh

echo "Updating dolt config for tests:"
dolt config --global --add metrics.disabled true
dolt config --global --add metrics.host localhost
dolt config --global --add user.name mysql-test-runner
dolt config --global --add user.email mysql-test-runner@liquidata.co

dolt sql -q "SET @@GLOBAL.dolt_log_committer_only = 1;"

echo "Running mysql-client-tests:"
bats /mysql-client-tests/mysql-client-tests.bats
