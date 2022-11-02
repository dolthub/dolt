#!/bin/sh

cd ./integration-tests/data-dump-loading-tests

echo "Updating dolt config for tests:"
dolt config --global --add metrics.disabled true
dolt config --global --add metrics.host localhost
dolt config --global --add user.name mysql-test-runner
dolt config --global --add user.email mysql-test-runner@liquidata.co

echo "Running data-dump-loading-tests:"
bats /data-dump-loading-tests/import-mysqldump.bats
bats /data-dump-loading-tests/sakila-data-dump-load.bats
