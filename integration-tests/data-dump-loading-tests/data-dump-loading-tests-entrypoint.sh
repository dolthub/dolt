#!/bin/sh

cd ./integration-tests/data-dump-loading-tests

echo "Updating dolt config for tests:"
dolt config --global --add metrics.disabled true
dolt config --global --add metrics.host localhost
dolt config --global --add user.name mysql-test-runner
dolt config --global --add user.email mysql-test-runner@liquidata.co

echo "Running data-dump-loading-tests:"

bats /data-dump-loading-tests/import-mysqldump.bats
res1=$(echo $status)

bats /data-dump-loading-tests/sakila-data-dump-load.bats
res2=$(echo $status)

# both the bats tests needs to pass to return 0
return $(($p1 + $p2))
