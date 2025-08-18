#!/bin/sh

echo "Updating dolt config for tests:"
dolt config --global --add metrics.disabled true
dolt config --global --add metrics.host localhost
dolt config --global --add user.name mysql-test-runner
dolt config --global --add user.email mysql-test-runner@liquidata.co

dolt sql -q "SET @@GLOBAL.dolt_log_compact_schema = 1;"

echo "Running mysql-client-tests:"
bats /build/bin/bats/mysql-client-tests.bats

# We run mariadb-binlog integration in this suite same as with mysqldump in mysql-client-tests.bats.
# However, there's a bit more setup necessary to pipe the output from the dump in a mariadb client, so it's been
# separated into a separate bats.
echo "Running mariadb-binlog tests:"
bats /build/bin/bats/mariadb-binlog.bats
