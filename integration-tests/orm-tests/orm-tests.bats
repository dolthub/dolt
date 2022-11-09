#!/usr/bin/env bats

setup() {
  REPO_NAME="dolt_repo_$$"
  mkdir $REPO_NAME
  cd $REPO_NAME

  USER="dolt"
  dolt sql-server --host 0.0.0.0 --user=$USER --loglevel=trace &
  SERVER_PID=$!

  # Give the server a chance to start
  sleep 1

  export MYSQL_PWD=""
  cd ..
}

teardown() {
  cd ..
  kill $SERVER_PID
  rm -rf $REPO_NAME
  rm -f /tmp/mysql.sock
}

# Peewee is a lightweight ORM library for Python applications
@test "peewee ORM test suite" {
  skip "Dolt does not pass all tests yet"

  # peewee tests require the test database to be named peewee_test
  mysql -h 0.0.0.0 -u $USER -e "CREATE DATABASE IF NOT EXISTS peewee_test"

  # Setup and install pewee
  git clone https://github.com/coleifer/peewee
  cd peewee

  python3 setup.py install
  python3 runtests.py -e mysql --mysql-host=0.0.0.0 --mysql-user=$USER --mysql-password=""
}

# Prisma is an ORM for Node/TypeScript applications. This is a simple smoke test to make sure
# Dolt can support the most basic Prisma operation.
@test "prisma ORM smoke test" {
  mysql --protocol TCP -u dolt -e "create database obsidian;"

  cd prisma
  npm install
  npx -c "prisma migrate dev --name init"
}

@test "prisma ORM test suite" {
  skip "Not implemented yet"

  # More info on running Prisma's tests here:
  # https://github.com/prisma/prisma/blob/main/TESTING.md
  #
  # The MySQL integration tests for Prisma
  # https://github.com/prisma/prisma/tree/main/packages/integration-tests/src/__tests__/integration/mysql
}

# Turn this test on to prevent the container from exiting if you need to exec a shell into
# the container to debug failed tests.
#@test "Pause container for an hour to debug failures" {
#  sleep 3600
#}