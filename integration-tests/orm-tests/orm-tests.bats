#!/usr/bin/env bats

setup() {
  REPO_NAME="dolt_repo_$$"
  mkdir $REPO_NAME
  cd $REPO_NAME

  USER="dolt"
  dolt sql -q "CREATE USER dolt@'%'; GRANT ALL ON *.* TO dolt@'%';"
  dolt sql-server --host 0.0.0.0 --loglevel=trace &
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

# Peewee is a lightweight ORM library for Python applications. This test checks performs a basic
# smoke test to make sure Peewee can work with Dolt.
@test "Peewee ORM smoke test" {
  skip "Not implemented yet"
}

# Peewee is a lightweight ORM library for Python applications. This test checks out the Peewee test suite
# and runs it against Dolt.
@test "Peewee ORM test suite" {
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
# Dolt can support the most basic Prisma operations.
@test "Prisma ORM smoke test" {
  mysql --protocol TCP -u dolt -e "create database dolt;"

  cd $BATS_TEST_DIRNAME/prisma
  npm install
  npx -c "prisma migrate dev --name init"
}

# Prisma is an ORM for Node/TypeScript applications. This test checks out the Prisma test suite
# and runs it against Dolt.
@test "Prisma ORM test suite" {
  skip "Not implemented yet"

  # More info on running Prisma's tests here:
  # https://github.com/prisma/prisma/blob/main/TESTING.md
  #
  # The MySQL integration tests for Prisma
  # https://github.com/prisma/prisma/tree/main/packages/integration-tests/src/__tests__/integration/mysql
}

# TypeORM is an ORM for Node/TypeScript applications. This is a simple smoke test to make sure
# Dolt can support the most basic TypeORM operations.
@test "TypeORM smoke test" {
  mysql --protocol TCP -u dolt -e "create database dolt;"

  cd $BATS_TEST_DIRNAME/typeorm
  npm install
  npm start
}

# MikroORM is an ORM for Node/TypeScript applications. This is a simple smoke test to make sure
# Dolt can support the most basic MikroORM operations.
@test "MikroORM smoke test" {
  mysql --protocol TCP -u dolt -e "create database dolt;"

  cd $BATS_TEST_DIRNAME/mikro-orm
  npm install
  npm start
}

# Hibernate is an ORM for Java applications using JDBC driver. This is a simple smoke test to make sure
# Dolt can support the most basic Hibernate operations.
@test "Hibernate smoke test" {
  # need to create tables for it before running the test
  mysql --protocol TCP -u dolt -e "create database dolt; use dolt; create table STUDENT (id INT NOT NULL auto_increment PRIMARY KEY, first_name VARCHAR(30) NOT NULL, last_name VARCHAR(30) NOT NULL, section VARCHAR(30) NOT NULL);"

  cd $BATS_TEST_DIRNAME/hibernate/DoltHibernateSmokeTest
  mvn clean install
  mvn clean package
  mvn exec:java
}

# Turn this test on to prevent the container from exiting if you need to exec a shell into
# the container to debug failed tests.
#@test "Pause container for an hour to debug failures" {
#  sleep 3600
#}