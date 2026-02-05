#!/usr/bin/env bats
load helper/common

setup_file() {
    export BATS_TEST_RETRIES=5
}

setup() {
  setup_no_dolt_init
  REPO_NAME="$(basename "$PWD")"
  # TODO(elianddb): flaky dolt init and dolt sql-server lock race
  dolt init && flock -w 30 .dolt/noms/LOCK true

  PORT=$(definePORT)
  start_sql_server_with_args_no_port --host 0.0.0.0 --port="$PORT" --loglevel=debug

  export DB_HOST="127.0.0.1"
  export DB_PORT="$PORT"
  export DB_USER="root"
  export DB_PASSWORD=""
  export DB_NAME="$REPO_NAME"
  export_DB_URL

  export MYSQL_HOST="$DB_HOST"
  export MYSQL_TCP_PORT="$DB_PORT"
  export MYSQL_PWD="$DB_PASSWORD"
}

export_DB_URL() {
    export DB_URL="mysql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
}

teardown() {
  stop_sql_server 1
  teardown_common
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
  cd "$BATS_TEST_DIRNAME"/prisma
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
  cd $BATS_TEST_DIRNAME/typeorm
  npm install
  npm start
}

# MikroORM is an ORM for Node/TypeScript applications. This is a simple smoke test to make sure
# Dolt can support the most basic MikroORM operations.
@test "MikroORM smoke test" {
  cd $BATS_TEST_DIRNAME/mikro-orm
  npm install
  npm start
}

# Hibernate is an ORM for Java applications using JDBC driver. This is a simple smoke test to make sure
# Dolt can support the most basic Hibernate operations.
@test "Hibernate smoke test" {
  # need to create tables for it before running the test
  mysql -D "$DB_NAME" -e "create table STUDENT (id INT NOT NULL auto_increment PRIMARY KEY, first_name VARCHAR(30) NOT NULL, last_name VARCHAR(30) NOT NULL, section VARCHAR(30) NOT NULL);"

  cd $BATS_TEST_DIRNAME/hibernate/DoltHibernateSmokeTest
  mvn clean install
  mvn clean package
  mvn exec:java
}

# Prisma is an ORM for Node/TypeScript applications. This test reproduces a migration run against a branch-qualified connection string.
@test "Prisma ORM migration respects branch in connection string" {
  dolt branch newbranch

  export DB_NAME="${REPO_NAME}@newbranch"
  export_DB_URL

  cd "$BATS_TEST_DIRNAME"/prisma/branch
  run npm install
  log_status_eq 0

  run npx prisma migrate dev --name init
  log_status_eq 0

  run mysql -D "$DB_NAME" -e "show tables like 'employees';"
  log_status_eq 0
  [[ "$output" =~ "employees" ]] || false
}
