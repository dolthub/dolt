#!/usr/bin/env bats

setup() {
  REPO_NAME="dolt_repo_$$"
  mkdir $REPO_NAME
  cd $REPO_NAME

  dolt init

  let PORT="$$ % (65536-1024) + 1024"
  USER="dolt"
  dolt sql-server --host 0.0.0.0 --port=$PORT --user=$USER --loglevel=trace &
  SERVER_PID=$!

  # Give the server a chance to start
  sleep 1

  export MYSQL_PWD=""
}

teardown() {
  cd ..
  kill $SERVER_PID
  rm -rf $REPO_NAME
}

@test "peewee client test" {
  skip "Dolt does not pass all tests yet"

  # peewee tests require the test database to be named peewee_test
  mysql -h 0.0.0.0 -u $USER --port=$PORT -e "CREATE DATABASE IF NOT EXISTS peewee_test"

  # Setup and install pewee
  git clone https://github.com/coleifer/peewee
  cd peewee

  python3 setup.py install
  python3 runtests.py -e mysql --mysql-host=0.0.0.0 --mysql-port=$PORT --mysql-user=$USER --mysql-password=""
}
