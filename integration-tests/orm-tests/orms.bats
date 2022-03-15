#!/usr/bin/env bats

setup() {
    let PORT="$$ % (65536-1024) + 1024"
    USER="dolt"
}

setupDoltRepoWithName() {
  mdkir $1 && cd $1
  dolt init
  dolt sql-server --host 0.0.0.0 --port=$PORT --user=$USER --loglevel=trace &
  SERVER_PID=$!
}

teardown() {

}

@test "peewee client test" {
  # Setup and install pewee
  git clone https://github.com/coleifer/peewee

  mkdir peewee_test && cd peewee_test
  dolt init
  dolt sql-server --host 0.0.0.0 --port=$PORT --user=$USER --loglevel=trace &
  SERVER_PID=$!

  cd peewee
  python runtests.py -e mysql --mysql-host=0.0.0.0 --mysql-port=$PORT --mysql-user=$USER --mysql-password=""
}