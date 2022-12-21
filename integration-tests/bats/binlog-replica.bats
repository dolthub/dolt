#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

setup() {
    setup_common
}

teardown() {
    stop_mysql_server
    stop_sql_server

    echo "BATS_TMPDIR: $BATS_TMPDIR"
    rm -rf "$BATS_TMPDIR/dolt"
    rm -rf "$BATS_TMPDIR/mysql"
    rm -rf "$BATS_TMPDIR/mysql_data"

#    assert_feature_version
#    teardown_common
}

start_mysql_server() {
    MYSQL_PORT=$( definePORT )

    # Create a fresh MySQL server for the primary
    cd $BATS_TMPDIR
    mkdir mysql
    cd mysql
    mysqld \
      --initialize-insecure \
      --user=root \
      --datadir=$PWD/mysql_data

    mysqld --datadir=$PWD/mysql_data \
      --default-authentication-plugin=mysql_native_password \
      --gtid-mode=ON --enforce-gtid-consistency=ON \
      --port=$MYSQL_PORT --server-id=11223344 \
      --socket=mysql-$MYSQL_PORT.sock  \
      --binlog-checksum=NONE \
      --general_log_file=$PWD/general_log \
      --log-bin=$PWD/log_bin \
      --slow_query_log_file=$PWD/slow_query_log \
      --log-error=$PWD/log_error \
      --pid-file=$PWD/pid-$MYSQL_PORT.pid &

    # TODO: Debug why this doesn't work
    #DEFAULT_DB="information_schema"
    #wait_for_connection $MYSQL_PORT 5000
    sleep 3

    # Create the initial database on the MySQL server
    # TODO: Do we need a --use-db param here? otherwise it tries the current working directory (which should be "mysql"?)
    dolt sql-client -P $MYSQL_PORT --use-db information_schema -u root -q "create database db01;"
}

stop_mysql_server() {
  mysqladmin shutdown --port=$MYSQL_PORT --protocol=TCP
}

@test "binlog-replica: basic case" {
  start_mysql_server
  start_sql_server
  DOLT_PORT=$PORT

  dolt sql-client -P $DOLT_PORT --use-db information_schema -u dolt -q "create database db01;"

  # Configure replication
  dolt sql-client -P $DOLT_PORT -u dolt --use-db db01 -q "
  change replication source to SOURCE_HOST='localhost', SOURCE_USER='root', SOURCE_PASSWORD='', SOURCE_PORT=$MYSQL_PORT; "

  # Start replication
  dolt sql-client -P $DOLT_PORT -u dolt --use-db db01 -q "start replica;"

  # Make changes on the source
  dolt sql-client -P $MYSQL_PORT -u root --use-db db01 -q "create table t (pk int primary key);"
  # TODO: Create a table with all types

  # Test that the change replicated
  echo "testing change on dolt replica..."
  run dolt sql-client -P $DOLT_PORT -u dolt --result-format=csv --use-db db01 -q "show create table t"
  [ $status -eq 0 ]
  singleline_output=$(convert_multiline_output output[@])
  [[ $singleline_output =~ "CREATE TABLE t ( pk int NOT NULL, PRIMARY KEY (pk) )" ]] || false

  # Show replication status
  run dolt sql-client -P $DOLT_PORT -u dolt --result-format=csv --use-db db01 -q "show replica status;"
  [ $status -eq 0 ]
  [[ $output =~ "localhost,root,$MYSQL_PORT" ]] || false

  # Stop replication
  # TODO: Debug why this hangs...
  # dolt sql-client -P $DOLT_PORT -u dolt --use-db db01 -q "stop replica;"
}

# Removes newlines and backtick quotes from the specified input and collapses
# multiple spaces into a single space.
convert_multiline_output() {
  input=${!1}
  singleline_output="${input[*]//$'\n'/ }"
  singleline_output="${singleline_output[*]//\`/}"
  singleline_output=$(echo "$singleline_output" | tr -s ' ')

  echo $singleline_output
}