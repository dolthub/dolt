
setup_dolt_repo() {
    REPO_NAME="dolt_repo_$$"
    mkdir $REPO_NAME	
    cd $REPO_NAME

    dolt init

    dolt sql -q "CREATE TABLE mysqldump_table(pk int)"
    dolt sql -q "INSERT INTO mysqldump_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext)"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"

    PORT=$( definePORT )
    USER="dolt"
    dolt sql -q "CREATE USER dolt@'%'; GRANT ALL ON *.* TO dolt@'%';"
    dolt sql-server --host 0.0.0.0 --port=$PORT --loglevel=trace &
    SERVER_PID=$!
    # Give the server a chance to start
    sleep 1

    export MYSQL_PWD=""
}

teardown_dolt_repo() {
    kill $SERVER_PID
    rm -rf $REPO_NAME
}

definePORT() {
  getPORT=""
  for i in {0..9}
  do
    let getPORT="($$ + $i) % (65536-1024) + 1024"
    portinuse=$(lsof -i -P -n | grep LISTEN | grep $attemptedPORT | wc -l)
      if [ $portinuse -eq 0 ]
      then
        echo "$getPORT"
        break
      fi
  done
}