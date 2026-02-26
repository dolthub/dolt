
setup_dolt_repo() {
    REPO_NAME="dolt_repo_$$"
    mkdir "$REPO_NAME"
    cd "$REPO_NAME" || exit

    dolt init

    dolt sql -q "CREATE TABLE mysqldump_table(pk int)"
    dolt sql -q "INSERT INTO mysqldump_table VALUES (1);"
    dolt sql -q "CREATE TABLE warehouse(warehouse_id int primary key, warehouse_name longtext)"
    dolt sql -q "INSERT into warehouse VALUES (1, 'UPS'), (2, 'TV'), (3, 'Table');"

    PORT=$( definePORT )
    USER="dolt"
    # Use an explicit auth method and password value so auth negotiation failures are actionable.
    dolt sql -q "CREATE USER dolt@'%' IDENTIFIED BY ''; GRANT ALL ON *.* TO dolt@'%';"
    dolt sql-server --host 0.0.0.0 --port="$PORT" --loglevel=trace &
    SERVER_PID=$!
    # Give the server a chance to start
    sleep 1
}

teardown_dolt_repo() {
    kill $SERVER_PID
    rm -rf $REPO_NAME
}

definePORT() {
  local portInUse
  local getPORT=""
  local i

  for i in {0..9}
  do
    ((getPORT = ($$ + i) % (65536 - 1024) + 1024))
    portInUse="$(lsof -i -P -n | grep LISTEN | grep -c "$getPORT")"
      if [[ "$portInUse" -eq 0 ]] || false
      then
        echo "$getPORT"
        break
      fi
  done
}