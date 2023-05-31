load helper/query-server-common

setup_remote_server() {
  script_name=$(basename $BASH_SOURCE)
  if [ "$START_SQL_SERVER" = "true" ];
  then
    if [[ "$SKIP_SERVER_TESTS" =~ "$script_name" ]];
    then
      skip
    else
      USER=root
      start_sql_server
    fi
  fi
}

teardown_remote_server() {
  if [ "$START_SQL_SERVER" = "true" ];
  then
    stop_sql_server
  fi
}