SKIP_SERVER_TESTS=(

)

setup_remote_server() {
  script_name = $(basename $BASH_SOURCE)
  if [ "$START_SQL_SERVER" = $true ];
  then
    if [ echo "$SKIP_SERVER_TESTS" | grep "$script_name" ];
    then
      skip
    else
      start_sql_server
    fi
  fi
}

teardown_remote_server() {
  if [ "$START_SQL_SERVER" = $true ];
  then
    stop_sql_server
  fi
}