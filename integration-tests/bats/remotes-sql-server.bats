#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/query-server-common.bash

make_repo() {
  mkdir "$1"
  cd "$1"
  dolt init
  cd ..
}

setup() {
    if [ "$SQL_ENGINE" = "remote-engine" ]; then
      skip "This test tests remote connections directly, SQL_ENGINE is not needed."
    fi
    setup_no_dolt_init
    make_repo repo1
    mkdir rem1

    cd repo1
    dolt remote add remote1 file://../rem1
    dolt push remote1 main

    cd ..
    dolt clone file://./rem1 repo2
    cd repo2

    dolt remote add remote1 file://../rem1
    cd ../repo1
    dolt sql -q "create table test (pk int primary key)"
    dolt add .
    dolt sql -q "insert into test values (0),(1),(2)"

    cd ..
}

teardown() {
    stop_sql_server 1
    sleep 0.5
    teardown_common
}

@test "remotes-sql-server: sql-push --set-remote within session" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt remote add origin file://../rem1
    dolt commit -am "add test"
    dolt checkout -b other
    start_sql_server repo1

    run dolt sql -q "call dolt_push()"
    [ $status -ne 0 ]
    [[ "$output" =~ "The current branch other has no upstream branch" ]] || false

    dolt sql -q "call dolt_push('--set-upstream', 'origin', 'other')"

    skip "In-memory branch doesn't track upstream"
    dolt sql -q "call dolt_push()"
}

@test "remotes-sql-server: push on sql-session commit" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    start_sql_server repo1

    dolt sql -q "CALL DOLT_COMMIT('-am', 'Step 1');"
    stop_sql_server 1 && sleep 0.5

    cd ../repo2
    dolt pull remote1
    run dolt sql -q "select * from test" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk" ]] || false
    [[ "${lines[1]}" =~ "0" ]] || false
    [[ "${lines[2]}" =~ "1" ]] || false
    [[ "${lines[3]}" =~ "2" ]] || false
}

@test "remotes-sql-server: async push on sql-session commit" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt config --local --add sqlserver.global.dolt_async_replication 1
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1

    start_sql_server repo1

    dolt sql -q "CALL DOLT_COMMIT('-am', 'Step 1');"

    # wait for the process to exit after we stop it
    stop_sql_server 1

    cd ../repo2
    dolt pull remote1
    
    run dolt sql -q "select * from test" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk" ]] || false
    [[ "${lines[1]}" =~ "0" ]] || false
    [[ "${lines[2]}" =~ "1" ]] || false
    [[ "${lines[3]}" =~ "2" ]] || false
}

@test "remotes-sql-server: pull new commits on read" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt commit -am "cm"
    dolt push remote1 main

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo2 && sleep 1

    run dolt sql -q "show tables" --result-format csv
    [ $status -eq 0 ]
    [[ "$output" =~ "Tables_in_repo2" ]] || false
    [[ "$output" =~ "test" ]] || false
}

@test "remotes-sql-server: pull remote not found" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main

    start_sql_server repo1 ./server-log.txt

    run dolt sql -q "show tables"
    [ $status -eq 0 ]
    [[ "$output" =~ "Table" ]] || false

    run grep 'replication disabled' ./server-log.txt
    [[ "$output" =~ "replication disabled" ]] || false
}

@test "remotes-sql-server: quiet pull warnings" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main

    start_sql_server repo1 ./server-log.txt

    run dolt sql -q "show tables"
    [ $status -eq 0 ]
    [[ "$output" =~ "Table" ]] || false

    run grep 'failed to load replica database from remote' ./server-log.txt
    [[ "$output" =~ "failed to load replica database from remote" ]] || false
}

@test "remotes-sql-server: push remote not found" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown

    start_sql_server repo1 ./server-log.txt

    run dolt sql -q "show tables"
    [[ "$output" =~ "Table" ]] || false

    run grep 'replication disabled' ./server-log.txt
    [[ "$output" =~ "replication disabled" ]] || false
}

@test "remotes-sql-server: quiet push warnings" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors 1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown
    start_sql_server repo1

    run dolt sql -q "show tables"
    [ $status -eq 0 ]
    [[ "$output" =~ "Tables_in_repo1" ]] || false
    [[ "$output" =~ "test" ]] || false
}

@test "remotes-sql-server: pull multiple heads" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b new_feature
    dolt push remote1 new_feature
    dolt push remote1 main

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main,new_feature
    start_sql_server repo2

    run dolt sql -q "call dolt_checkout('new_feature')"
    [ $status -eq 0 ]
    [[ "$output" =~ "0" ]] || false
    
    run dolt sql -q "select name from dolt_branches order by name"
    [ $status -eq 0 ]
    [[ "$output" =~ "name" ]] || false
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "new_feature" ]] || false
}

@test "remotes-sql-server: connect to remote head" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b b1
    dolt commit -am "first commit"
    dolt branch b2
    dolt branch b3
    dolt push remote1 b1
    dolt push remote1 b2
    dolt push remote1 b3
    dolt checkout main
    dolt push remote1 main

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    start_sql_server repo2

    # No data on main
    run dolt sql -q "show tables"
    [ $status -eq 0 ]
    [ "$output" = "" ]

    # Connecting to heads that exist only on the remote should work fine (they get fetched)
    dolt --use-db "repo2/b1" sql -q "show tables"
    dolt sql -q 'use `repo2/b2`'
    run dolt sql -q 'select * from `repo2/b2`.test'
    [ $status -eq 0 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ " 0 " ]] || false
    [[ "$output" =~ " 1 " ]] || false
    [[ "$output" =~ " 2 " ]] || false

    # Remote branch we have never USEd before
    run dolt sql -q 'select * from `repo2/b3`.test'
    [ $status -eq 0 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ " 0 " ]] || false
    [[ "$output" =~ " 1 " ]] || false
    [[ "$output" =~ " 2 " ]] || false
    
    # Connecting to heads that don't exist should error out
    run dolt --use-db "repo2/notexist" sql -q 'use `repo2/b2`'
    [ $status -ne 0 ]
    [[ $output =~ "database not found" ]] || false
    
    run dolt sql -q 'use `repo2/notexist`'
    [ $status -ne 0 ]
    [[ $output =~ "database not found" ]] || false
}

@test "remotes-sql-server: pull all heads" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt commit -am "new commit"
    dolt push remote1 main

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo2

    run dolt sql -q "show tables"
    [ $status -eq 0 ]
    [[ $output =~ "Tables_in_repo2" ]] || false
    [[ $output =~ "test" ]] || false
}

@test "remotes-sql-server: pull invalid head" {
    skiponwindows "Missing dependencies"
    skip "query retry prevents error checking"

    cd repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads unknown
    start_sql_server repo2

    run dolt sql -q "show tables"
    [ $status -ne 0 ]
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false    
}

@test "remotes-sql-server: pull multiple heads, one invalid" {
    skiponwindows "Missing dependencies"
    skip "query retry prevents error checking"

    cd repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo2

    run dolt sql -q "show tables"
    [ $status -ne 0 ]
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
}

@test "remotes-sql-server: quiet pull all heads warnings" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt commit -am "cm"
    dolt push remote1 main

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo2

    run dolt sql -q "SHOW tables"
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

@test "remotes-sql-server: connect to missing branch pulls remote" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b feature-branch
    dolt sql -q "create table newTable (a int primary key)"
    dolt commit -Am "new commit"
    dolt push remote1 feature-branch

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo2

    run dolt sql -q "SHOW tables"
    [ $status -eq 0 ]
    [ "$output" = "" ]

    run dolt --use-db "repo2/feature-branch" sql -q "SHOW Tables"
    [ $status -eq 0 ]
    [[ $output =~ "newTable" ]] || false
}

@test "remotes-sql-server: connect to hash works" {
    skiponwindows "Missing dependencies"
    
    cd repo1
    dolt commit -am "cm"
    dolt push remote1 main
    head_hash=$(get_head_commit)

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo2

    dolt sql -q "show tables"
    run dolt sql -q "show tables"
    [ $status -eq 0 ]
    [[ $output =~ "Tables_in_repo2" ]] || false
    [[ $output =~ "test" ]] || false
    
    run dolt sql -q "use \`repo2/$head_hash\`"
    [ $status -eq 0 ]
    [ ! "$output" = "Database changed" ]
}

@test "remotes-sql-server: connect to tag works" {
    skiponwindows "Missing dependencies"
    
    cd repo1
    dolt commit -am "cm"
    dolt push remote1 main
    head_hash=$(get_head_commit)

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    dolt tag v1
    start_sql_server repo2

    dolt sql -q "show tables"
    run dolt sql -q "show tables"
    [ $status -eq 0 ]
    [[ $output =~ "Tables_in_repo2" ]] || false
    [[ $output =~ "test" ]] || false

    run dolt sql -q "use \`repo2/v1\`"
    [ $status -eq 0 ]
    [ ! "$output" = "Database changed" ]
}

@test "remotes-sql-server: connect to remote branch that does not exist locally" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b feature
    dolt commit -am "first commit"
    dolt push remote1 feature
    dolt checkout main
    dolt push remote1 main

    cd ../repo2
    dolt fetch
    run dolt branch
    [[ ! "$output" =~ "feature" ]] || false

    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1

    start_sql_server repo2

    # No data on main
    run dolt sql -q "show tables"
    [ $status -eq 0 ]
    [ "$output" = "" ]

    run dolt --use-db repo2/feature sql -q "select active_branch()"
    [ $status -eq 0 ]
    [[ "$output" =~ "feature" ]] || false
    [[ ! "$output" =~ "main" ]] || false

    run dolt --use-db repo2/feature sql -q "show tables"
    [ $status -eq 0 ]
    [[ "$output" =~ "Tables_in_repo2/feature" ]] || false
    [[ "$output" =~ "test" ]] || false

    run dolt branch
    [[ "$output" =~ "feature" ]] || false
}

@test "remotes-sql-server: connect to remote branch pushed after server starts" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b feature
    dolt commit -am "first commit"
    dolt push remote1 feature
    dolt checkout main
    dolt push remote1 main

    cd ../repo2
    dolt fetch
    run dolt branch
    [[ ! "$output" =~ "feature" ]] || false

    dolt config --local --add sqlserver.global.dolt_replicate_all_heads 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1

    start_sql_server repo2

    cd ../repo1
    dolt branch newbranch
    dolt push remote1 newbranch

    run dolt --use-db repo2/feature --port $PORT --host 0.0.0.0 --no-tls sql -q "select active_branch()"
    [ $status -eq 0 ]
    [[ "$output" =~ "feature" ]] || false

    run dolt --use-db repo2/newbranch --port $PORT --host 0.0.0.0 --no-tls sql -q "select active_branch()"
    [ $status -eq 0 ]
    [[ "$output" =~ "newbranch" ]] || false

    run dolt branch
    [[ "$output" =~ "feature" ]] || false
    [[ "$output" =~ "newbranch" ]] || false
}

@test "remotes-sql-server: connect to remote tracking branch fails if there are multiple remotes" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b feature
    dolt commit -am "first commit"
    dolt push remote1 feature
    dolt checkout main
    dolt push remote1 main

    cd ../repo2
    dolt fetch
    dolt remote add remote2 file://../rem1
    dolt fetch remote2
    run dolt branch
    [[ ! "$output" =~ "feature" ]] || false

    start_sql_server repo2 >> server_log.txt 2>&1

    # No data on main
    run dolt sql -q "show tables"
    [ $status -eq 0 ]
    [ "$output" = "" ]

    run dolt --use-db repo2/feature sql -q "select active_branch()"
    [ $status -eq 1 ]
    [[ "$output" =~ "'feature' matched multiple remote tracking branches" ]] || false

    run grep "'feature' matched multiple remote tracking branches" server_log.txt
    [ "${#lines[@]}" -ne 0 ]
}

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | cut -c 13-44
}
