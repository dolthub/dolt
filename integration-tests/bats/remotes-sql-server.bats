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

    run dolt sql-client --use-db repo1 -P $PORT -u dolt -q "call dolt_push()"
    [ $status -ne 0 ]
    [[ "$output" =~ "the current branch has no upstream branch" ]] || false

    dolt sql-client --use-db repo1 -P $PORT -u dolt -q "call dolt_push('--set-upstream', 'origin', 'other')"

    skip "In-memory branch doesn't track upstream"
    dolt sql-client --use-db repo1 -P $PORT -u dolt -q "call dolt_push()"
}

@test "remotes-sql-server: push on sql-session commit" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    start_sql_server repo1

    dolt sql-client --use-db repo1 -P $PORT -u dolt -q "CALL DOLT_COMMIT('-am', 'Step 1');"
    stop_sql_server 1 && sleep 0.5

    cd ../repo2
    dolt pull remote1
    run dolt sql -q "select * from test" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk" ]]
    [[ "${lines[1]}" =~ "0" ]]
    [[ "${lines[2]}" =~ "1" ]]
    [[ "${lines[3]}" =~ "2" ]]
}

@test "remotes-sql-server: async push on sql-session commit" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt config --local --add sqlserver.global.dolt_async_replication 1
    start_sql_server repo1

    dolt sql-client --use-db repo1 -P $PORT -u dolt -q "CALL DOLT_COMMIT('-am', 'Step 1');"

    # wait for the process to exit after we stop it
    stop_sql_server 1

    cd ../repo2
    dolt pull remote1
    run dolt sql -q "select * from test" -r csv
    [ "$status" -eq 0 ]
    [[ "${lines[0]}" =~ "pk" ]]
    [[ "${lines[1]}" =~ "0" ]]
    [[ "${lines[2]}" =~ "1" ]]
    [[ "${lines[3]}" =~ "2" ]]
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

    skip "todo"
    dolt sql-client --use-db repo2 -P $PORT -u dolt -q "show tables" -r csv
    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q "show tables" -r csv
    [ $status -eq 0 ]
    [[ "$output" =~ "Tables_in_repo2" ]] || false
    [[ "$output" =~ "test" ]] || false
}

@test "remotes-sql-server: pull remote not found error" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main

    run dolt sql-server -P 3333
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
}

@test "remotes-sql-server: quiet pull warnings" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo1

    run dolt sql-client --use-db repo1 -P $PORT -u dolt -q "show tables"
    [ $status -eq 0 ]
    [[ "$output" =~ "Table" ]] || false
}

@test "remotes-sql-server: push remote not found error" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown

    run dolt sql-server -P 3333
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
}

@test "remotes-sql-server: quiet push warnings" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors 1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown
    start_sql_server repo1

    run dolt sql-client --use-db repo1 -P $PORT -u dolt -q "show tables"
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

    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q "select dolt_checkout('new_feature') as b"
    [ $status -eq 0 ]
    [[ "$output" =~ "b" ]] || false
    [[ "$output" =~ "0" ]] || false
    
    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q "select name from dolt_branches order by name"
    [ $status -eq 0 ]
    [[ "$output" =~ "name" ]] || false
    [[ "$output" =~ "main" ]] || false
    [[ "$output" =~ "new_feature" ]] || false
}

@test "remotes-sql-server: connect to remote head" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b new_feature
    dolt commit -am "first commit"
    dolt branch new_feature2
    dolt push remote1 new_feature
    dolt push remote1 new_feature2
    dolt checkout main
    dolt push remote1 main

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo2

    # No data on main
    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q "show tables"
    [ $status -eq 0 ]
    [ "$output" = "" ]

    # Can't use dolt sql-client to connect to branches
    
    # Connecting to heads that exist only on the remote should work fine (they get fetched)
    dolt sql-client --use-db "repo2/new_feature" -u dolt -P $PORT -q "show tables" "Tables_in_repo2/new_feature\ntest"
    dolt sql-client --use-db repo2 -P $PORT -u dolt -q 'use `repo2/new_feature2`'
    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q 'select * from `repo2/new_feature2`.test'
    [ $status -eq 0 ]
    [[ "$output" =~ "pk" ]] || false
    [[ "$output" =~ " 0 " ]] || false
    [[ "$output" =~ " 1 " ]] || false
    [[ "$output" =~ " 2 " ]] || false
    
    # Connecting to heads that don't exist should error out
    run dolt sql-client --use-db "repo2/notexist" -u dolt -P $PORT -q 'use `repo2/new_feature2`'
    [ $status -ne 0 ]
    [[ $output =~ "database not found" ]] || false
    
    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q 'use `repo2/notexist`'
    [ $status -ne 0 ]
    [[ $output =~ "database not found" ]] || false

    # Creating a branch locally that doesn't exist on the remote
    # works, but connecting to it is an error (nothing to pull)
    dolt sql-client --use-db "repo2/new_feature" -u dolt -P $PORT -q "select dolt_checkout('-b', 'new_branch')"

    run dolt sql-client --use-db "repo2/new_branch" -u dolt -P $PORT -q "show tables"
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

    dolt sql-client --use-db repo2 -P $PORT -u dolt -q "show tables"
    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q "show tables"
    [ $status -eq 0 ]
    skip "todo"
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

    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q "show tables"
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

    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q "show tables"
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

    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q "SHOW tables"
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

@test "remotes-sql-server: connect to missing branch pulls remote" {
    skiponwindows "Missing dependencies"

    cd repo1
    dolt checkout -b feature-branch
    dolt commit -am "new commit"
    dolt push remote1 feature-branch

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo2

    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q "SHOW tables"
    [ $status -eq 0 ]
    [ "$output" = "" ]

    # Can't connect to a specific branch with dolt sql-client
    run dolt sql-client --use-db "repo2/feature-branch" -u dolt -P $PORT -q "SHOW Tables"
    [ $status -eq 0 ]
    [[ $output =~ "feature-branch" ]] || false
    [[ $output =~ "test" ]] || false
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

    dolt sql-client --use-db repo2 -P $PORT -u dolt -q "show tables"
    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q "show tables"
    [ $status -eq 0 ]
    [[ $output =~ "Tables_in_repo2" ]] || false
    [[ $output =~ "test" ]] || false
    
    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q "use \`repo2/$head_hash\`"
    [ $status -eq 0 ]
    [ "$output" = "" ]
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

    dolt sql-client --use-db repo2 -P $PORT -u dolt -q "show tables"
    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q "show tables"
    [ $status -eq 0 ]
    [[ $output =~ "Tables_in_repo2" ]] || false
    [[ $output =~ "test" ]] || false

    run dolt sql-client --use-db repo2 -P $PORT -u dolt -q "use \`repo2/v1\`"
    [ $status -eq 0 ]
    [ "$output" = "" ]
}

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | cut -c 13-44
}
