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
    dolt sql -q "insert into test values (0),(1),(2)"

    cd ..
}

teardown() {
    stop_sql_server
    teardown_common
}

@test "remotes-sql-server: sql-push --set-remote within session" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt remote add origin file://../rem1
    start_sql_server repo1

    dolt push origin main
    run server_query repo1 1 "select dolt_push() as p" "p\n0"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "the current branch has no upstream branch" ]] || false

    server_query repo1 1 "select dolt_push('--set-upstream', 'origin', 'main') as p" "p\n1"

    skip "In-memory branch doesn't track upstream"
    server_query repo1 1 "select dolt_push() as p" "p\n1"
}

@test "remotes-sql-server: push on sql-session commit" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    start_sql_server repo1

    multi_query repo1 1 "
        SELECT DOLT_COMMIT('-am', 'Step 1');"

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
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote remote1
    dolt config --local --add sqlserver.global.dolt_async_replication 1
    start_sql_server repo1

    multi_query repo1 1 "
        SELECT DOLT_COMMIT('-am', 'Step 1');"

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
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt commit -am "cm"
    dolt push remote1 main

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo2

    server_query repo2 1 "show tables" "Tables_in_repo2\ntest"
}

@test "remotes-sql-server: pull remote not found error" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main

    run dolt sql-server
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
}

@test "remotes-sql-server: quiet pull warnings" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo1

    run server_query repo1 1 "show tables" "Table\n"
}

@test "remotes-sql-server: push remote not found error" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown

    run dolt sql-server
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
}

@test "remotes-sql-server: quiet push warnings" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors 1
    dolt config --local --add sqlserver.global.dolt_replicate_to_remote unknown
    start_sql_server repo1

    server_query repo1 1 "show tables" "Tables_in_repo1\ntest"
}

@test "remotes-sql-server: pull multiple heads" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt checkout -b new_feature
    dolt push remote1 new_feature
    dolt push remote1 main

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main,new_feature
    start_sql_server repo2

    server_query repo2 1 "select dolt_checkout('new_feature') as b" "b\n0"
    server_query repo2 1 "select name from dolt_branches order by name" "name\nmain\nnew_feature"
}

@test "remotes-sql-server: connect to remote head" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

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
    server_query repo2 1 "show tables" ""
    
    # Connecting to heads that exist only on the remote should work fine (they get fetched)
    server_query "repo2/new_feature" 1 "show tables" "Tables_in_repo2/new_feature\ntest"
    server_query repo2 1 'use `repo2/new_feature2`' ""
    server_query repo2 1 'select * from `repo2/new_feature2`.test' "pk\n0\n1\n2"

    # Connecting to heads that don't exist should error out
    run server_query "repo2/notexist" 1 'use `repo2/new_feature2`' ""
    [ $status -eq 1 ]
    [[ $output =~ "database not found" ]] || false
    
    run server_query repo2 1 'use `repo2/notexist`' ""
    [ $status -eq 1 ]
    [[ $output =~ "database not found" ]] || false

    # Creating a branch locally that doesn't exist on the remote
    # works, but connecting to it is an error (nothing to pull)
    server_query "repo2/new_feature" 1 "select dolt_checkout('-b', 'new_branch') as b" "b\n0"

    run server_query "repo2/new_branch" 1 "show tables" "Table\ntest"
    [ $status -eq 1 ]
    [[ $output =~ "database not found" ]] || false
}

@test "remotes-sql-server: pull all heads" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt commit -am "new commit"
    dolt push remote1 main

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo2

    server_query repo2 1 "show tables" "Tables_in_repo2\ntest"
}

@test "remotes-sql-server: pull invalid head" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."
    skip "query retry prevents error checking"

    cd repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads unknown
    start_sql_server repo2

    run server_query repo2 1 "show tables" "Table\n"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
}

@test "remotes-sql-server: pull multiple heads, one invalid" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."
    skip "query retry prevents error checking"

    cd repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo2

    run server_query repo2 1 "show tables" "Table\n"
    [ "$status" -eq 1 ]
    [[ ! "$output" =~ "panic" ]]
    [[ "$output" =~ "remote not found: 'unknown'" ]] || false
}

@test "remotes-sql-server: quiet pull all heads warnings" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt commit -am "cm"
    dolt push remote1 main

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_skip_replication_errors 1
    dolt config --local --add sqlserver.global.dolt_read_replica_remote unknown
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo2

    server_query repo2 1 "show tables" "Table\n"
}

@test "remotes-sql-server: connect to missing branch pulls remote" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."

    cd repo1
    dolt checkout -b feature-branch
    dolt commit -am "new commit"
    dolt push remote1 feature-branch

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo2

    server_query repo2 1 "SHOW tables" "" # no tables on main
    server_query "repo2/feature-branch" 1 "SHOW Tables" "Tables_in_repo2/feature-branch\ntest"
}

@test "remotes-sql-server: connect to hash works" {
    skiponwindows "Has dependencies that are missing on the Jenkins Windows installation."
    
    cd repo1
    dolt commit -am "cm"
    dolt push remote1 main
    head_hash=$(get_head_commit)

    cd ../repo2
    dolt config --local --add sqlserver.global.dolt_read_replica_remote remote1
    dolt config --local --add sqlserver.global.dolt_replicate_heads main
    start_sql_server repo2

    server_query repo2 1 "show tables" "Tables_in_repo2\ntest"
    server_query repo2 1 "use \`repo2/$head_hash\`" ""
}

get_head_commit() {
    dolt log -n 1 | grep -m 1 commit | cut -c 13-44
}
