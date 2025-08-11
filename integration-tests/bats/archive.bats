#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/data-generation.bash

setup() {
    setup_common

    create_test_table
    dolt sql -q "$(insert_statement)"
}

teardown() {
    if [ -n "$remotesrv_pid" ]; then
        kill "$remotesrv_pid"
        wait "$remotesrv_pid" || :
        remotesrv_pid=""
    fi

    assert_feature_version
    teardown_common
}

# For reasons unknown, lambda fails on this test about 10% of the time. It seems to be something having to do with
# the IO subsystem of lambda. The output of the `dolt archive` command is truncated occasionally. Doesn't ever happen
# on regular hosts.
# bats test_tags=no_lambda
@test "archive: too few chunks" {
  dolt sql -q "$(update_statement)"
  dolt gc

  run dolt archive
  [ "$status" -eq 0 ]

  lines="$(echo "$output" | grep -ci 'Not enough chunks to build archive.*skipping')"
  [ "$lines" -eq "2" ]
}

@test "archive: single archive oldgen" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive

  files=$(find . -name "*darc" | wc -l | sed 's/[ \t]//g')
  [ "$files" -eq "1" ]

  # Ensure updates continue to work.
  dolt sql -q "$(update_statement)"
}

@test "archive: single archive newgen" {
  dolt sql -q "$(mutations_and_gc_statement)"

  mkdir remote
  dolt remote add origin file://remote
  dolt push origin main

  dolt clone file://remote cloned
  cd cloned

  dolt archive

  files=$(find . -name "*darc" | wc -l | sed 's/[ \t]//g')
  [ "$files" -eq "1" ]

  # Ensure updates continue to work.
  dolt sql -q "$(update_statement)"
}

@test "archive: multi archive newgen then revert" {
  # Getting multiple table files in `newgen` is a little gross.
  dolt sql -q "$(mutations_and_gc_statement)"
  mkdir remote
  dolt remote add origin file://remote
  dolt push origin main

  dolt clone file://remote cloned
  cd cloned
  dolt archive --purge
  files=$(find . -name "*darc" | wc -l | sed 's/[ \t]//g')
  [ "$files" -eq "1" ]

  cd ..
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt push origin main

  cd cloned
  dolt fetch
  dolt archive --purge
  files=$(find . -name "*darc" | wc -l | sed 's/[ \t]//g')
  [ "$files" -eq "2" ]

  dolt archive --revert
  files=$(find . -name "*darc" | wc -l | sed 's/[ \t]//g')
  [ "$files" -eq "0" ]

  dolt fsck
}

@test "archive: multiple archives" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt sql -q "$(mutations_and_gc_statement)"

  dolt archive

  files=$(find . -name "*darc" | wc -l | sed 's/[ \t]//g')
  [ "$files" -eq "3" ]

  # dolt log --stat will load every single chunk.
  commits=$(dolt log --stat --oneline | wc -l | sed 's/[ \t]//g')
  [ "$commits" -eq "186" ]
}

@test "archive: archive multiple times" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive

  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive

  files=$(find . -name "*darc" | wc -l | sed 's/[ \t]//g')
  [ "$files" -eq "2" ]
}

@test "archive: archive --revert (fast)" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive
  dolt archive --revert

  # dolt log --stat will load every single chunk. 66 manually verified.
  commits=$(dolt log --stat --oneline | wc -l | sed 's/[ \t]//g')
  [ "$commits" -eq "66" ]
}

@test "archive: archive --revert (rebuild)" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive
  dolt archive --revert

  # dolt log --stat will load every single chunk. 66 manually verified.
  commits=$(dolt log --stat --oneline | wc -l | sed 's/[ \t]//g')
  [ "$commits" -eq "66" ]
}

@test "archive: archive --purge" {
  dolt sql -q "$(mutations_and_gc_statement)"

  # find impl differences by platform makes this a pain.
  tablefile=$(find .dolt/noms/oldgen -type f -print | awk -F/ 'length($NF) == 32 && $NF ~ /^[a-v0-9]{32}$/')

  [ -e "$tablefile" ] # extreme paranoia. make sure it exists before.
  dolt archive --purge
  # Ensure the table file is gone.
  [ ! -e "$tablefile" ]
}


@test "archive: can clone archived repository" {
    mkdir -p remote/.dolt
    mkdir cloned

    # Copy the archive test repo to remote directory
    cp -R $BATS_TEST_DIRNAME/archive-test-repos/base/* remote/.dolt
    cd remote

    port=$( definePORT )

    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ../cloned
    dolt clone http://localhost:$port/test-org/test-repo repo1
    cd repo1

    # Verify we can read data
    run dolt sql -q 'select sum(i) from tbl;'
    [[ "$status" -eq 0 ]] || false
    [[ "$output" =~ "138075" ]] || false # i = 1 - 525, sum is 138075

    kill $remotesrv_pid
    wait $remotesrv_pid || :
    remotesrv_pid=""

    ## The above test is the setup for the next test - so we'll stick both in here.
    ## This tests cloning from a clone. Archive files are generally in oldgen, but not the case with a fresh clone.
    cd ../../
    mkdir clone2

    cd cloned/repo1 # start the server using the clone from above.
    port=$( definePORT )
    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ../../clone2
    run dolt clone http://localhost:$port/test-org/test-repo repo2
    [ "$status" -eq 0 ]
    cd repo2

    run dolt sql -q 'select sum(i) from tbl;'
    [[ "$status" -eq 0 ]] || false
    [[ "$output" =~ "138075" ]] || false # i = 1 - 525, sum is 138075
}

@test "archive: can clone repository with mixed types" {
    mkdir -p remote/.dolt
    mkdir cloned

    # Copy the archive test repo to remote directory
    cp -R $BATS_TEST_DIRNAME/archive-test-repos/base/* remote/.dolt
    cd remote

    # Insert data (commits automatically), but don't gc/archive yet. Want to make sure we can still clone it.
    dolt sql -q "$(insert_statement)"

    port=$( definePORT )

    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ../cloned
    run dolt clone http://localhost:$port/test-org/test-repo repo1
    [ "$status" -eq 0 ]
    cd repo1

    # verify new data is there.
    run dolt sql -q 'select sum(i) from tbl;'
    [[ "$status" -eq 0 ]] || false

    [[ "$output" =~ "151525" ]] || false # i = 1 - 550, sum is 151525
}

@test "archive: can fetch chunks from an archived repo" {
    mkdir -p remote/.dolt
    mkdir cloned

    # Copy the archive test repo to remote directory
    cp -R $BATS_TEST_DIRNAME/archive-test-repos/base/* remote/.dolt
    cd remote

    port=$( definePORT )

    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ../cloned
    dolt clone http://localhost:$port/test-org/test-repo repo1
    # Fetch when there are no changes.
    cd repo1
    dolt fetch

    ## update the remote repo directly. Need to run the archive command when the server is stopped.
    ## This will result in archived files on the remote, which we will need to read chunks from when we fetch.
    cd ../../remote
    kill $remotesrv_pid
    wait $remotesrv_pid || :
    remotesrv_pid=""
    dolt sql -q "$(mutations_and_gc_statement)"
    dolt archive

    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ../cloned/repo1
    dolt fetch

    run dolt status
    [ "$status" -eq 0 ]

    [[ "$output" =~ "Your branch is behind 'origin/main' by 20 commits, and can be fast-forwarded" ]] || false

    # Verify the repo has integrity.
    dolt fsck
}

@test "archive: backup and restore" {
  # cp the repository from the test dir.
  mkdir -p original/.dolt
  cp -R $BATS_TEST_DIRNAME/archive-test-repos/base/* original/.dolt

  cd original
  dolt backup add bac1 file://../bac1
  dolt backup sync bac1

  cd ..

  dolt backup restore file://./bac1 restored
  cd restored
  # Verify we can read data
  run dolt sql -q 'select sum(i) from tbl;'
  [[ "$status" -eq 0 ]] || false
  [[ "$output" =~ "138075" ]] || false # i = 1 - 525, sum is 138075
}

@test "archive: mixed compression types" {
  port=$( definePORT )
  # run a bare server.
  mkdir remotesrv
  cd remotesrv
  remotesrv --http-port $port --grpc-port $port &
  remotesrv_pid=$!
  [[ "$remotesrv_pid" -gt 0 ]] || false
  cd ..

  # Copy the archive test repo to remote directory
  mkdir -p repo/.dolt
  cp -R $BATS_TEST_DIRNAME/archive-test-repos/base/* repo/.dolt
  cd repo

  # Make some new commits.
  update_statement

  # Get everything into a non-journal form. Repository has mixed storage types now.
  dolt gc

  # Push, and enable the archive streamer. In the future this will be the default.
  dolt remote add origin http://localhost:$port/test-org/test-repo
  DOLT_ARCHIVE_PULL_STREAMER=1 dolt push origin main

  cd ..

  dolt clone http://localhost:$port/test-org/test-repo repo2
  cd repo2

  dolt fsck

}

@test "archive: large push remote without archive default produces no new archives" {
    unset DOLT_ARCHIVE_PULL_STREAMER

    mkdir -p remote/.dolt
    cp -R $BATS_TEST_DIRNAME/archive-test-repos/base/* remote/.dolt
    cd remote

    port=$( definePORT )
    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false
    
    cd ..
    mkdir -p clone/.dolt
    cp -R $BATS_TEST_DIRNAME/archive-test-repos/large_clone/* clone/.dolt
    cd clone
    dolt remote add r1 http://localhost:$port/test-org/test-repo
    dolt push r1 HEAD:main

    cd ../remote
    run dolt admin storage
    [ $status -eq 0 ]

    ## This output indicates that the new content pushed to the remote all landed as snappy chunks
    ## in a classic table file. multiline regex - no quotes - to match this text:
    #   Table File Metadata:
    #     Snappy Chunk Count: 1609
    [[ $output =~ Table[[:space:]]File[[:space:]]Metadata:[[:space:]]*Snappy[[:space:]]Chunk[[:space:]]Count:[[:space:]]1609[[:space:]] ]] || false
}

@test "archive: small push remote without archive default produces no new archives" {
    unset DOLT_ARCHIVE_PULL_STREAMER

    mkdir -p remote/.dolt
    cp -R $BATS_TEST_DIRNAME/archive-test-repos/base/* remote/.dolt
    cd remote

    port=$( definePORT )
    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ..
    mkdir -p clone/.dolt
    cp -R $BATS_TEST_DIRNAME/archive-test-repos/small_clone/* clone/.dolt
    cd clone
    dolt remote add r1 http://localhost:$port/test-org/test-repo
    dolt push r1 HEAD:main

    cd ../remote
    run dolt admin storage
    [ $status -eq 0 ]

    ## This output indicates that the new content pushed to the remote all landed as snappy chunks
    ## in a classic table file. multiline regex - no quotes - to match this text:
    #   Table File Metadata:
    #     Snappy Chunk Count: 9
    [[ $output =~ Table[[:space:]]File[[:space:]]Metadata:[[:space:]]*Snappy[[:space:]]Chunk[[:space:]]Count:[[:space:]]9[[:space:]] ]] || false
}

@test "archive: large push remote with archive default produces new archive with converted snappy chunks" {
    export DOLT_ARCHIVE_PULL_STREAMER=1

    mkdir -p remote/.dolt
    cp -R $BATS_TEST_DIRNAME/archive-test-repos/base/* remote/.dolt
    cd remote

    port=$( definePORT )
    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ..
    mkdir -p clone/.dolt
    cp -R $BATS_TEST_DIRNAME/archive-test-repos/large_clone/* clone/.dolt
    cd clone
    dolt remote add r1 http://localhost:$port/test-org/test-repo
    dolt push r1 HEAD:main

    cd ../remote
    run dolt admin storage
    [ $status -eq 0 ]

    ## This output indicates that the new content pushed to the remote all landed as zStd chunks
    ## in an archive file. multiline regex - no quotes - to match this text:
    #   Archive Metadata:
    #     Format Version: 3
    #     Snappy Chunk Count: 0 (bytes: 0)
    #     ZStd Chunk Count: 1609
    [[ $output =~ Archive[[:space:]]Metadata:[[:space:]]*Format[[:space:]]Version:[[:space:]]3[[:space:]]*Snappy[[:space:]]Chunk[[:space:]]Count:[[:space:]]0.*ZStd[[:space:]]Chunk[[:space:]]Count:[[:space:]]1609 ]] || false
}

@test "archive: small push remote with archive default produces archive with snappy chunks" {
    export DOLT_ARCHIVE_PULL_STREAMER=1

    mkdir -p remote/.dolt
    cp -R $BATS_TEST_DIRNAME/archive-test-repos/base/* remote/.dolt
    cd remote

    port=$( definePORT )
    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ..
    mkdir -p clone/.dolt
    cp -R $BATS_TEST_DIRNAME/archive-test-repos/small_clone/* clone/.dolt
    cd clone
    dolt remote add r1 http://localhost:$port/test-org/test-repo
    dolt push r1 HEAD:main

    cd ../remote
    run dolt admin storage
    [ $status -eq 0 ]

    ## This output indicates that the new content pushed to the remote all landed as snappy chunks
    ## in an archive file. multiline regex - no quotes - to match this text:
    #   Archive Metadata:
    #     Format Version: 3
    #     Snappy Chunk Count: 9
    [[ $output =~ Archive[[:space:]]Metadata:[[:space:]]*Format[[:space:]]Version:[[:space:]]3[[:space:]]*Snappy[[:space:]]Chunk[[:space:]]Count:[[:space:]]9[[:space:]] ]] || false
}

@test "archive: fetch into empty database with archive default" {
    mkdir -p remote/.dolt
    cp -R $BATS_TEST_DIRNAME/archive-test-repos/base/* remote/.dolt
    cd remote

    port=$( definePORT )
    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ../
    dolt remote add r1 http://localhost:$port/test-org/test-repo
    DOLT_ARCHIVE_PULL_STREAMER=1 dolt fetch r1

    run dolt admin storage
    [ $status -eq 0 ]

    ## This output indicates that the new content was fetch from the remote into an archive file. Note that since
    ## the remote is all archive, the chunks end up as zStd as well.
    ## multiline regex - no quotes - to match this text:
    #   Archive Metadata:
    #     Format Version: 3
    #     Snappy Chunk Count: 0 (bytes: 0)
    #     ZStd Chunk Count: 260
    [[ $output =~ Archive[[:space:]]Metadata:[[:space:]]*Format[[:space:]]Version:[[:space:]]3[[:space:]]*Snappy[[:space:]]Chunk[[:space:]]Count:[[:space:]]0.*ZStd[[:space:]]Chunk[[:space:]]Count:[[:space:]]260 ]] || false

    dolt fsck
}

@test "archive: fetch into empty database with archive disabled" {
    unset DOLT_ARCHIVE_PULL_STREAMER

    mkdir -p remote/.dolt
    cp -R $BATS_TEST_DIRNAME/archive-test-repos/base/* remote/.dolt
    cd remote

    port=$( definePORT )
    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ../
    dolt remote add r1 http://localhost:$port/test-org/test-repo
    dolt fetch r1

    run dolt admin storage
    [ $status -eq 0 ]

    ## This output indicates that the new content was fetched from the remote into a table file. Note that since
    ## the remote is all archive, the chunks are translated into the snappy format
    ## multiline regex - no quotes - to match this text:
    #   Table File Metadata:
    #     Snappy Chunk Count: 260
    [[ $output =~ Table[[:space:]]File[[:space:]]Metadata:[[:space:]]*Snappy[[:space:]]Chunk[[:space:]]Count:[[:space:]]260[[:space:]] ]] || false

    dolt fsck
}

@test "archive: read legacy v1 database" {
  mkdir -p original/.dolt
  cp -R $BATS_TEST_DIRNAME/archive-test-repos/v1/* original/.dolt
  cd original

  dolt fsck
}

@test "archive: read legacy v2 database" {
  mkdir -p original/.dolt
  cp -R $BATS_TEST_DIRNAME/archive-test-repos/v2/* original/.dolt
  cd original

  dolt fsck
}

@test "archive: can mmap archive index" {
    mkdir -p remote/.dolt

    # Copy the archive test repo to remote directory
    cp -R $BATS_TEST_DIRNAME/archive-test-repos/base/* remote/.dolt
    cd remote

    # When mmap_archive_indexes is not set in the config, or is set to false,
    # setting DOLT_TEST_ASSERT_NO_IN_MEMORY_ARCHIVE_INDEX should result in an error
    run env DOLT_TEST_ASSERT_NO_IN_MEMORY_ARCHIVE_INDEX=1 dolt sql -q 'select sum(i) from tbl;'
    [ $status -ne 0 ]
    echo "$output"
    [[ $output =~ "attempted to load archive index into memory but DOLT_TEST_ASSERT_NO_IN_MEMORY_ARCHIVE_INDEX was set" ]] || false

    dolt config --local --set "mmap_archive_indexes" false
    run env DOLT_TEST_ASSERT_NO_IN_MEMORY_ARCHIVE_INDEX=1 dolt sql -q 'select sum(i) from tbl;'
    [ $status -ne 0 ]
    [[ $output =~ "attempted to load archive index into memory but DOLT_TEST_ASSERT_NO_IN_MEMORY_ARCHIVE_INDEX was set" ]] || false

    dolt config --local --set "mmap_archive_indexes" true
    # Verify we can read data
    run env DOLT_TEST_ASSERT_NO_IN_MEMORY_ARCHIVE_INDEX=1 dolt sql -q 'select sum(i) from tbl;'
    echo "$output"
    [[ "$status" -eq 0 ]] || false
    [[ "$output" =~ "138075" ]] || false # i = 1 - 525, sum is 138075
}
