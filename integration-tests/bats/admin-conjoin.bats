#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash
load $BATS_TEST_DIRNAME/helper/data-generation.bash

setup() {
    setup_common
    
    create_test_table
    dolt sql -q "$(insert_statement)"
}

teardown() {
    teardown_common
}

# Helper function to count storage files in oldgen (32 char hash names)
count_oldgen_files() {
    find .dolt/noms/oldgen -type f -name "????????????????????????????????" 2>/dev/null | grep -v LOCK | grep -v manifest | wc -l | tr -d ' '
}

# Helper function to get table file IDs from oldgen
get_oldgen_table_ids() {
    find .dolt/noms/oldgen -type f -name "????????????????????????????????" 2>/dev/null | grep -v LOCK | grep -v manifest | xargs -I {} basename {}
}
# Helper function to get archive file IDs from oldgen
get_oldgen_archive_ids() {
  find .dolt/noms/oldgen -type f -name "????????????????????????????????.darc" 2>/dev/null \
    | grep -v LOCK \
    | grep -v manifest \
    | xargs -I {} basename {} \
    | sed 's/\.darc$//'
}



# Helper function to extract chunk count from fsck output
get_chunk_count() {
    local fsck_output="$1"
    echo "$fsck_output" | grep "Chunks Scanned:" | sed 's/Chunks Scanned: //' | tr -d '\n'
}

@test "admin-conjoin: --all flag and storage file IDs are mutually exclusive" {
    run dolt admin conjoin --all somevalidhash1234567890abcdef12345678
    [ "$status" -eq 1 ]
    [[ "$output" =~ "--all flag and storage file IDs are mutually exclusive" ]] || false
}

@test "admin-conjoin: must specify either --all flag or storage file IDs" {
    run dolt admin conjoin
    [ "$status" -eq 1 ]
    [[ "$output" =~ "must specify either --all flag or storage file IDs" ]] || false
}

@test "admin-conjoin: invalid storage file ID format" {
    run dolt admin conjoin invalidhash
    [ "$status" -eq 1 ]
    [[ "$output" =~ "invalid storage file ID: invalidhash" ]] || false
}

@test "admin-conjoin: --all messages when no oldgen files exist" {
    run dolt admin conjoin --all
    [ "$status" -eq 0 ]
    [[ "$output" =~ "No table files to conjoin" ]] || false
}

@test "admin-conjoin: valid storage file ID that doesn't exist" {
    run dolt admin conjoin abcdef1234567890abcdef1234567890
    [ "$status" -eq 1 ]
    [[ "$output" =~ "storage file not found" ]] || false
}

@test "admin-conjoin: test conjoin with specific IDs" {
    dolt sql -q "$(mutations_and_gc_statement 0)"
    dolt sql -q "$(mutations_and_gc_statement 0)"
    dolt sql -q "$(mutations_and_gc_statement 0)"
    
    storage_ids=($(get_oldgen_table_ids))
    [ "${#storage_ids[@]}" -eq 3 ]
    
    id1="${storage_ids[0]}"
    id2="${storage_ids[1]}"
    
    run dolt fsck
    [ "$status" -eq 0 ]
    chunks_before=$(get_chunk_count "$output")
    
    run dolt admin conjoin "$id1" "$id2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully conjoined table files" ]] || false
    
    run dolt fsck
    [ "$status" -eq 0 ]
    chunks_after=$(get_chunk_count "$output")
    [[ "$chunks_before" -eq "$chunks_after" ]] || false

    storage_ids=($(get_oldgen_table_ids))
    [ "${#storage_ids[@]}" -eq 2 ]
}


@test "admin-conjoin: test conjoin with --all" {
    dolt sql -q "$(mutations_and_gc_statement 0)"
    dolt sql -q "$(mutations_and_gc_statement 0)"
    dolt sql -q "$(mutations_and_gc_statement 0)"
    dolt sql -q "$(mutations_and_gc_statement 0)"
    dolt sql -q "$(mutations_and_gc_statement 0)"

    storage_ids=($(get_oldgen_table_ids))
    [ "${#storage_ids[@]}" -eq 5 ]

    run dolt fsck
    [ "$status" -eq 0 ]
    chunks_before=$(get_chunk_count "$output")

    run dolt admin conjoin --all
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully conjoined all table files" ]] || false

    run dolt fsck
    [ "$status" -eq 0 ]
    chunks_after=$(get_chunk_count "$output")
    [[ "$chunks_before" -eq "$chunks_after" ]] || false

    storage_ids=($(get_oldgen_table_ids))
    [ "${#storage_ids[@]}" -eq 1 ]
}

@test "admin-conjoin: test conjoin with archive files" {
    dolt sql -q "$(mutations_and_gc_statement 1)"
    dolt sql -q "$(mutations_and_gc_statement 1)"
    dolt sql -q "$(mutations_and_gc_statement 1)"
    
    storage_ids=($(get_oldgen_archive_ids))
    [ "${#storage_ids[@]}" -eq 3 ]

    # Get archive file IDs - these should be .darc files in oldgen
    archive_ids=($(get_oldgen_archive_ids))
    [ "${#archive_ids[@]}" -eq 3 ]
    
    # Record chunk count before conjoin
    run dolt fsck
    [ "$status" -eq 0 ]
    chunks_before=$(get_chunk_count "$output")

    id1="${archive_ids[0]}"
    id2="${archive_ids[1]}"
    run dolt admin conjoin "$id1" "$id2"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Successfully conjoined" ]] || false
    
    # Verify data integrity after conjoin
    run dolt fsck
    [ "$status" -eq 0 ]
    chunks_after=$(get_chunk_count "$output")
    [[ "$chunks_before" -eq "$chunks_after" ]] || false

    # There should only be two archive files now.
    archive_ids=($(get_oldgen_archive_ids))
    [ "${#archive_ids[@]}" -eq 2 ]
}

@test "admin-conjoin: test conjoin with mixed file types" {
  dolt sql -q "$(mutations_and_gc_statement 1)"
  dolt sql -q "$(mutations_and_gc_statement 1)"
  dolt sql -q "$(mutations_and_gc_statement 1)"

  # Table files, not archives.
  dolt sql -q "$(mutations_and_gc_statement 0)"
  dolt sql -q "$(mutations_and_gc_statement 0)"
  dolt sql -q "$(mutations_and_gc_statement 0)"

  # sanity check that we have three table files and three archive files
  table_ids=($(get_oldgen_table_ids))
  [ "${#table_ids[@]}" -eq 3 ]
  archive_ids=($(get_oldgen_archive_ids))
  [ "${#archive_ids[@]}" -eq 3 ]

  # Record chunk count before conjoin
  run dolt fsck
  [ "$status" -eq 0 ]
  chunks_before=$(get_chunk_count "$output")

  run dolt admin conjoin --all
  [ "$status" -eq 0 ]

  # Verify data integrity after conjoin
  run dolt fsck
  [ "$status" -eq 0 ]
  chunks_after=$(get_chunk_count "$output")
  [[ "$chunks_before" -eq "$chunks_after" ]] || false

  # There should be no tables files left in oldgen
  storage_ids=($(get_oldgen_table_ids))
  [ "${#storage_ids[@]}" -eq 0 ]
  # There should be one archive file left in oldgen
  archive_ids=($(get_oldgen_archive_ids))
  [ "${#archive_ids[@]}" -eq 1 ]
}