#! /usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

ARCHIVE_PATH="$BATS_TEST_DIRNAME/archive-test-repos/v2/noms/oldgen/27avtn2a3upddh52eu750m4709gfps7s.darc"

setup() {
    setup_no_dolt_init
}

teardown() {
    teardown_common
}

@test "admin-archive-inspect: basic archive inspection" {
    run dolt admin archive-inspect "$ARCHIVE_PATH"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Archive file:" ]] || false
    [[ "$output" =~ "File size:" ]] || false
    [[ "$output" =~ "Format version:" ]] || false
    [[ "$output" =~ "File signature:" ]] || false
    [[ "$output" =~ "Chunk count:" ]] || false
    [[ "$output" =~ "Byte span count:" ]] || false
    [[ "$output" =~ "Index size:" ]] || false
    [[ "$output" =~ "Metadata size:" ]] || false
}

@test "admin-archive-inspect: archive file must exist" {
    run dolt admin archive-inspect "/nonexistent/file.darc"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: Archive file does not exist:" ]] || false
}

@test "admin-archive-inspect: mmap flag works" {
    run dolt admin archive-inspect --mmap "$ARCHIVE_PATH"
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Archive file:" ]] || false
    [[ "$output" =~ "Chunk count:" ]] || false
}

@test "admin-archive-inspect: object-id inspection with invalid hash" {
    run dolt admin archive-inspect --object-id "invalid" "$ARCHIVE_PATH"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: Invalid object ID format. Expected 32-character base32 encoded hash." ]] || false
}

@test "admin-archive-inspect: object-id inspection with valid hash format but not found" {
    run dolt admin archive-inspect --object-id "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "$ARCHIVE_PATH"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Object inspection:" ]] || false
    [[ "$output" =~ "Hash:" ]] || false
    [[ "$output" =~ "Prefix:" ]] || false
    [[ "$output" =~ "Suffix:" ]] || false
    [[ "$output" =~ "Error inspecting object:" ]] || false
    [[ "$output" =~ "not found" ]] || false
}

@test "admin-archive-inspect: inspect-index with invalid index" {
    run dolt admin archive-inspect --inspect-index "invalid" "$ARCHIVE_PATH"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: Invalid index format. Expected unsigned integer." ]] || false
}

@test "admin-archive-inspect: inspect-index with valid index" {
    run dolt admin archive-inspect --inspect-index "0" "$ARCHIVE_PATH"
    [ "$status" -eq 0 ]

    [[ "$output" =~ "Index inspection:" ]] || false
    [[ "$output" =~ "Index: 0" ]] || false
    [[ "$output" =~ "Index reader type: *nbs.inMemoryArchiveIndexReader" ]] || false
    [[ "$output" =~ "Chunk count: 230" ]] || false
    [[ "$output" =~ "Byte span count: 231" ]] || false
    [[ "$output" =~ "Prefix: 0xdee0ad259117ac" ]] || false
    [[ "$output" =~ "Suffix: 0xbbb4152ab219aeffc3c56acd" ]] || false
}

@test "admin-archive-inspect: inspect-index with out of range index" {
    run dolt admin archive-inspect --inspect-index "99999999" "$ARCHIVE_PATH"
    [ "$status" -eq 1 ]
    [[ "$output" =~ "Error: index out of range" ]] || false
}

@test "admin-archive-inspect: mmap and non-mmap produce similar output format" {
    run dolt admin archive-inspect "$ARCHIVE_PATH"
    [ "$status" -eq 0 ]
    output_nommap="$output"
    
    run dolt admin archive-inspect --mmap "$ARCHIVE_PATH"
    [ "$status" -eq 0 ]
    output_mmap="$output"
    
    # Both should have the same basic structure
    [[ "$output_nommap" =~ "Archive file:" ]] || false
    [[ "$output_mmap" =~ "Archive file:" ]] || false
    [[ "$output_nommap" =~ "Chunk count:" ]] || false
    [[ "$output_mmap" =~ "Chunk count:" ]] || false
}
