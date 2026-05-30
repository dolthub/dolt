#!/usr/bin/env bats
# Regression suite for the v2.0.7 adaptive-encoding read panic.
#
# Background:
#   PR #11017 flipped UseAdaptiveEncoding to true by default, shipped in
#   v2.0.7. blobStringType.Encoding() and blobBytesType.Encoding() consult
#   that flag when their per-column enc field is the zero value (the case for
#   columns whose schema was loaded via the package-level TextType /
#   LongTextType singletons). The 2.0.7 reader therefore reported a TEXT or
#   BLOB column that an older 1.x server had persisted in StringAddrEnc /
#   BytesAddrEnc form (raw 20-byte hash, no varint prefix) as
#   StringAdaptiveEnc / BytesAdaptiveEnc. The adaptive dispatch then tried
#   to parse the field as [varint length][20-byte hash] and crashed inside
#   hash.New with "invalid hash length: 19".
#
#   In a real-world deployment with many rows in a 1.x-written database, the
#   supervised dolt sql-server panicked ~25/min. errguard recover() caught
#   each panic and surfaced it as a soft SQL error while dolt itself exited
#   0 -- which is why every TEXT/BLOB test in types_compatibility.bats
#   silently passed against the broken 2.0.7 binary.
#
# What this file tests:
#   Every backward-compat run (see runner.sh -> test_backward_compatibility)
#   sets REPO_DIR to a repository that was originally written by an older
#   dolt binary (per backward_compatible_versions.txt) and then invokes the
#   current build's `dolt` to query it. That is exactly the read path that
#   trips the regression for old-format TEXT/BLOB cells. The tests below
#   make four kinds of assertions on that read path:
#
#     1. The SQL command exits 0 (existing baseline).
#     2. The output does NOT contain "invalid hash length", "panic recovered",
#        or "runtime error" -- the panic shapes errguard converts to soft
#        errors. A test that only checks $status will miss these.
#     3. The output's data matches the expected value (also existing
#        baseline, kept here so the test exercises the full read pipeline
#        rather than letting an empty/garbage payload "pass").
#     4. For the large-value cases (>500B), the same checks run on values
#        large enough to be out-of-band addressed -- the exact storage shape
#        that triggers the regression.
#
# Why this file is separate from types_compatibility.bats:
#   types_compatibility.bats is a broad type-coverage suite. This file is
#   the regression-specific suite: it pins down what "no adaptive encoding
#   panic" means in test terms so a future refactor that re-introduces the
#   bug (or a related one in the same dispatch heuristic) trips the test
#   here loudly and obviously, regardless of whether a maintainer ever
#   touches the broader types_compatibility.bats file.
#
# Expected behavior:
#   - FAILS against unpatched dolt v2.0.7 (panic shapes leak into output).
#   - PASSES against the patched binary on
#     fix/adaptive-encoding-invalid-hash-length and later builds.
#
# Skipped on:
#   - Backward-compat runs where the old binary post-dates the
#     StringAddrEnc/BytesAddrEnc -> adaptive-encoding cutover. There is
#     nothing to regress against in that case.

bats_load_library common.bash
bats_load_library compat-common.bash

setup() {
    setup_common
    cp -Rpf $REPO_DIR bats_repo
    cd bats_repo
}

teardown() {
    teardown_common
    cd ..
    rm -rf bats_repo
}

# ---------------------------------------------------------------------------
# Read-path panic-shape regression tests. Each test selects a TEXT or BLOB
# column written by the old dolt version and asserts that the current
# build's reader returns the expected value WITHOUT any panic marker
# leaking into the captured output.
# ---------------------------------------------------------------------------

@test "adaptive_legacy: small TEXT column read does not panic with invalid hash length" {
    # Small TEXT cells may be inline or out-of-band depending on prolly tuple
    # layout decisions in the writer's era. Both cases must read clean.
    run dolt sql -q "SELECT pk, c_text FROM all_types WHERE pk=1;" -r csv
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "${lines[1]}" =~ "1,text val" ]] || false
}

@test "adaptive_legacy: large TEXT column read does not panic with invalid hash length" {
    # Row pk=3 stores REPEAT('t', 500) -- comfortably above the inline
    # threshold for older writers, so the cell is stored as a 20-byte raw
    # hash address. This is the exact shape that tripped the regression.
    run dolt sql -q "SELECT pk, LENGTH(c_text) FROM all_types WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "${lines[1]}" =~ "3,500" ]] || false
}

@test "adaptive_legacy: large MEDIUMTEXT column read does not panic" {
    run dolt sql -q "SELECT pk, LENGTH(c_mediumtext) FROM all_types WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "${lines[1]}" =~ "3,500" ]] || false
}

@test "adaptive_legacy: large LONGTEXT column read does not panic" {
    run dolt sql -q "SELECT pk, LENGTH(c_longtext) FROM all_types WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "${lines[1]}" =~ "3,500" ]] || false
}

@test "adaptive_legacy: large BLOB column read does not panic" {
    # BytesAddrEnc shares the dispatch path with StringAddrEnc -- the same
    # legacy raw-hash misinterpretation hits BLOB columns symmetrically.
    run dolt sql -q "SELECT pk, LENGTH(c_blob) FROM all_types WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "${lines[1]}" =~ "3,500" ]] || false
}

@test "adaptive_legacy: large MEDIUMBLOB column read does not panic" {
    run dolt sql -q "SELECT pk, LENGTH(c_mediumblob) FROM all_types WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "${lines[1]}" =~ "3,500" ]] || false
}

@test "adaptive_legacy: large LONGBLOB column read does not panic" {
    run dolt sql -q "SELECT pk, LENGTH(c_longblob) FROM all_types WHERE pk=3;" -r csv
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "${lines[1]}" =~ "3,500" ]] || false
}

@test "adaptive_legacy: bulk scan over all_types reads every TEXT/BLOB row clean" {
    # A SELECT * scan exercises every TEXT/BLOB column on every row in a
    # single read pass. If any one cell trips the adaptive dispatch the
    # panic marker shows up in $output and assert_no_panic_shape fails.
    run dolt sql -q "SELECT pk, LENGTH(c_text), LENGTH(c_longtext), LENGTH(c_blob), LENGTH(c_longblob) FROM all_types ORDER BY pk;" -r csv
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    # Three rows seeded by setup_repo.sh; header + 3 = 4 lines minimum.
    [ "${#lines[@]}" -ge 4 ]
}

@test "adaptive_legacy: dolt diff over old TEXT column does not panic" {
    # diff invokes the comparator path through the adaptive value
    # machinery -- a separate entry into the dispatch heuristic than a
    # plain SELECT. Verify it also lands on a clean read.
    dolt sql -q "INSERT INTO all_types (pk, c_text, c_blob) VALUES (4242, 'diff test text', 'diff test blob');"
    run dolt diff
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "$output" =~ "4242" ]] || false
}
