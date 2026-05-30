#!/usr/bin/env bats
# Regression suite for the repair-orphans-content incident (ignored_log).
#
# Background:
#   `dolt admin schema-encoding-drift repair` flips a column's persisted
#   encoding from its adaptive variant back to the legacy sibling WITHOUT
#   rewriting row data. On a column that still holds real adaptive content this
#   STRANDS every adaptive row under a legacy (StringAddr/BytesAddr) schema:
#   reads then panic `invalid hash length: N` and any insert's node flush panics
#   in countAddresses. That is exactly what happened to ignored_log.{old,new}
#   _value when the affected deploy ran `repair` (schema-only flip) instead of
#   the row-rewriting heal.
#
# Scope note (important):
#   The EXACT stranded state — a legacy raw-hash witness coexisting with
#   adaptive-INLINE rows under a flipped-to-legacy schema — is only producible by
#   the historical buggy v2.0.7 ALTER MODIFY path. The current (fixed) binary
#   preserves encodings across ALTER, so that drift cannot be reconstructed
#   through the CLI alone. The precise inline-heterogeneous
#   repair-flip-then-panic is covered with full encoding control by the Go suite:
#     go/cmd/dolt/commands/admin/schemadrift/repair_inline_heterogeneous_test.go
#
#   This bats suite covers the two CLI behaviors that ARE reproducible on the
#   fixed binary and that guard the same failure class:
#     (a) `migrate-adaptive` forward-heals a legacy TEXT column to readable
#         v2-native adaptive with no panic shape and byte-identical content.
#     (b) `repair` REFUSES a column that holds genuine content rather than
#         flipping it (the schema-only flip that stranded real-world), and does
#         so without leaking a panic shape.
#
# Expected behavior:
#   - (a) FAILS-WITHOUT the `migrate-adaptive` command (unknown
#     subcommand -> non-zero), PASSES-WITH it.
#   - (b) the refusal is the safety guard that prevents re-stranding; it must
#     never silently flip a content-bearing column.

bats_load_library common.bash
bats_load_library compat-common.bash

setup() {
    # Offline, single-process repo (no remote server) so the per-command
    # DOLT_USE_ADAPTIVE_ENCODING toggle actually governs schema construction.
    setup_no_dolt_init
    dolt init
}

teardown() {
    teardown_common
}

# seed_legacy_table builds a table whose TEXT column is persisted in legacy
# StringAddrEnc form by forcing the writer off adaptive encoding for the
# CREATE + INSERT. The leading VARCHAR mirrors ignored_log (the target column
# sits after a variable-width sibling).
seed_legacy_table() {
    DOLT_USE_ADAPTIVE_ENCODING=false dolt sql <<'SQL'
CREATE TABLE t (
  id INT PRIMARY KEY,
  actor VARCHAR(255) NOT NULL,
  body LONGTEXT NOT NULL
);
INSERT INTO t VALUES
  (1, 'a-1', 'example inline content payload'),
  (2, 'a-2', REPEAT('x', 500));
SQL
}

@test "migrate-adaptive: heals a legacy TEXT column to readable adaptive (no panic shape)" {
    seed_legacy_table

    run dolt admin schema-encoding-drift migrate-adaptive --table t --column body
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"

    # Every row reads back clean and byte-identical after the forward heal.
    run dolt sql -q "SELECT id, body FROM t WHERE id=1;" -r csv
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "${lines[1]}" =~ "example inline content payload" ]] || false

    run dolt sql -q "SELECT id, LENGTH(body) FROM t WHERE id=2;" -r csv
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "${lines[1]}" =~ "2,500" ]] || false
}

@test "migrate-adaptive: bulk scan after heal reads every row clean" {
    seed_legacy_table
    dolt admin schema-encoding-drift migrate-adaptive --table t --column body

    run dolt sql -q "SELECT id, LENGTH(body) FROM t ORDER BY id;" -r csv
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [ "${#lines[@]}" -ge 3 ]
}

@test "migrate-adaptive: is idempotent on an already-migrated column" {
    seed_legacy_table
    dolt admin schema-encoding-drift migrate-adaptive --table t --column body

    run dolt admin schema-encoding-drift migrate-adaptive --table t --column body
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
}

@test "migrate-adaptive: force-inlines a dolt_ignore'd table (storage report: 0 addressed)" {
    # CLI-level analog of the Go FieldAdaptiveAddressed invariant
    # (TestMigrateAdaptive_DoltIgnoredTable_ForceInlineInvariant). dolt_ignore'd
    # tables (`ignored_%`) have never-committed content chunks; the force-inline mode force-
    # INLINES their values so ZERO out-of-line (addressed) refs remain — which is
    # what lets the working set persist without a dangling ref. migrate-adaptive
    # emits a deterministic storage report:
    #   "storage: N values inline, M addressed (M MUST be 0 for a dolt_ignore'd table)"
    # We assert M == 0 directly (deterministic — supersedes the earlier gc-proxy).
    # FAILS-WITHOUT the force-inline mode (a >2KB value migrates to adaptive-ADDRESSED -> M>0);
    # PASSES-WITH it. NB: byte-exact inline check is the Go test above; the
    # authoritative persist check is the real-data deploy gate (see the validation notes), not a fresh-fixture test. On an ignored
    # table migrate-adaptive force-inlines the whole table's columns via a
    # working-set persist (no dolt commit), so we assert on output + read-back.
    dolt sql -q "INSERT INTO dolt_ignore VALUES ('ignored_%', true);"
    DOLT_USE_ADAPTIVE_ENCODING=false dolt sql <<'SQL'
CREATE TABLE ignored_log (
  id INT PRIMARY KEY,
  actor VARCHAR(255) NOT NULL,
  body LONGTEXT NOT NULL
);
INSERT INTO ignored_log VALUES (1, 'a-1', REPEAT('z', 3000));
SQL
    # >2KB value: without force-inline this migrates to adaptive-ADDRESSED.
    run dolt admin schema-encoding-drift migrate-adaptive --table ignored_log --column body
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"

    # Force-inline INVARIANT: the storage report must exist and show 0 addressed.
    echo "$output" | grep -qE "storage:.*values inline" || { echo "no storage report in output: $output" >&2; false; }
    addressed=$(echo "$output" | grep -oE "[0-9]+ addressed" | grep -oE "^[0-9]+" | head -1)
    [ -n "$addressed" ] || { echo "could not parse 'addressed' count from: $output" >&2; false; }
    [ "$addressed" -eq 0 ] || { echo "force-inline invariant VIOLATED: $addressed addressed (must be 0): $output" >&2; false; }

    # And content reads back clean (force-inlined value is self-contained).
    run dolt sql -q "SELECT id, LENGTH(body) FROM ignored_log WHERE id=1;" -r csv
    [ "$status" -eq 0 ]
    assert_no_panic_shape "$output"
    [[ "${lines[1]}" =~ "1,3000" ]] || false
}

@test "repair: refuses to flip a content-bearing column (no silent stranding)" {
    # A column written with genuine adaptive content must NOT be flipped to
    # legacy by repair — the schema-only flip is exactly what stranded
    # ignored_log. repair must refuse with a clear, non-panicking error.
    dolt sql <<'SQL'
CREATE TABLE u (
  id INT PRIMARY KEY,
  actor VARCHAR(255) NOT NULL,
  body LONGTEXT NOT NULL
);
INSERT INTO u VALUES
  (1, 'a-1', 'genuine adaptive content row'),
  (2, 'a-2', REPEAT('y', 4000));
SQL
    run dolt admin schema-encoding-drift repair --table u --column body
    [ "$status" -ne 0 ]
    assert_no_panic_shape "$output"
}
