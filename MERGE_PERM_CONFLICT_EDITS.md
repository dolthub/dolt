# Merge-Permission Conflict Edits

## Goal

Let a user who holds only `Permissions_Merge` on a branch resolve a merge that produced **data conflicts** by editing the conflicted rows directly, without granting full `Permissions_Write`. This unblocks the web SQL workbench PR flow: a reviewer can complete a merge that hits conflicts without being given general write access to the target branch.

**Scope: data conflicts only.** Schema conflicts (`dolt_schema_conflicts`) and constraint violations (`dolt_constraint_violations_*`) are out of scope for this change and continue to require `Permissions_Write`.

## Current state

- `go/libraries/doltcore/branch_control/access.go:32-37` defines `Admin ⊃ Write ⊃ Merge ⊃ Read`. `Permissions_Merge` allows `DOLT_MERGE` and `DOLT_COMMIT` but not ordinary `INSERT/UPDATE/DELETE`.
- `go/libraries/doltcore/sqle/tables.go` (~L714 / L973 / L1110): the `Inserter`, `Deleter`, and `Updater` *factory* methods each call `dsess.CheckAccessForDb(ctx, db, Permissions_Write)` **once per statement** to construct the row writer. The per-row hot path is on the returned writer (`go/libraries/doltcore/sqle/writer/prolly_table_writer.go:149,180,207`) and currently has no permission check.
- `dsess.CheckAccessForDb` (`go/libraries/doltcore/sqle/dsess/branch_control.go:40`) is an RWMutex-protected `Match` against a small trie of `MatchExpression` rules (`branch_control/access.go:75-76`). Cost is O(rules), uncached, statement-level — already negligible.
- `go/libraries/doltcore/sqle/dprocedures/dolt_conflicts_resolve.go:378`: `DoDoltConflictsResolve` requires `Permissions_Write`, so a merge-only user cannot run `CALL DOLT_CONFLICTS_RESOLVE` today.
- After a merge with conflicts, `dolt_conflicts_<table>` is populated and `working_set.MergeActive()` is true.
- Conflicts are stored as one prolly artifact map per table, keyed on PK + rootIsh + artType + violationInfoHash (`go/store/prolly/artifact_map.go:66-72`). `ArtifactMap.Has(ctx, key)` is the natural membership API (line 74-76); cost is O(log N) prolly descent with high branching factor (~3-4 node fetches even at 10k conflicts).
- There is no server-side "PR mode" — conflict editing is plain SQL writes.
- Existing coverage in `go/libraries/doltcore/sqle/enginetest/branch_control_test.go:1450+` asserts the current restrictive behavior (line 1678 confirms merge-only users cannot resolve conflicts).

## Plan

### 1. Lock down semantics

Rule:

> While `working_set.MergeActive()` is true on a branch where the caller has `Permissions_Merge` (but not `Write`), allow row mutations **iff** the row's primary key is present in `dolt_conflicts_<table>`. Also allow `CALL DOLT_CONFLICTS_RESOLVE(...)`, `DELETE FROM dolt_conflicts_<table>`, and `CALL DOLT_COMMIT(...)`. Block everything else.

Decisions to confirm before coding:

- **Schema conflicts** — out of scope. Editing during a schema conflict still requires `Write`.
- **Constraint violations** — out of scope. Same as today.
- **`DOLT_MERGE --abort`** — recommend allowing under `Merge` (reverses the user's own merge).
- **Re-editing a row after its conflict row is deleted** — disallowed. Once resolved, the row is no longer "in conflict" and the merge-only user loses edit rights to it.
- **`INSERT` of a new PK** — disallowed in v1. Only `UPDATE` and `DELETE` of existing conflicted rows are supported. `DOLT_CONFLICTS_RESOLVE --theirs` covers the "accept incoming row" case.

### 2. Add a capability helper

In `go/libraries/doltcore/branch_control/`, add:

```go
func CheckMergeConflictEditAccess(ctx, db, table, key) bool
```

True iff:
1. Caller has at least `Permissions_Merge` on the branch,
2. The working set has an active merge,
3. `key` is present in the data-conflicts artifact map for `table` on the current working root.

Keep it adjacent to `CheckAccess` / `CheckAccessForDb` so all permission logic lives in one place.

### 3. Soften the writer gates — split into a statement-level admit and a per-row check

The existing `CheckAccessForDb(..., Permissions_Write)` in `tables.go` runs once per statement at writer construction. The new check has to be per row, so the change has two parts:

**Statement-level admit** (in `tables.go` `Inserter`/`Updater`/`Deleter`):

- If `Permissions_Write` passes → construct the writer as today, no row-level checking.
- Else if `Permissions_Merge` passes AND `ws.MergeActive()` AND the table has data conflicts → construct the writer in **conflict-edit mode** (a flag on the writer / `WriterState`).
- Else → return the same access-denied error as today.

**Per-row check** (in `writer/prolly_table_writer.go` `Insert`/`Update`/`Delete`, ~L149/L180/L207):

- If the writer is not in conflict-edit mode → proceed unchanged.
- Else → look up the affected PK in the table's conflicts artifact map; permit on hit, deny on miss.

Per-operation specifics:

- `Update`: check against the **old** row's PK.
- `Delete`: check against the row's PK.
- `Insert`: v1 — always deny in conflict-edit mode. Revisit if a concrete use case requires it (`DOLT_CONFLICTS_RESOLVE --theirs` covers the "accept incoming row" case).

### 4. Allow conflict-table mutations under merge permission

- **`dolt_conflicts_<table>` writers** (`go/libraries/doltcore/sqle/dtables/conflicts_tables_prolly.go`): permit `DELETE` under `Permissions_Merge` when `MergeActive()`. Other operations on this table remain gated by `Write`.
- **`DoDoltConflictsResolve`** (`go/libraries/doltcore/sqle/dprocedures/dolt_conflicts_resolve.go:378`): relax from `Permissions_Write` to "Write OR (Merge AND MergeActive)".

### 5. Surrounding operations — keep coherent

Audit and confirm behavior for a merge-only user with `MergeActive()`:

| Operation | Allowed? |
|---|---|
| `UPDATE`/`DELETE` on a conflicted row | yes (new) |
| `UPDATE`/`DELETE` on a non-conflicted row | no |
| `UPDATE`/`DELETE` on a table with no conflicts | no |
| `INSERT` (any) | no (v1) |
| `DELETE FROM dolt_conflicts_<t>` | yes (new) |
| `CALL DOLT_CONFLICTS_RESOLVE` | yes (new) |
| `CALL DOLT_COMMIT` | yes (unchanged) |
| `CALL DOLT_MERGE --abort` | yes (new, per step 1) |
| Schema changes (`ALTER`, `CREATE`, `DROP TABLE`) | no |
| Resolving a schema conflict | no (out of scope) |
| Resolving a constraint violation | no (out of scope) |
| `DOLT_RESET`, `DOLT_REVERT`, `DOLT_CHECKOUT <other>` | no |

After the merge is committed (no more `MergeActive`), the merge-only user loses all edit rights again.

### 6. Tests

Extend `go/libraries/doltcore/sqle/enginetest/branch_control_test.go`:

- Merge-only user runs `DOLT_MERGE`, gets data conflicts, **can** `UPDATE` a conflicted row.
- Same user **cannot** `UPDATE` a non-conflicted row in the same table.
- Same user **cannot** `UPDATE` a row in a table with no conflicts.
- Same user **can** `DELETE FROM dolt_conflicts_<t>` and `CALL DOLT_CONFLICTS_RESOLVE('--ours', 't')`.
- Same user **can** `CALL DOLT_COMMIT(...)` to finalize.
- After commit, the same user **cannot** edit the previously conflicted row.
- Negative: same user **cannot** `ALTER TABLE`, `DROP TABLE`, `CREATE TABLE`, or `INSERT` a never-conflicted PK.
- Negative: schema conflicts and constraint violations still require `Write` to resolve.

Add an end-to-end bats test under `integration-tests/bats/branch-control.bats`.

### 7. Rollout

- Document the new behavior in branch control docs and in the workbench PR docs.
- Coordinate with the workbench team — they may want to surface "you can only edit conflicted rows" in the UI. The existing `dolt_merge_status` table is the right server signal; no new table needed.
- Note the `DOLT_CONFLICTS_RESOLVE` permission relaxation in the changelog. It is strictly a loosening, so no caller breaks.

## Performance

The proposed check is the first per-row permission gate on the write path, so it deserves a real cost model.

**Existing baseline.** Today's `Permissions_Write` check runs once per statement when the SQL engine asks for the row writer (`tables.go` `Inserter`/`Updater`/`Deleter` factories). It is an RWMutex-guarded trie match against a small ruleset (`branch_control/access.go:75-76`) — effectively free. The per-row methods on the returned writer (`prolly_table_writer.go:149,180,207`) currently do **no** permission work, so any check we add there is strictly new cost.

**New per-row cost.** For a writer in conflict-edit mode:

1. Branch on a boolean field (free).
2. Encode the affected PK into the artifact-map key form (`srcKey || rootIsh || artType || violationInfoHash`-prefix). For a "does any conflict exist for this PK?" question we want a prefix scan, not an exact `Has`, since the trailing `violationInfoHash` varies per conflict — confirm the API; if no prefix-existence helper exists, add one in `store/prolly/artifact_map.go`.
3. Descend the conflicts prolly tree: O(log N) with branching factor ~256, so ~3-4 node fetches even at 10k conflicts in a table. Nodes hot in the chunk cache are in-memory map hits.

In practice this is in the same order of magnitude as one secondary-index touch, of which a normal `UPDATE` already does several. **The check is not free, but it is dominated by costs the write path already pays.**

**Cache plan.** Cache the conflicted PK set per table on `WriterState`, keyed by `(tableName, root.Hash())` — same pattern as `SessionCache`'s `DataCacheKey`-based entries (`dsess/session_cache.go:27-48`). Invalidation is automatic: any mutation produces a new root hash, so the old entry becomes unreachable rather than stale. Two open implementation choices:

- **Eager materialization** (small/medium conflict sets): on first access, walk the artifact map and materialize a `map[hash]struct{}` of PK hashes. Subsequent checks are O(1).
- **Lazy with prolly cursor reuse** (large conflict sets): keep the prolly tree open and let the chunk cache do its job — accept O(log N) per check. Simpler, no upfront cost.

Recommend starting with eager materialization gated on conflict count (e.g., <50k); fall through to lazy lookup above that. The threshold can be tuned with a benchmark.

**Worst-case scale.** No published benchmarks for large conflict sets exist in the tree, and `merge_stats.go` treats `DataConflicts` as a simple counter — the implicit expectation is hundreds to low thousands. A pathological 100k-conflict merge would still be fine with lazy lookups (~5 node fetches per row). Add a benchmark covering 10k and 100k conflicts before merging.

**Early-exit ordering.** Hot path should short-circuit aggressively:

1. Writer not in conflict-edit mode → return immediately. (The common case: user has `Write`.)
2. Statement-cached "is merge active?" flag → if false, return immediately. (Cannot happen given step 1, but cheap defense.)
3. Then do the artifact lookup.

This keeps the regression on `Write`-permission write paths to a single predictable branch.

## Risks

- **Per-row check cost on conflict-edit writers.** Covered above. The mitigation is the session-scoped cache and the eager-vs-lazy threshold; the residual risk is a pathological conflict-set size, which is bounded by the merge algorithm's own cost.
- **Cache invalidation if conflicts mutate mid-statement.** If a `DELETE FROM dolt_conflicts_<t>` and a row `UPDATE` interleave within one statement (unlikely, but possible via multi-statement transactions), the cached keyset can lag. Root-hash keying handles cross-statement cases; within a statement, the writer's `WriterState` should refresh its cache on any mutation it performs to the conflicts table.
- **API gap on the artifact map.** If the prolly artifact map exposes only exact-key `Has` (full key including `violationInfoHash`), we need a `HasPKConflict(tableKey)` helper that does a prefix existence check. Confirm during implementation; this is a small store-level addition, not a redesign.
- **Coherence with future conflict types.** This change introduces a permission path keyed on the data-conflicts artifact map. If schema-conflict or constraint-violation resolution is later brought into the same model, the helper in step 2 should be extended rather than duplicated.
