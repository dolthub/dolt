# Merge-Permission Writes via `dolt_conflicts_*` Tables

## Goal

Let a user who holds only `Permissions_Merge` on a branch resolve a merge that produced data conflicts **by writing through the `dolt_conflicts_<table>` system tables only**. This unblocks the web SQL workbench PR flow: a reviewer can complete a merge that hits conflicts without being given general write access to the target branch.

**Scope:**

- In scope: `UPDATE` and `DELETE` on `dolt_conflicts_<table>` rows, plus `CALL DOLT_CONFLICTS_RESOLVE(...)`, while a merge is active.
- Out of scope: direct writes to underlying user tables, `INSERT` on `dolt_conflicts_<table>`, schema-conflict resolution, constraint-violation resolution. All of these continue to require `Permissions_Write`.

## How the conflicts table writes work today

The UPDATE and DELETE paths are **asymmetric** in their current permission behavior, and the plan has to handle each differently.

**UPDATE on `dolt_conflicts_<t>` — gated by source-table `Write`:**

- `ProllyConflictsTable.Updater` (`go/libraries/doltcore/sqle/dtables/conflicts_tables_prolly.go:124`) returns a `prollyConflictOurTableUpdater`.
- `newProllyConflictOurTableUpdater` (line 554-561) **eagerly constructs the source-table updater**: `ourUpdater := ct.sqlTable.Updater(ctx)` (line 555). That call goes through the source table's `Updater` factory in `tables.go` (~L1110), which runs `CheckAccessForDb(..., Permissions_Write)` at construction time.
- The `Update` method (line 564-577) then delegates row writes to `cu.srcUpdater.Update`.
- Net: today an UPDATE on `dolt_conflicts_<t>` requires `Permissions_Write` on the underlying source table.

**DELETE on `dolt_conflicts_<t>` — ungated by branch control (verified, pre-existing bug):**

- `ProllyConflictsTable.Deleter` (line 128) returns a `prollyConflictDeleter`. No permission check.
- `newProllyConflictDeleter` (line 614-640) does **not** construct any source-table writer. It opens an `ArtifactsEditor` directly: `ed := ct.artM.Editor()` (line 616) and writes only to the conflicts artifact map. No permission check.
- The `Delete` method (line 644) operates on `cd.ed` and tuple builders only. No permission check.
- Higher-up table resolution (`go/libraries/doltcore/sqle/database.go:551-582`, `getTableInsensitiveWithRoot` → `dtables.NewConflictsTable` at line 578) returns the conflicts table unwrapped; no analyzer-level or session-level interception fires.
- Of the 31 `CheckAccessForDb` callsites in the sqle package, **none** are on the conflicts-table DELETE path.
- Compare to `WritableDoltTable.Deleter` (`tables.go:973-981`) which gates on `Permissions_Write` via `sqlutil.NewStaticErrorEditor`. The conflicts-table `Deleter` was implemented without the corresponding check.

Net: today a `Permissions_Read`-only user can `DELETE FROM dolt_conflicts_<t>` and it succeeds. This is a pre-existing permission gap that the new feature will incidentally close.

Consequences for the implementation:

- For UPDATE, gate at the conflicts-table factory **and** bypass the inner source-table `Write` check via a tightly-scoped context marker (the delegation chain still has to run).
- For DELETE, **add** an explicit gate at the conflicts-table factory. No bypass is needed because the deleter never touches the source-table writer. Pattern after `WritableDoltTable.Deleter`'s use of `sqlutil.NewStaticErrorEditor`.

## Current state

- `go/libraries/doltcore/branch_control/access.go:32-37` defines `Admin ⊃ Write ⊃ Merge ⊃ Read`. `Permissions_Merge` allows `DOLT_MERGE` and `DOLT_COMMIT` but not ordinary `INSERT/UPDATE/DELETE`.
- `go/libraries/doltcore/sqle/tables.go` (~L714 / L973 / L1110): the underlying-table `Inserter`/`Updater`/`Deleter` factories each call `dsess.CheckAccessForDb(ctx, db, Permissions_Write)` once per statement.
- `dsess.CheckAccessForDb` (`dsess/branch_control.go:40`) is a cheap RWMutex-guarded match against a small ruleset.
- `go/libraries/doltcore/sqle/dprocedures/dolt_conflicts_resolve.go:378`: `DoDoltConflictsResolve` requires `Permissions_Write` today.
- `ProllyConflictsTable` (`dtables/conflicts_tables_prolly.go`) does **not** perform any branch-control check of its own. UPDATE inherits the source-table `Write` check via eager `srcUpdater` construction; DELETE has *no* branch-control gate today (verified — see "How the conflicts table writes work today" above). This means a `Permissions_Read`-only user can already `DELETE FROM dolt_conflicts_<t>` — a pre-existing bug the new feature will close.
- After a merge with conflicts, `working_set.MergeActive()` is true and `dolt_conflicts_<table>` is populated.
- `enginetest/branch_control_test.go:1450+` covers merge-permission semantics; line 1678 asserts merge-only users cannot resolve conflicts today.

## Plan

### 1. Lock down semantics

Rule for a caller with `Permissions_Merge` but not `Permissions_Write`, while `working_set.MergeActive()` is true on the target branch:

| Operation | Allowed? |
|---|---|
| `UPDATE dolt_conflicts_<t> SET our_<col> = ...` | yes (new) |
| `DELETE FROM dolt_conflicts_<t> WHERE ...` | yes (new) |
| `INSERT INTO dolt_conflicts_<t> ...` | no |
| `CALL DOLT_CONFLICTS_RESOLVE('--ours' | '--theirs', 't')` | yes (new) |
| `CALL DOLT_COMMIT(...)` | yes (unchanged) |
| `CALL DOLT_MERGE('--abort')` | yes — recommend; confirm with team |
| `UPDATE`/`INSERT`/`DELETE` on underlying `<t>` directly | no |
| Schema changes, table create/drop, other branch ops | no |
| Schema-conflict or constraint-violation resolution | no (still requires `Write`) |

After the merge is committed or aborted (no more `MergeActive`), the merge-only user loses these rights.

### 2. Gate the conflicts-table writers explicitly

Add an explicit `dsess.CheckAccessForDb` call at both `ProllyConflictsTable.Updater` and `ProllyConflictsTable.Deleter` (`dtables/conflicts_tables_prolly.go:124,128`):

- Permit if `Permissions_Write`, **or** (`Permissions_Merge` AND `ws.MergeActive()` AND the conflicts artifact map for this table is non-empty).
- Otherwise return the existing access-denied error.

For the **Updater**, this becomes the authoritative gate (replacing the inherited source-table check, which step 3 then neutralizes for this code path). For the **Deleter**, this is the *first* branch-control check on the path — strictly a tightening relative to today's behavior, where any session can DELETE from a conflicts table (see "How the conflicts table writes work today"). Use the same `sqlutil.NewStaticErrorEditor` pattern as `WritableDoltTable.Deleter` (`tables.go:973-975`) so a denied caller gets a clean error at execution time.

### 3. Bypass the source-table `Write` check for the UPDATE delegation chain

This step applies only to the UPDATE path. The DELETE path does not touch the source-table writer and needs no bypass.

`prollyConflictOurTableUpdater` captures `srcUpdater` eagerly at line 555, and the source-table `Updater` factory runs `CheckAccessForDb(..., Permissions_Write)` at that moment. A merge-only caller will be rejected before any row write is attempted.

Add a context-scoped marker — a flag on `*sql.Context` private storage or on the `DoltSession`'s per-statement state — that the conflicts-table writer sets before constructing the source-table updater and clears on `Close`. In the source-table `Updater` factory, treat the marker as a bypass **only when** the caller also satisfies the merge-conflict admission rule from step 2. The bypass does not grant general write access; it propagates an already-validated decision through the delegation chain.

Concretely:

- Set marker at the top of `newProllyConflictOurTableUpdater` (line 554), before line 555's `ct.sqlTable.Updater(ctx)` call. Clear it in `prollyConflictOurTableUpdater.Close` (line 596).
- The source-table `Updater` factory (`tables.go` ~L1110) reads the marker; if present and the merge-conflict condition holds, skip the `Write` check.
- Marker is keyed by `(db, branch, table)` so it cannot be misused to bypass writes on a *different* table during the same statement.
- Marker lifetime must cover the entire statement, not just construction — `StatementBegin`/`StatementComplete` on `prollyConflictOurTableUpdater` already delegate to `srcUpdater` (lines 581-592), but any per-row revalidation inside the source-table writer needs the marker still set. Belt-and-suspenders: set in `newProllyConflictOurTableUpdater`, refresh in `StatementBegin`, clear in `Close`.

### 4. Loosen `DoDoltConflictsResolve`

`dprocedures/dolt_conflicts_resolve.go:378`: relax the check from `Permissions_Write` to "Write OR (Merge AND MergeActive)". The procedure writes through the same conflicts machinery, so the bypass from step 3 covers any delegated underlying-table writes.

### 5. Surrounding operations — keep coherent

Audit the merge-only-during-merge-active state for:

- `DOLT_COMMIT` — already allowed; ensure it finalizes the merge correctly.
- `DOLT_MERGE --abort` — decide per step 1.
- `DOLT_RESET`, `DOLT_REVERT`, `DOLT_CHECKOUT <other>`, table drops/creates — must remain blocked.
- Schema changes during the merge — must remain blocked.
- Writes through the conflicts table for a table that has **no** conflicts in the artifact map — block (the artifact-map-non-empty condition in step 2 handles this).

### 6. Tests

Extend `go/libraries/doltcore/sqle/enginetest/branch_control_test.go`:

- Merge-only user, `MergeActive`, **can** `UPDATE dolt_conflicts_<t>` and the change appears in `<t>`.
- Same user **can** `DELETE FROM dolt_conflicts_<t>`.
- Same user **can** `CALL DOLT_CONFLICTS_RESOLVE('--ours', 't')` and `('--theirs', 't')`.
- Same user **can** `CALL DOLT_COMMIT(...)` after resolution.
- Same user **cannot** `UPDATE` or `DELETE` on `<t>` directly.
- Same user **cannot** `INSERT INTO dolt_conflicts_<t>`.
- Same user **cannot** write to `dolt_conflicts_<t>` for a table with no current conflicts.
- After commit (no more `MergeActive`), same user **cannot** write to `dolt_conflicts_<t>`.
- Negative: bypass marker does not allow writes on a different table — set up the marker for `t1`, attempt write on `t2`, confirm rejection.
- Negative: schema conflicts and constraint violations still require `Write`.
- **Tightening regression**: `Permissions_Read`-only user (no merge in flight) now *cannot* `DELETE FROM dolt_conflicts_<t>`. Today this succeeds — the test would fail against `main` and pass against the new code, confirming the bug is closed.

Add a bats integration test under `integration-tests/bats/branch-control.bats` for the end-to-end flow.

### 7. Rollout

- Document the new behavior in branch control docs and in the workbench PR docs.
- Coordinate with the workbench team: surface "edit via `dolt_conflicts_<t>` only" in the UI, using the existing `dolt_merge_status` table as the server signal.
- Note the `DOLT_CONFLICTS_RESOLVE` permission relaxation in the changelog (strictly a loosening, no breakage).
- Note that `DELETE FROM dolt_conflicts_<t>` now requires `Permissions_Write` or `Permissions_Merge` (during an active merge). Today the check is missing entirely — this is a security fix as well as a feature. Unlikely to break real callers since branch_control configurations grant Read-only users for read access, not for conflicts cleanup, but worth calling out.

## Performance

No per-row check is added. The new gating lives at two existing statement-level factory sites (`ProllyConflictsTable.Updater`/`Deleter`) and at the entry of `DoDoltConflictsResolve`. The bypass marker is a single context-private read on the underlying-table factory path, in the same statement-level scope.

Net cost on the `Permissions_Write` hot path is a single additional context-private lookup at writer construction — negligible. No per-row work, no caches, no prolly lookups added.

## Risks

- **Bypass-marker scope discipline.** The marker must be tightly scoped (one specific table on one specific branch, lifetime bounded by a single delegated UPDATE statement). A leaky implementation could allow writes outside the intended scope. Mitigation: keying by `(db, branch, table)`, set/clear in `newProllyConflictOurTableUpdater` and `Close`, plus an explicit negative test (step 6).
- **`prollyConflictDeleter` write paths.** Even though the deleter file-locally does not touch the source-table writer, audit `Delete`, `putPrimaryKeys`, `putKeylessHash` (`conflicts_tables_prolly.go:644,674,696`) and any helpers they invoke to confirm no source-table write happens out-of-line. If any does, it needs the same bypass-marker treatment as the updater.
- **Coherence with future conflict types.** Schema and constraint-violation resolution remain out of scope. If they are later brought into the same model, they should reuse the gating helper and the bypass-marker pattern rather than duplicating them.

## Effort estimate

Engineer-days for one Dolt-familiar engineer.

| Step | Work | Days |
|---|---|---|
| 1 | Semantics lock-down | 0.5 |
| 2 | Gate `ProllyConflictsTable.Updater`/`Deleter` | 0.5 |
| 3 | Bypass marker plumbing + source-table check (UPDATE path only) | 1.0 |
| 4 | Relax `DoDoltConflictsResolve` | 0.25 |
| 5 | Surrounding-ops audit | 0.5 |
| 6 | Tests (enginetest + bats), incl. tightening regression | 1.25 |
| 7 | Docs, changelog, workbench coordination | 0.5 |
| | **Subtotal** | **4.5** |
| | Risk buffer (deleter audit, review cycles) | +1.5–2.5 |
| | **Total** | **6–7 engineer-days** |

Calendar time: ~1.5 weeks for one engineer end-to-end.

### If Claude Code drives the implementation

Active coding compresses to roughly **4–6 hours** of session time. The same caveats from the prior plan apply (user decisions, test wall-clock, human review). End-to-end with an active reviewer: **~0.5–1 working day to "passing local tests, ready for review"**, **2–4 calendar days to merged**.
