// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schemadrift

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/mohae/uvarint"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

// RecoverRowsCmd is the heterogeneous-payload sibling of RepairCmd. Where
// repair refuses (correctly) to flip a schema whose rows are a mix of legacy-
// raw-hash and adaptive-formatted bytes, recover-rows rewrites every
// non-canonical row's target field into its canonical legacy form (a bare
// 20-byte content-hash address), then flips the schema record exactly the way
// repair would. The row data and the schema record both update inside a single
// dolt commit so a partial migration can never be observed.
//
// The migration preserves CONTENT — the bytes the user originally inserted are
// readable byte-for-byte before and after — but it changes STORAGE LAYOUT for
// inline-adaptive rows. Specifically:
//
//   - A legacy-raw-hash row (20-byte non-zero leader) is already canonical
//     and is skipped without a chunkstore round-trip.
//   - A 0x00-leading 20-byte row whose hash IS present in the chunkstore is a
//     legacy raw hash with a coincidentally-zero leader (~1/256 hashes); it is
//     canonical and is skipped.
//   - An adaptive-addressed row (varint length + 20 hash bytes, total >= 21)
//     has its trailing 20 bytes extracted — those bytes are already the
//     content address — and replaces the field bytes verbatim. No chunkstore
//     write is needed; the content is already stored out-of-band.
//   - A 0x00-leading row whose hash is absent from the chunkstore, OR a
//     0x00-leading row whose length differs from 20, is a genuine inline
//     adaptive value: its content bytes (v[1:]) are written to the chunkstore
//     via vs.WriteBytes, and the resulting hash becomes the field bytes.
//   - A row whose field bytes cannot be classified into any of the above
//     shapes is fatal — recover-rows aborts the entire migration and reports
//     the affected row's primary-key bytes so the operator can triage manually.
//
// Atomicity: a single dolt commit records both the rewritten prolly map and
// the schema-encoding flip. Readers either see the pre-migration state (all
// reads via adaptive dispatch, panics on legacy bytes) or the post-migration
// state (all reads via legacy dispatch, succeeds on every row). They never
// see an intermediate state where some rows have moved and the schema hasn't.
type RecoverRowsCmd struct{}

var _ cli.Command = RecoverRowsCmd{}

var recoverRowsDocs = cli.CommandDocumentationContent{
	ShortDesc: "Row-by-row migration of heterogeneous adaptive/legacy payload into canonical legacy form",
	LongDesc: `Walks every row in ` + "`--table`" + `, reads the target ` + "`--column`" + `'s field bytes through the correct per-row dispatch (legacy raw-hash, adaptive inline, or adaptive addressed), and rewrites the field as a canonical 20-byte legacy content-hash address. After every row is processed the persisted column encoding is flipped from its adaptive variant back to the matching legacy sibling. Row data and schema update inside a single dolt commit.

This command is the heterogeneous-payload counterpart to ` + "`schema-encoding-drift repair`" + `. Use ` + "`repair`" + ` when every row in the affected column is in the same legacy-raw-hash format. Use ` + "`recover-rows`" + ` when ` + "`check`" + ` reports a column as heterogeneous (a mix of legacy and adaptive rows) — that situation can arise on tables that survived the v2.0.7 ALTER MODIFY corruption AND received subsequent writes through the adaptive encoder.

Content is unchanged — the bytes the user originally inserted are readable byte-for-byte before and after — but inline-adaptive values are promoted to out-of-band storage so every row's field is exactly the same 20-byte hash address shape.

The command aborts the entire migration and reports the offending row's primary-key bytes if any field cannot be classified into a known shape; partial migrations are never committed.

Required flags: ` + "`--table`" + ` and ` + "`--column`" + ` — one column per invocation, to keep each commit a single auditable change.

Exit codes:
  0  migration succeeded (or the column was already at a legacy encoding — idempotent no-op)
  1  migration refused (the column is genuinely adaptive with no legacy rows, or has unknown-shape rows, or the operation failed)`,
	Synopsis: []string{"--table <name> --column <name>"},
}

// Name returns the subcommand name as registered in admin's command list.
func (cmd RecoverRowsCmd) Name() string { return "recover-rows" }

// Description is the short summary shown in `dolt admin` help.
func (cmd RecoverRowsCmd) Description() string {
	return "Row-by-row migration of heterogeneous adaptive/legacy payload into canonical legacy form (atomic prolly rewrite + schema flip + dolt commit)"
}

// RequiresRepo is true: we need an initialized dolt environment to write to.
func (cmd RecoverRowsCmd) RequiresRepo() bool { return true }

// Docs is the full documentation surfaced by `dolt admin schema-encoding-drift recover-rows --help`.
func (cmd RecoverRowsCmd) Docs() *cli.CommandDocumentation {
	return cli.NewCommandDocumentation(recoverRowsDocs, cmd.ArgParser())
}

// ArgParser declares the flag surface.
func (cmd RecoverRowsCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsString(tableFlag, "", "name", "the table to migrate")
	ap.SupportsString(colFlag, "", "name", "the column whose heterogeneous payload should be rewritten to canonical legacy form")
	return ap
}

// Hidden mirrors the rest of the admin commands.
func (cmd RecoverRowsCmd) Hidden() bool { return true }

// Exec is the CLI entry point.
func (cmd RecoverRowsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, _ cli.CliContext) int {
	ap := cmd.ArgParser()
	usage, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, recoverRowsDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, usage)

	tableName, ok := apr.GetValue(tableFlag)
	if !ok || tableName == "" {
		verr := errhand.BuildDError("--table is required").SetPrintUsage().Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}
	colName, ok := apr.GetValue(colFlag)
	if !ok || colName == "" {
		verr := errhand.BuildDError("--column is required").SetPrintUsage().Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	result, err := recoverRowsColumn(ctx, dEnv, tableName, colName)
	if err != nil {
		verr := errhand.BuildDError("recover-rows failed").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	emitRecoverRowsResult(cli.OutStream, result)
	return 0
}

// RecoverRowsOutcome enumerates the three terminal states of a recover-rows
// call:
//
//   - RecoverRowsMigrated    — at least one row was rewritten and the schema
//     was flipped to the matching legacy encoding. Includes the count of
//     rows touched and the resulting dolt commit hash.
//   - RecoverRowsAlreadyOK   — the column is already at a legacy encoding;
//     nothing to do. Returned as a successful no-op so scripted migration
//     loops are idempotent.
//   - RecoverRowsNoLegacyRows — the column has an adaptive schema tag and
//     ONLY genuine adaptive rows (no legacy-raw-hash payload). recover-rows
//     refuses this case for the same reason repair refuses it: the column is
//     internally consistent and migrating would be wasted work that
//     promotes nothing.
type RecoverRowsOutcome int

const (
	RecoverRowsMigrated RecoverRowsOutcome = iota
	RecoverRowsAlreadyOK
	RecoverRowsNoLegacyRows
)

// RecoverRowsResult is the structured outcome of a recover-rows call, used by
// the CLI emitter and by the integration tests.
type RecoverRowsResult struct {
	Outcome            RecoverRowsOutcome
	Database           string
	Table              string
	Column             string
	OldEncoding        val.Encoding
	NewEncoding        val.Encoding
	RowsScanned        int    // total rows iterated
	RowsRewritten      int    // subset of RowsScanned whose target field bytes changed
	CollateralRewrites int    // subset of RowsRewritten where one or more NON-target AddrEnc fields had to be normalized to satisfy the new descriptor — surfaces residual schema-row mismatch left by a prior `repair` invocation on a sibling column
	RowsMerged         int    // keyless only: rewritten rows whose canonical bytes collided with an existing entry and were merged by summing cardinality (duplicate logical rows stored once per format)
	CommitHash         string // empty for RecoverRowsAlreadyOK and RecoverRowsNoLegacyRows
	CommitMessage      string // empty for RecoverRowsAlreadyOK and RecoverRowsNoLegacyRows
}

// recoverRowsColumn is the entire migration pipeline. Broken out so unit /
// integration tests can call it without invoking the CLI wrapper.
func recoverRowsColumn(ctx context.Context, dEnv *env.DoltEnv, tableName, colName string) (RecoverRowsResult, error) {
	ddb := dEnv.DoltDB(ctx)
	dbName := ddb.GetDatabaseName()

	rsr := dEnv.RepoStateReader()
	headRef, err := rsr.CWBHeadRef(ctx)
	if err != nil {
		return RecoverRowsResult{}, fmt.Errorf("get current branch: %w", err)
	}

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return RecoverRowsResult{}, fmt.Errorf("get roots: %w", err)
	}

	tn := doltdb.TableName{Name: tableName, Schema: doltdb.DefaultSchemaName}
	tbl, resolvedName, ok, err := doltdb.GetTableInsensitive(ctx, roots.Working, tn)
	if err != nil {
		return RecoverRowsResult{}, fmt.Errorf("get table %s: %w", tableName, err)
	}
	if !ok {
		return RecoverRowsResult{}, fmt.Errorf("table %q not found on current working root", tableName)
	}
	tn.Name = resolvedName

	// recover-rows commits unconditionally, which dangling-faults on a
	// dolt_ignore'd table: those tables are never committed, so their content
	// chunks are not commit-rooted and the rewritten rows' out-of-band refs
	// cannot be rooted at commit time. Refuse such tables and point the operator
	// at migrate-adaptive, whose force-inline path is the safe heal for them.
	ignored, ignErr := doltdb.IdentifyIgnoredTables(ctx, roots, []doltdb.TableName{tn})
	if ignErr != nil {
		return RecoverRowsResult{}, fmt.Errorf("check dolt_ignore status for %s: %w", resolvedName, ignErr)
	}
	if len(ignored) > 0 {
		return RecoverRowsResult{}, fmt.Errorf(
			"table %s is dolt_ignore'd; recover-rows commits unconditionally and would dangling-fault on its never-committed content chunks. Use `dolt admin schema-encoding-drift migrate-adaptive --table %s --column <col>` instead — it force-inlines dolt_ignore'd tables (no out-of-band refs) and persists the working set",
			resolvedName, resolvedName,
		)
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return RecoverRowsResult{}, fmt.Errorf("get schema for %s: %w", resolvedName, err)
	}

	// Keyless tables are supported (rewriteColumn rehashes their content-
	// derived row ids), EXCEPT when the table has secondary indexes: keyless
	// index entries embed each row's hash id, and re-keying rewritten rows
	// would strand those entries. Refuse with the workaround spelled out.
	if err := refuseKeylessWithSecondaryIndexes(sch, resolvedName, "recover-rows"); err != nil {
		return RecoverRowsResult{}, err
	}

	existingCol, ok := sch.GetAllCols().GetByNameCaseInsensitive(colName)
	if !ok {
		return RecoverRowsResult{}, fmt.Errorf("column %q not found in table %s", colName, resolvedName)
	}

	oldEnc := existingCol.TypeInfo.Encoding()

	// Fast-path: already at a legacy encoding. Nothing to do — return success
	// as an idempotent no-op. This matches repair's contract for the same
	// shape so scripted loops can call either command without branching.
	if IsLegacyEncoding(oldEnc) {
		return RecoverRowsResult{
			Outcome:     RecoverRowsAlreadyOK,
			Database:    dbName,
			Table:       resolvedName,
			Column:      existingCol.Name,
			OldEncoding: oldEnc,
			NewEncoding: oldEnc,
		}, nil
	}

	newEnc, hasLegacy := LegacySibling(oldEnc)
	if !IsAdaptiveEncoding(oldEnc) || !hasLegacy {
		return RecoverRowsResult{
				Outcome:     RecoverRowsNoLegacyRows,
				Database:    dbName,
				Table:       resolvedName,
				Column:      existingCol.Name,
				OldEncoding: oldEnc,
			}, fmt.Errorf(
				"column %s.%s has encoding %s which is not an adaptive TEXT/BLOB/JSON/GEOMETRY variant; "+
					"there is no legacy sibling to migrate to",
				resolvedName, existingCol.Name, encodingName(oldEnc),
			)
	}

	if existingCol.IsPartOfPK {
		return RecoverRowsResult{}, fmt.Errorf(
			"column %s.%s is part of the primary key; PK columns do not carry adaptive encodings",
			resolvedName, existingCol.Name,
		)
	}

	tupleIndex, ok := valueTupleIndexForColumn(sch, existingCol.Name)
	if !ok {
		return RecoverRowsResult{}, fmt.Errorf(
			"column %s.%s does not appear in the value tuple (maybe virtual?)",
			resolvedName, existingCol.Name,
		)
	}

	// Build the patched schema with the target column flipped to its legacy
	// sibling encoding. We use the same replaceColumnByTag helper repair.go
	// uses so the index/check/constraint carryover is identical.
	newCol := existingCol
	newCol.TypeInfo = existingCol.TypeInfo.WithEncoding(newEnc)
	newCol.Kind = newCol.TypeInfo.NomsKind()

	newSch, err := replaceColumnByTag(sch, newCol)
	if err != nil {
		return RecoverRowsResult{}, fmt.Errorf("rebuild schema for %s: %w", resolvedName, err)
	}

	// Walk every row, classify the target field, and rewrite the row into the
	// canonical legacy form. We refuse the migration up front if the column
	// has no legacy-raw-hash witnesses (it's purely adaptive — repair would
	// also refuse). Otherwise we proceed even with mixed rows; the entire
	// point of recover-rows is to handle that case.
	rewritten, scanned, sawAnyLegacy, err := rewriteColumn(ctx, tbl, tupleIndex, newSch, ddb, resolveFieldToLegacy)
	if err != nil {
		return RecoverRowsResult{}, fmt.Errorf("rewrite rows of %s.%s: %w", resolvedName, existingCol.Name, err)
	}

	// If we walked every row and saw no canonical-legacy / inline / addressed
	// content at all, the column was empty (no value rows). Then we still need
	// to flip the schema because the encoding tag is the load-bearing thing.
	// If we saw NO legacy witnesses but DID see adaptive rows, refuse: that's
	// repair's "genuine adaptive" case, not recover-rows territory.
	if scanned > 0 && !sawAnyLegacy {
		// Pure-adaptive column. recover-rows should NOT flip — that would
		// corrupt the adaptive payload exactly the way repair guards against.
		return RecoverRowsResult{
				Outcome:     RecoverRowsNoLegacyRows,
				Database:    dbName,
				Table:       resolvedName,
				Column:      existingCol.Name,
				OldEncoding: oldEnc,
				RowsScanned: scanned,
			}, fmt.Errorf(
				"column %s.%s has only genuine adaptive rows (no legacy-raw-hash witnesses); recover-rows refused — the column is internally consistent and a migration would be a no-op flip best handled by checking column health (run `check`) first",
				resolvedName, existingCol.Name,
			)
	}

	// Wrap the rewritten map as a durable index and apply both changes (rows
	// + schema) to the table. Order matters: SetSchema first so subsequent
	// reads use the legacy descriptors; SetTableRows second so the new bytes
	// are correctly interpreted.
	updatedTbl, err := tbl.UpdateSchema(ctx, newSch)
	if err != nil {
		return RecoverRowsResult{}, fmt.Errorf("apply schema change to %s: %w", resolvedName, err)
	}
	updatedTbl, err = updatedTbl.UpdateRows(ctx, durable.IndexFromProllyMap(rewritten.Map))
	if err != nil {
		return RecoverRowsResult{}, fmt.Errorf("apply rewritten rows to %s: %w", resolvedName, err)
	}

	newWorking, err := roots.Working.PutTable(ctx, tn, updatedTbl)
	if err != nil {
		return RecoverRowsResult{}, fmt.Errorf("put updated table %s: %w", resolvedName, err)
	}
	roots.Working = newWorking
	roots.Staged = newWorking

	// Count rows rewritten — the unchanged subset is the difference between
	// scanned and rewritten. We surface both so the operator can audit the
	// migration's blast radius. Collateral rewrites (non-target AddrEnc
	// fields healed in the same row) are reported separately so the operator
	// can tell when a prior `repair` left a sibling column with a schema-row
	// mismatch that this migration repaired as a side-effect.
	rowsRewritten := rewritten.RowsRewritten
	collateral := rewritten.CollateralRewrites
	merged := rewritten.RowsMerged
	var commitMsg string
	if collateral > 0 {
		commitMsg = fmt.Sprintf(
			"admin: schema-encoding-drift recover-rows on %s.%s (%d rows rewritten to legacy format + schema flipped to %s; %d rows had non-target AddrEnc fields healed as collateral; content unchanged)",
			resolvedName, existingCol.Name, rowsRewritten, encodingName(newEnc), collateral,
		)
	} else {
		commitMsg = fmt.Sprintf(
			"admin: schema-encoding-drift recover-rows on %s.%s (%d rows rewritten to legacy format + schema flipped to %s; content unchanged)",
			resolvedName, existingCol.Name, rowsRewritten, encodingName(newEnc),
		)
	}
	if merged > 0 {
		commitMsg += fmt.Sprintf(" [%d duplicate keyless rows merged by cardinality]", merged)
	}
	commitHash, err := commitWorkingRoot(ctx, dEnv, headRef, roots, commitMsg)
	if err != nil {
		return RecoverRowsResult{}, fmt.Errorf("commit recover-rows: %w", err)
	}

	return RecoverRowsResult{
		Outcome:            RecoverRowsMigrated,
		Database:           dbName,
		Table:              resolvedName,
		Column:             existingCol.Name,
		OldEncoding:        oldEnc,
		NewEncoding:        newEnc,
		RowsScanned:        scanned,
		RowsRewritten:      rowsRewritten,
		CollateralRewrites: collateral,
		RowsMerged:         merged,
		CommitHash:         commitHash.String(),
		CommitMessage:      commitMsg,
	}, nil
}

// rewrittenMap is the output of rewriteColumnToLegacy. It carries the new
// prolly.Map (suitable for wrapping as a durable Index) and the count of rows
// whose target field bytes were actually changed by the migration. We use a
// named struct rather than a multi-return so the row-count travels with the
// map all the way back to the commit-message builder.
//
// CollateralRewrites counts rows where one or more NON-TARGET fields had to be
// normalized to satisfy the new value descriptor. The case this surfaces:
// a previous `repair` invocation on a sibling column may have flipped its
// schema to legacy without scanning every row — leaving non-canonical
// adaptive bytes in that column under a legacy encoding tag. When THIS
// migration constructs a new tuple, the serializer's countAddresses walks
// every AddrEnc field and calls hash.New on its bytes, which panics on
// any field whose length is not exactly 20. Normalizing those sibling fields
// in-tuple unblocks the rewrite AND repairs the latent mismatch in one shot
// (content-preserving: inline bytes are written to the chunkstore, addressed
// trailers are reused verbatim).
type rewrittenMap struct {
	Map                prolly.Map
	RowsRewritten      int
	CollateralRewrites int
	// RowsMerged counts keyless rows whose canonical rewrite collided with an
	// existing entry (the same logical row stored once in legacy form and once
	// in adaptive form). The colliding entries are merged by summing their
	// cardinalities; without the merge the second Put would silently overwrite
	// the first and lose rows.
	RowsMerged int
}

// targetFieldResolver resolves a single value-tuple field's raw bytes into the
// canonical form for a migration's TARGET encoding. The two implementations are
// resolveFieldToLegacy (adaptive/legacy → canonical legacy 20-byte hash) and
// resolveFieldToAdaptive (legacy/adaptive → canonical adaptive inline/addressed).
// Both share the same signature so rewriteColumn can drive either direction
// while keeping the (direction-agnostic) sibling-AddrEnc collateral heal fixed
// on resolveFieldToLegacy. Return contract:
//   - newField: the resolved bytes for the target position.
//   - wasSourceFormat: the input was in the migration's SOURCE format (a legacy
//     witness for the forward heal; a legacy witness for the legacy heal too) —
//     the caller ORs these into sawAnySource to gate the schema flip.
//   - changed: newField is not byte-identical to the input (drives Put/skip).
type targetFieldResolver func(ctx context.Context, b []byte, vs val.ValueStore, cs ChunkPresenceChecker) (newField []byte, wasSourceFormat bool, changed bool, err error)

// rewriteColumnToLegacy iterates every row of |tbl|'s primary index, resolves
// the field bytes at |tupleIndex| into canonical legacy form, and produces a
// new prolly.Map whose value tuples carry the canonical bytes. The resulting
// map's value descriptor is derived from |newSch|, so downstream readers (with
// the legacy schema applied) interpret every field correctly.
//
// Returns:
//   - rewritten: the new prolly.Map (always non-empty unless the original was
//     empty), wrapped with the rewritten-row count.
//   - scanned: total rows iterated from the source map (NOT the same as the
//     resulting map's row count when an error aborts the loop early).
//   - sawAnyLegacy: true if at least one row had a legacy-raw-hash field, OR
//     the table had no row data at all (vacuous truth: a 0-row migration is
//     always safe). Used by the caller to decide whether the schema flip is
//     justified.
//   - err: non-nil if a row's bytes cannot be classified (unknown shape) or a
//     vs.WriteBytes / vs.ReadBytes call fails. Aborts mid-iteration; partial
//     mutations are discarded.
func rewriteColumn(
	ctx context.Context,
	tbl *doltdb.Table,
	tupleIndex int,
	newSch schema.Schema,
	cs ChunkPresenceChecker,
	resolveTarget targetFieldResolver,
) (rewritten rewrittenMap, scanned int, sawAnyLegacy bool, err error) {
	durableIdx, err := tbl.GetRowData(ctx)
	if err != nil {
		return rewrittenMap{}, 0, false, err
	}
	if durableIdx == nil {
		return rewrittenMap{}, 0, true, nil
	}
	empty, err := durableIdx.Empty()
	if err != nil {
		return rewrittenMap{}, 0, false, err
	}
	pm, err := durable.ProllyMapFromIndex(durableIdx)
	if err != nil {
		return rewrittenMap{}, 0, false, err
	}
	ns := pm.NodeStore()
	pool := pm.Pool()

	// Derive the post-migration value descriptor from the patched schema. We
	// seed the MutableMap rewriter with this descriptor so any caller that
	// later reads from the rewritten map sees the canonical legacy view.
	newValDesc := newSch.GetValueDescriptor(ns)
	mut := pm.Rewriter(pm.KeyDesc(), newValDesc)

	// Keyless tables key each row by xxh3.Hash128 of the value tuple's field
	// bytes (val.HashTupleFromValue), so a rewritten row must (a) carry bytes
	// the engine's own TupleBuilder would produce for the same logical content
	// — future DML re-derives the hash from a freshly-built tuple and must hit
	// the stored key — and (b) be re-keyed to the hash of those canonical
	// bytes, deleting the entry under the stale key.
	isKeyless := schema.IsKeyless(newSch)
	keylessTarget := newSch.GetTargetRowSize()

	// Pre-compute which positions in the new value descriptor are legacy
	// AddrEnc — i.e., positions where the field bytes MUST be either NULL or
	// exactly 20 bytes (a legacy raw content-hash address). Any other shape
	// would trip ProllyMapSerializer.countAddresses' hash.New call on the
	// serialization path (panic: "invalid hash length: N"). We normalize
	// non-canonical bytes in these positions into proper 20-byte hashes
	// before assembling the new tuple — see the sibling-mismatch case-study comment on
	// rewrittenMap.CollateralRewrites.
	addrEncPositions := make([]bool, newValDesc.Count())
	val.IterAddressFields(newValDesc, func(i int, _ val.Type) {
		addrEncPositions[i] = true
	})

	if empty {
		// No rows to migrate. The rewriter is still a no-op MutableMap; we
		// materialize it to get a properly-typed (key/val descriptors) empty
		// Map back. sawAnyLegacy=true (vacuous truth) so the caller flips the
		// schema for the (also-empty) row payload.
		nm, mErr := mut.Map(ctx)
		if mErr != nil {
			return rewrittenMap{}, 0, false, mErr
		}
		return rewrittenMap{Map: nm}, 0, true, nil
	}

	iter, err := pm.IterAll(ctx)
	if err != nil {
		return rewrittenMap{}, 0, false, err
	}

	rowsRewritten := 0
	collateralRewrites := 0
	rowsMerged := 0
	keyDesc := pm.KeyDesc()
	for {
		key, value, iterErr := iter.Next(ctx)
		if errors.Is(iterErr, io.EOF) {
			break
		}
		if iterErr != nil {
			return rewrittenMap{}, scanned, sawAnyLegacy, iterErr
		}
		scanned++

		oldField := value.GetField(tupleIndex)
		newField, isLegacy, targetChanged, resolveErr := resolveTarget(ctx, oldField, ns, cs)
		if resolveErr != nil {
			pkHex := formatKeyForError(key, keyDesc)
			return rewrittenMap{}, scanned, sawAnyLegacy, fmt.Errorf(
				"row with primary key %s: %w", pkHex, resolveErr,
			)
		}
		if isLegacy {
			sawAnyLegacy = true
		}

		// a later fix/ defensive width check on BOTH sides. The encoding flip
		// performed by this command MUST NOT change column count — in-place
		// schema mutations preserve the value-tuple shape. If we ever observe
		// a source tuple WIDER than the new value descriptor (e.g. a
		// concurrent ALTER raced ahead and dropped a column we still see),
		// silently growing `values` to descCount would produce a tuple that
		// disagrees with the persisted schema. Refuse rather than swallow it.
		fieldCount := value.Count()
		descCount := newValDesc.Count()
		if fieldCount > descCount {
			pkHex := formatKeyForError(key, keyDesc)
			return rewrittenMap{}, scanned, sawAnyLegacy, fmt.Errorf(
				"schema-row width mismatch at primary key %s: source tuple has %d fields, new value descriptor expects %d — refusing rewrite (likely concurrent schema change)",
				pkHex, fieldCount, descCount,
			)
		}
		if descCount > fieldCount {
			fieldCount = descCount
		}

		// Pre-scan: do any NON-TARGET AddrEnc fields need normalization? If
		// every AddrEnc field is already canonical (20-byte hash or NULL) AND
		// the target field is unchanged, we can skip this row entirely.
		needCollateral := false
		for i := 0; i < fieldCount; i++ {
			if i == tupleIndex {
				continue
			}
			if i >= len(addrEncPositions) || !addrEncPositions[i] {
				continue
			}
			b := value.GetField(i)
			if len(b) == 0 || len(b) == hashByteLen {
				// NULL or canonical 20-byte hash — already serializer-safe.
				// 20-byte 0x00-leading bytes that are actually adaptive
				// inline payloads get caught by the chunkstore-aware
				// resolveFieldToLegacy if we DO normalize, but the structural
				// fast-path here treats them as fine; mis-classification at
				// this stage only forces an unnecessary normalization, not a
				// silent corruption.
				continue
			}
			needCollateral = true
			break
		}

		if !targetChanged && !needCollateral {
			continue
		}

		// Rebuild the value tuple with the rewritten target field. We
		// construct it via val.NewTuple directly rather than via TupleBuilder
		// — the TupleBuilder.Build path can re-shuffle adaptive columns based
		// on a size heuristic, and that's exactly what we need to avoid when
		// the source tuple may contain another not-yet-migrated adaptive
		// column with non-canonical bytes.
		//
		// Normalization model for each position:
		//   - target column: use newField (already 20-byte canonical hash)
		//   - non-target legacy AddrEnc position w/ non-canonical bytes: resolve
		//     to canonical 20-byte hash via resolveFieldToLegacy. This is the
		//     content-preserving heal for the documented case where a prior `repair`
		//     left a sibling column with adaptive bytes under a legacy schema.
		//   - non-target legacy AddrEnc position w/ canonical bytes (NULL or
		//     20-byte hash): copy verbatim — already serializer-safe.
		//   - non-target adaptive / non-address position: copy verbatim — the
		//     serializer expects variable-length bytes here.
		values := make([][]byte, fieldCount)
		rowHadCollateral := false
		for i := 0; i < fieldCount; i++ {
			if i == tupleIndex {
				values[i] = newField
				continue
			}
			b := value.GetField(i)
			if i < len(addrEncPositions) && addrEncPositions[i] && len(b) != 0 && len(b) != hashByteLen {
				normalized, _, normChanged, normErr := resolveFieldToLegacy(ctx, b, ns, cs)
				if normErr != nil {
					pkHex := formatKeyForError(key, keyDesc)
					return rewrittenMap{}, scanned, sawAnyLegacy, fmt.Errorf(
						"row with primary key %s, field %d (non-target AddrEnc heal): %w",
						pkHex, i, normErr,
					)
				}
				values[i] = normalized
				if normChanged {
					rowHadCollateral = true
				}
				continue
			}
			values[i] = b
		}
		newValue := val.NewTuple(pool, values...)
		putKey := key

		if isKeyless {
			// Rebuild the tuple through the engine's TupleBuilder so the
			// stored bytes are exactly what a future engine-built tuple for
			// the same logical content hashes to. In particular, adaptive
			// fields are fed inline-first and spilled out-of-band only by the
			// builder's whole-tuple size heuristic — NOT by any fixed
			// per-content rule.
			canonical, cErr := buildCanonicalKeylessValue(ctx, ns, pool, newValDesc, keylessTarget, values)
			if cErr != nil {
				pkHex := formatKeyForError(key, keyDesc)
				return rewrittenMap{}, scanned, sawAnyLegacy, fmt.Errorf(
					"canonicalize keyless row with hash id %s: %w", pkHex, cErr,
				)
			}
			if bytes.Equal(canonical, value) {
				// The canonical rebuild proved this row was already in engine-
				// canonical form; nothing to rewrite, key stays valid.
				continue
			}
			newValue = canonical
			newKey := val.HashTupleFromValue(pool, newValue)
			if !bytes.Equal(newKey, key) {
				// The row moves to a new content-derived key. If the
				// destination already holds an entry (the same logical row
				// stored once per format — possible in a heterogeneous
				// column), MERGE by summing cardinalities; a plain Put would
				// silently overwrite the earlier entry and lose rows.
				var existing val.Tuple
				if gErr := mut.Get(ctx, newKey, func(_, v val.Tuple) error {
					existing = v
					return nil
				}); gErr != nil {
					pkHex := formatKeyForError(key, keyDesc)
					return rewrittenMap{}, scanned, sawAnyLegacy, fmt.Errorf(
						"check destination key for keyless row %s: %w", pkHex, gErr,
					)
				}
				if existing != nil {
					newValue, _ = val.ModifyKeylessCardinality(pool, newValue, int64(val.ReadKeylessCardinality(existing)))
					rowsMerged++
				}
				if dErr := mut.Delete(ctx, key); dErr != nil {
					pkHex := formatKeyForError(key, keyDesc)
					return rewrittenMap{}, scanned, sawAnyLegacy, fmt.Errorf(
						"delete stale keyless key %s: %w", pkHex, dErr,
					)
				}
				putKey = newKey
			}
		}

		if putErr := mut.Put(ctx, putKey, newValue); putErr != nil {
			pkHex := formatKeyForError(key, keyDesc)
			return rewrittenMap{}, scanned, sawAnyLegacy, fmt.Errorf(
				"put rewritten row with primary key %s: %w", pkHex, putErr,
			)
		}
		rowsRewritten++
		if rowHadCollateral {
			collateralRewrites++
		}
	}

	newMap, err := mut.Map(ctx)
	if err != nil {
		return rewrittenMap{}, scanned, sawAnyLegacy, err
	}
	return rewrittenMap{
		Map:                newMap,
		RowsRewritten:      rowsRewritten,
		CollateralRewrites: collateralRewrites,
		RowsMerged:         rowsMerged,
	}, scanned, sawAnyLegacy, nil
}

// refuseKeylessWithSecondaryIndexes returns an error when |sch| is a keyless
// schema with one or more secondary indexes. Keyless secondary index entries
// embed each row's content-derived hash id; the row migrations re-key every
// rewritten row, which would strand those entries. Until the migrations learn
// to rebuild secondary indexes, the safe posture is an explicit refusal with
// the workaround spelled out. Keyed tables are unaffected (their index entries
// reference primary keys, which the migrations never change).
func refuseKeylessWithSecondaryIndexes(sch schema.Schema, tableName, cmdName string) error {
	if !schema.IsKeyless(sch) {
		return nil
	}
	if n := len(sch.Indexes().AllIndexes()); n > 0 {
		return fmt.Errorf(
			"table %s is keyless and has %d secondary index(es); keyless index entries embed row hash ids that %s's re-keying would invalidate. Drop the index(es), re-run %s, then re-create them",
			tableName, n, cmdName, cmdName,
		)
	}
	return nil
}

// buildCanonicalKeylessValue rebuilds a keyless value tuple in the engine-
// canonical form for |desc|: every adaptive field is fed to the TupleBuilder
// as full inline content (exactly like the writer's tuplesFromRow → PutField
// path) and the builder's whole-tuple heuristic decides inline vs out-of-band
// placement; every other field — including the cardinality at position 0 — is
// copied verbatim. The result is byte-identical to what the engine would
// build for the same logical row, which is the invariant keyless DML depends
// on: UPDATE/DELETE locate rows by hashing a freshly-built tuple.
//
// |fields| holds the resolved per-position bytes (length >= desc.Count();
// positions beyond the source tuple's width are nil → NULL). |target| is the
// schema's tuple-length target (schema.GetTargetRowSize), matching the
// writer's WithMaxRowSize configuration.
func buildCanonicalKeylessValue(ctx context.Context, vs val.ValueStore, bp pool.BuffPool, desc *val.TupleDesc, target uint16, fields [][]byte) (val.Tuple, error) {
	tb := val.NewTupleBuilder(desc, vs).WithMaxRowSize(target)
	for i := 0; i < desc.Count(); i++ {
		var b []byte
		if i < len(fields) {
			b = fields[i]
		}
		if len(b) == 0 {
			// NULL (zero-length and NULL are the same thing at tuple level —
			// see val.Tuple.GetField). Leave the builder slot empty.
			continue
		}
		if val.IsAdaptiveEncoding(desc.Types[i].Enc) {
			content, present, cErr := underlyingContent(ctx, vs, b)
			if cErr != nil {
				return nil, fmt.Errorf("field %d: %w", i, cErr)
			}
			if !present {
				return nil, fmt.Errorf("field %d: content chunk is genuinely absent; refusing to rewrite the row", i)
			}
			if pErr := tb.PutAdaptiveFromInline(ctx, i, content); pErr != nil {
				return nil, fmt.Errorf("field %d: re-encode adaptive content (%d bytes): %w", i, len(content), pErr)
			}
			continue
		}
		tb.PutRaw(i, b)
	}
	return tb.BuildPermissive(ctx, bp)
}

// resolveFieldToLegacy is the per-row workhorse. Given a single field's bytes
// from a (possibly drifted) adaptive-tagged value tuple, it returns the
// canonical legacy form of those bytes plus two flags:
//
//   - isCanonicalLegacy: true if the input bytes were already in the legacy
//     shape (NULL, or non-zero-leading 20 bytes, or 0x00-leading 20 bytes
//     whose hash IS present in the chunkstore). The caller uses this to
//     compute the "sawAnyLegacy" signal that guards the schema flip.
//   - changed: true if the returned bytes are NOT byte-identical to the input.
//     The caller uses this to decide whether to Put the row into the rewriter
//     (saves a skip-list slot per unchanged row).
//
// The classification logic mirrors classify.go's structural classifier but
// promotes the ambiguous 0x00-leading 20-byte case using the chunkstore in the
// same way ClassifyFieldWithChunkstore does. We don't call that function
// directly because we also need the resolved CONTENT for inline-to-OOB
// promotion, and that's not part of the classifier's contract.
//
// Errors are returned for:
//   - FieldUnknown shapes (1-19 bytes, or 21+ bytes with malformed varint
//     trailer). The caller adds the row's primary-key bytes to the error.
//   - vs.ReadBytes / vs.WriteBytes failures (e.g., missing NBS chunk for an
//     adaptive-addressed row whose hash is no longer present).
func resolveFieldToLegacy(ctx context.Context, b []byte, vs val.ValueStore, cs ChunkPresenceChecker) (newField []byte, isCanonicalLegacy bool, changed bool, err error) {
	if len(b) == 0 {
		// NULL: same in both formats.
		return nil, true, false, nil
	}

	if b[0] != 0 {
		// Non-zero leading byte: either legacy raw hash (20 bytes) or
		// adaptive addressed (varint length + 20 hash bytes, total >= 21).
		switch {
		case len(b) == hash.ByteLen:
			// Legacy raw hash: already canonical. No rewrite.
			return b, true, false, nil
		case len(b) > hash.ByteLen:
			// Try to interpret as adaptive addressed.
			declaredLen, varintSize := uvarint.Uvarint(b)
			if varintSize > 0 && varintSize+hash.ByteLen == len(b) {
				_ = declaredLen
				// Extract the trailing 20 bytes — that IS the canonical legacy
				// hash. The content is already stored out-of-band at that
				// hash; no chunkstore write needed.
				addr := b[varintSize:]
				out := make([]byte, hash.ByteLen)
				copy(out, addr)
				return out, false, true, nil
			}
			// 21+ bytes, non-zero leader, but the varint+hash shape doesn't
			// add up. Unrecognized.
			return nil, false, false, fmt.Errorf("field has unknown shape: %d bytes, non-zero leader, malformed adaptive-addressed trailer", len(b))
		default:
			// 1-19 bytes, non-zero leader — too short for either legacy
			// (needs 20) or adaptive addressed (needs >= 21).
			return nil, false, false, fmt.Errorf("field has unknown shape: %d bytes, non-zero leader, too short for any known encoding", len(b))
		}
	}

	// Leading byte is 0x00 — structurally adaptive inline. But this is also the
	// disambiguous case for legacy raw hashes whose first byte happens to be
	// 0x00 (~1/256 of all hashes). We disambiguate via chunkstore presence for
	// the 20-byte case only.
	//
	// This deployed recover-rows path KEEPS cs.Has deliberately (the new
	// forward-migrate path resolveFieldToAdaptive uses the authoritative
	// vs.ReadBytes-attempt). The review code-proved cs.Has is correct at runtime in
	// real-world: cs MUST be the table's own ddb (callers satisfy this), vs is
	// the same table's NodeStore, and both resolve to ONE chunks.ChunkStore
	// where Has⟺ReadBytes-success are consistent (GenerationalNBS ORs all
	// generations) — no stranding false-negative is possible. Switching this
	// call site to the ReadBytes-attempt is NOT behavior-equivalent here: cs.Has
	// PROPAGATES a transient chunkstore error (a contract the
	// ChunkstoreHasError regression pins), whereas a ReadBytes-attempt would
	// swallow it as "absent ⇒ inline" and could misclassify a present legacy
	// row. Per the a maintainer escape hatch we keep cs.Has + this comment.
	// Defense in depth: recover-rows now REFUSES dolt_ignore'd tables up front,
	// so the only case where chunk visibility could ever differ across stores
	// (never-committed chunks) can't reach here.
	if len(b) == hash.ByteLen && cs != nil {
		h := hash.New(b)
		present, hasErr := cs.Has(ctx, h)
		if hasErr != nil {
			return nil, false, false, fmt.Errorf("chunkstore presence check: %w", hasErr)
		}
		if present {
			// Legacy raw hash with coincidental 0x00 leader. Canonical.
			return b, true, false, nil
		}
	}

	// Genuine adaptive inline. Promote to out-of-band: write the inline
	// content (b[1:]) to the chunkstore, then store the resulting hash as
	// the 20-byte field bytes.
	content := b[1:]
	addr, writeErr := vs.WriteBytes(ctx, content)
	if writeErr != nil {
		return nil, false, false, fmt.Errorf("write inline content (%d bytes) to chunkstore: %w", len(content), writeErr)
	}
	out := make([]byte, hash.ByteLen)
	copy(out, addr[:])
	return out, false, true, nil
}

// formatKeyForError renders a primary-key tuple as a human-readable hex string
// for inclusion in error messages. We deliberately don't invoke the type
// descriptor's Format method here: that path can itself touch adaptive
// dispatch for index keys that contain adaptive fields, and we don't want
// error reporting to recurse into the very panic recover-rows exists to fix.
func formatKeyForError(key val.Tuple, keyDesc *val.TupleDesc) string {
	// Show one field per ordinal, hex-encoded, comma-separated. For most
	// real-world tables the PK is a small integer or short string so this stays
	// readable.
	if keyDesc == nil {
		return hex.EncodeToString(key)
	}
	out := "["
	for i := 0; i < keyDesc.Count(); i++ {
		if i > 0 {
			out += ", "
		}
		f := key.GetField(i)
		if f == nil {
			out += "NULL"
			continue
		}
		out += "0x" + hex.EncodeToString(f)
	}
	out += "]"
	return out
}

// emitRecoverRowsResult writes a human-readable summary of the migration to
// |w|.
func emitRecoverRowsResult(w io.Writer, r RecoverRowsResult) {
	switch r.Outcome {
	case RecoverRowsMigrated:
		_, _ = fmt.Fprintf(w, "recover-rows %s.%s: %d/%d rows rewritten, schema %s → %s\n",
			r.Table, r.Column, r.RowsRewritten, r.RowsScanned,
			encodingName(r.OldEncoding), encodingName(r.NewEncoding))
		if r.CollateralRewrites > 0 {
			_, _ = fmt.Fprintf(w, "  %d rows had non-target AddrEnc fields healed as collateral (residual mismatch from prior repair on a sibling column)\n",
				r.CollateralRewrites)
		}
		if r.RowsMerged > 0 {
			_, _ = fmt.Fprintf(w, "  %d duplicate keyless rows merged by cardinality\n", r.RowsMerged)
		}
		_, _ = fmt.Fprintf(w, "  commit: %s\n", r.CommitHash)
		_, _ = fmt.Fprintf(w, "  message: %s\n", r.CommitMessage)
	case RecoverRowsAlreadyOK:
		_, _ = fmt.Fprintf(w, "no-op for %s.%s: already at %s (idempotent success)\n",
			r.Table, r.Column, encodingName(r.OldEncoding))
	default:
		// RecoverRowsNoLegacyRows surfaces via the returned error, so the CLI
		// emitter never reaches this branch.
	}
}
