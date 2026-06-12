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
	"github.com/dolthub/dolt/go/store/val"
)

// MigrateAdaptiveCmd is the FORWARD (v2-native) counterpart of RecoverRowsCmd.
// Where recover-rows rewrites a heterogeneous column DOWN to canonical legacy
// (1.x) form, migrate-adaptive rewrites it UP to canonical adaptive (2.x) form:
// every legacy-raw-hash row is converted to a canonical adaptive value and the
// persisted column encoding is flipped to its adaptive sibling. Rows that are
// already adaptive (inline or addressed) are left byte-for-byte untouched.
//
// This is the heal for the operator's chosen "the forward-migration approach" — resolve the drift
// natively for the v2 format, with no reader-compat fallback. After this runs
// on every affected column, the column's schema record AND every row share one
// consistent adaptive format, so reads no longer panic and future writes (which
// already emit adaptive by default on 2.0.7+) stay consistent.
//
// CONTENT is preserved byte-for-byte. Only STORAGE LAYOUT changes for the
// migrated legacy rows:
//
//   - NULL stays NULL.
//   - A legacy-raw-hash row (a bare 20-byte content address, either non-zero
//     leader or a 0x00 leader whose hash IS present in the chunkstore) is
//     dereferenced to its content and re-encoded canonically: content ≤ 20
//     bytes is inlined as `[0x00][content]`; larger content stays out-of-band
//     as `[varint(len)][20-byte hash]`, REUSING the existing chunk (the content
//     hash is unchanged), so no large blob is rewritten.
//   - An already-adaptive row — inline (`[0x00]…`) or addressed
//     (`[varint][20-byte hash]`) — is canonical and copied verbatim.
//   - A row whose field bytes cannot be classified is fatal; the migration
//     aborts and reports the row's primary key. Partial migrations never commit.
//
// Atomicity matches recover-rows: a single dolt commit records both the
// rewritten prolly map and the schema-encoding flip.
type MigrateAdaptiveCmd struct{}

var _ cli.Command = MigrateAdaptiveCmd{}

var migrateAdaptiveDocs = cli.CommandDocumentationContent{
	ShortDesc: "Row-by-row forward migration of a drifted/legacy column into canonical adaptive (v2) form",
	LongDesc: `Walks every row in ` + "`--table`" + `, reads the target ` + "`--column`" + `'s field bytes through the correct per-row dispatch (legacy raw-hash, adaptive inline, or adaptive addressed), and rewrites each legacy row into a canonical adaptive value. After every row is processed the persisted column encoding is flipped to its adaptive sibling (e.g. ` + "`StringAddrEnc` → `StringAdaptiveEnc`" + `). Row data and schema update inside a single dolt commit.

This is the FORWARD counterpart to ` + "`recover-rows`" + ` (which migrates the other way, to legacy). Use ` + "`migrate-adaptive`" + ` to resolve encoding drift natively for the v2 format: after it runs, the column's schema record and every row share one consistent adaptive format, with no reader-compat fallback required.

Content is unchanged — the bytes the user originally inserted are readable byte-for-byte before and after. Legacy rows whose content is larger than 20 bytes keep their existing out-of-band chunk (only a length prefix is added); content of 20 bytes or fewer is inlined. Already-adaptive rows are copied verbatim.

The command aborts the entire migration and reports the offending row's primary-key bytes if any field cannot be classified into a known shape; partial migrations are never committed.

Required flags: ` + "`--table`" + ` and ` + "`--column`" + ` — one column per invocation, to keep each commit a single auditable change.

Exit codes:
  0  migration succeeded (or the column was already fully canonical adaptive — idempotent no-op)
  1  migration refused (unknown-shape rows, or the operation failed)`,
	Synopsis: []string{"--table <name> --column <name>"},
}

func (cmd MigrateAdaptiveCmd) Name() string { return "migrate-adaptive" }

func (cmd MigrateAdaptiveCmd) Description() string {
	return "Row-by-row migration of a drifted/legacy payload into canonical adaptive (v2) form (atomic prolly rewrite + schema flip + dolt commit)"
}

func (cmd MigrateAdaptiveCmd) RequiresRepo() bool { return true }

func (cmd MigrateAdaptiveCmd) Docs() *cli.CommandDocumentation {
	return cli.NewCommandDocumentation(migrateAdaptiveDocs, cmd.ArgParser())
}

const dryRunFlag = "dry-run"

func (cmd MigrateAdaptiveCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsString(tableFlag, "", "name", "the table to migrate")
	ap.SupportsString(colFlag, "", "name", "the column whose payload should be rewritten to canonical adaptive form")
	ap.SupportsFlag(dryRunFlag, "", "Report, per row class, what would change (legacy rows to rewrite / already-adaptive / empty) WITHOUT writing or committing. Classifies by field shape only — no content is dereferenced, so it is safe on a column whose secondary indexes or content chunks are damaged.")
	return ap
}

func (cmd MigrateAdaptiveCmd) Hidden() bool { return true }

func (cmd MigrateAdaptiveCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, _ cli.CliContext) int {
	ap := cmd.ArgParser()
	usage, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, migrateAdaptiveDocs, ap))
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

	result, err := migrateAdaptiveColumnOpts(ctx, dEnv, tableName, colName, apr.Contains(dryRunFlag))
	if err != nil {
		verr := errhand.BuildDError("migrate-adaptive failed").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	emitMigrateAdaptiveResult(cli.OutStream, result)
	return 0
}

// MigrateAdaptiveResult is the structured outcome of a migrate-adaptive call.
type MigrateAdaptiveResult struct {
	Outcome            RecoverRowsOutcome
	Database           string
	Table              string
	Column             string
	OldEncoding        val.Encoding
	NewEncoding        val.Encoding
	RowsScanned        int
	RowsRewritten      int // legacy rows converted to adaptive
	CollateralRewrites int // sibling AddrEnc fields normalized to satisfy the descriptor
	RowsMerged         int // keyless only: rewritten rows merged into an existing entry by cardinality
	CommitHash         string
	CommitMessage      string

	// DryRun and the Class* tallies are populated only when --dry-run is set:
	// a non-mutating preview of what the real run would do, classified by field
	// shape (no content dereference).
	DryRun               bool
	ClassLegacyToRewrite int // FieldLegacyRawHash rows that would be rewritten to adaptive
	ClassAlreadyAdaptive int // adaptive inline/addressed rows that pass through untouched
	ClassEmptyOrNull     int // NULL or empty-inline rows (no-op)
	ClassUnknown         int // unclassifiable shapes (the real run would error on these)

	// MaxContentLen is the largest content size force-inlined (dolt_ignore'd
	// inline path only); surfaced so the operator is warned if it nears the
	// tuple size limit.
	MaxContentLen int

	// InlineCount / AddressedCount are the post-migration storage report (the
	// dolt_ignore'd inline path): how many migrated values ended up inline vs
	// out-of-band-addressed. AddressedCount MUST be 0 for a dolt_ignore'd table
	// — a deterministic deploy-verification + test signal.
	InlineCount    int
	AddressedCount int
}

// migrateAdaptiveColumn is the forward-migration pipeline. It mirrors
// recoverRowsColumn but drives the rewrite with resolveFieldToAdaptive and
// flips the schema to the adaptive sibling instead of the legacy one.
// migrateAdaptiveColumn performs the real (mutating) forward migration. It is
// the stable entry point used by the test hook (MigrateAdaptiveColumnForTest);
// Exec routes through migrateAdaptiveColumnOpts to thread --dry-run.
func migrateAdaptiveColumn(ctx context.Context, dEnv *env.DoltEnv, tableName, colName string) (MigrateAdaptiveResult, error) {
	return migrateAdaptiveColumnOpts(ctx, dEnv, tableName, colName, false)
}

func migrateAdaptiveColumnOpts(ctx context.Context, dEnv *env.DoltEnv, tableName, colName string, dryRun bool) (MigrateAdaptiveResult, error) {
	ddb := dEnv.DoltDB(ctx)
	dbName := ddb.GetDatabaseName()

	rsr := dEnv.RepoStateReader()
	headRef, err := rsr.CWBHeadRef(ctx)
	if err != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("get current branch: %w", err)
	}

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("get roots: %w", err)
	}

	tn := doltdb.TableName{Name: tableName, Schema: doltdb.DefaultSchemaName}
	tbl, resolvedName, ok, err := doltdb.GetTableInsensitive(ctx, roots.Working, tn)
	if err != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("get table %s: %w", tableName, err)
	}
	if !ok {
		return MigrateAdaptiveResult{}, fmt.Errorf("table %q not found on current working root", tableName)
	}
	tn.Name = resolvedName

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("get schema for %s: %w", resolvedName, err)
	}

	existingCol, ok := sch.GetAllCols().GetByNameCaseInsensitive(colName)
	if !ok {
		return MigrateAdaptiveResult{}, fmt.Errorf("column %q not found in table %s", colName, resolvedName)
	}

	if existingCol.IsPartOfPK {
		return MigrateAdaptiveResult{}, fmt.Errorf(
			"column %s.%s is part of the primary key; PK columns do not carry adaptive encodings",
			resolvedName, existingCol.Name)
	}

	oldEnc := existingCol.TypeInfo.Encoding()

	// Determine the target adaptive encoding.
	//   - legacy tag (e.g. StringAddr=23): flip to the adaptive sibling.
	//   - already-adaptive tag (e.g. StringAdaptive=135): target == old; we may
	//     still need to rewrite legacy ROWS that drifted under an adaptive tag.
	var newEnc val.Encoding
	switch {
	case IsAdaptiveEncoding(oldEnc):
		newEnc = oldEnc
	case IsLegacyEncoding(oldEnc):
		sib, ok := AdaptiveSibling(oldEnc)
		if !ok {
			return MigrateAdaptiveResult{}, fmt.Errorf(
				"column %s.%s has legacy encoding %s with no adaptive sibling",
				resolvedName, existingCol.Name, encodingName(oldEnc))
		}
		newEnc = sib
	default:
		return MigrateAdaptiveResult{}, fmt.Errorf(
			"column %s.%s has encoding %s which is not a TEXT/BLOB/JSON/GEOMETRY variant; nothing to migrate",
			resolvedName, existingCol.Name, encodingName(oldEnc))
	}

	tupleIndex, ok := valueTupleIndexForColumn(sch, existingCol.Name)
	if !ok {
		return MigrateAdaptiveResult{}, fmt.Errorf(
			"column %s.%s does not appear in the value tuple (maybe virtual?)",
			resolvedName, existingCol.Name)
	}

	// Dry-run: classify every row's target field by SHAPE (no content
	// dereference, so it is safe even when content chunks / secondary indexes
	// are damaged) and report what the real run would do, without writing.
	if dryRun {
		counts, scanned, err := classifyColumnForMigration(ctx, tbl, tupleIndex, ddb)
		if err != nil {
			return MigrateAdaptiveResult{}, fmt.Errorf("dry-run scan of %s.%s: %w", resolvedName, existingCol.Name, err)
		}
		outcome := RecoverRowsMigrated
		if oldEnc == newEnc && counts.legacyToRewrite == 0 {
			outcome = RecoverRowsAlreadyOK // already canonical adaptive; real run is a no-op
		}
		return MigrateAdaptiveResult{
			Outcome:              outcome,
			Database:             dbName,
			Table:                resolvedName,
			Column:               existingCol.Name,
			OldEncoding:          oldEnc,
			NewEncoding:          newEnc,
			RowsScanned:          scanned,
			RowsRewritten:        counts.legacyToRewrite, // the planned rewrite count
			DryRun:               true,
			ClassLegacyToRewrite: counts.legacyToRewrite,
			ClassAlreadyAdaptive: counts.alreadyAdaptive,
			ClassEmptyOrNull:     counts.emptyOrNull,
			ClassUnknown:         counts.unknown,
		}, nil
	}

	// Keyless tables are supported on the mutating path below (rewriteColumn
	// rehashes their content-derived row ids), EXCEPT when the table has
	// secondary indexes: keyless index entries embed each row's hash id, and
	// re-keying rewritten rows would strand those entries. The check sits
	// after the --dry-run block deliberately — dry-run never mutates, so it
	// stays available everywhere.
	if err := refuseKeylessWithSecondaryIndexes(sch, resolvedName, "migrate-adaptive"); err != nil {
		return MigrateAdaptiveResult{}, err
	}

	// dolt_ignore'd tables (ignored_log, ignored_meta) live only in the working set and
	// are NEVER committed, so their content chunks are not commit-rooted —
	// out-of-band adaptive values would reference chunks the persist's refCheck
	// cannot root (and dolt gc, which roots from commits, can reclaim them). The
	// correct storage policy for these tables is force-INLINE: migrate EVERY
	// text/blob column to adaptive-inline in one atomic pass so the rewritten map
	// references zero content chunks and retains no legacy AddrEnc columns.
	ignored, ignErr := doltdb.IdentifyIgnoredTables(ctx, roots, []doltdb.TableName{tn})
	if ignErr != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("check dolt_ignore status for %s: %w", resolvedName, ignErr)
	}
	if len(ignored) > 0 {
		return migrateIgnoredTableInline(ctx, dEnv, tbl, sch, dbName, resolvedName, tn, roots, ddb)
	}

	// Build the patched schema with the target column flipped to its adaptive
	// sibling. If oldEnc is already adaptive this is a no-op schema change, but
	// we still rebuild so the rewrite's value descriptor is derived consistently.
	newCol := existingCol
	newCol.TypeInfo = existingCol.TypeInfo.WithEncoding(newEnc)
	newCol.Kind = newCol.TypeInfo.NomsKind()

	newSch, err := replaceColumnByTag(sch, newCol)
	if err != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("rebuild schema for %s: %w", resolvedName, err)
	}

	// Rewrite every legacy row's target field forward to canonical adaptive,
	// leaving already-adaptive rows verbatim. Sibling AddrEnc fields with
	// non-canonical bytes are healed to canonical legacy in the same pass (the
	// direction-agnostic collateral heal), so the new tuple serializes cleanly.
	rewritten, scanned, _, err := rewriteColumn(ctx, tbl, tupleIndex, newSch, ddb, resolveFieldToAdaptive)
	if err != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("rewrite rows of %s.%s: %w", resolvedName, existingCol.Name, err)
	}

	// No-op detection: if the schema tag is already adaptive AND not a single
	// row changed, the column is already fully canonical adaptive — return a
	// successful idempotent no-op without a commit.
	if oldEnc == newEnc && rewritten.RowsRewritten == 0 {
		return MigrateAdaptiveResult{
			Outcome:     RecoverRowsAlreadyOK,
			Database:    dbName,
			Table:       resolvedName,
			Column:      existingCol.Name,
			OldEncoding: oldEnc,
			NewEncoding: newEnc,
			RowsScanned: scanned,
		}, nil
	}

	updatedTbl, err := tbl.UpdateSchema(ctx, newSch)
	if err != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("apply schema change to %s: %w", resolvedName, err)
	}
	updatedTbl, err = updatedTbl.UpdateRows(ctx, durable.IndexFromProllyMap(rewritten.Map))
	if err != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("apply rewritten rows to %s: %w", resolvedName, err)
	}

	newWorking, err := roots.Working.PutTable(ctx, tn, updatedTbl)
	if err != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("put updated table %s: %w", resolvedName, err)
	}
	roots.Working = newWorking

	rowsRewritten := rewritten.RowsRewritten
	collateral := rewritten.CollateralRewrites
	merged := rewritten.RowsMerged
	var commitMsg string
	if collateral > 0 {
		commitMsg = fmt.Sprintf(
			"admin: schema-encoding-drift migrate-adaptive on %s.%s (%d legacy rows rewritten to adaptive format + schema flipped to %s; %d rows had non-target AddrEnc fields healed as collateral; content unchanged)",
			resolvedName, existingCol.Name, rowsRewritten, encodingName(newEnc), collateral)
	} else {
		commitMsg = fmt.Sprintf(
			"admin: schema-encoding-drift migrate-adaptive on %s.%s (%d legacy rows rewritten to adaptive format + schema flipped to %s; content unchanged)",
			resolvedName, existingCol.Name, rowsRewritten, encodingName(newEnc))
	}
	if merged > 0 {
		commitMsg += fmt.Sprintf(" [%d duplicate keyless rows merged by cardinality]", merged)
	}

	// Non-ignored (committed) table: commit as usual so the heal lands in
	// history. (dolt_ignore'd tables took the force-inline path above and
	// returned before reaching here.)
	roots.Staged = newWorking
	commitHash, err := commitWorkingRoot(ctx, dEnv, headRef, roots, commitMsg)
	if err != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("commit migrate-adaptive: %w", err)
	}

	return MigrateAdaptiveResult{
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

// persistWorkingSetRoot updates the current working set's working root to
// |working| WITHOUT creating a dolt commit. This is the persistence path for
// dolt_ignore'd tables, whose data lives only in the working set.
func persistWorkingSetRoot(ctx context.Context, dEnv *env.DoltEnv, working doltdb.RootValue) error {
	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return fmt.Errorf("get working set: %w", err)
	}
	return dEnv.UpdateWorkingSet(ctx, ws.WithWorkingRoot(working))
}

// inlineSizeCapBytes is a defensive ceiling on the content we force-inline on a
// dolt_ignore'd table. The census shows ignored_*/ignored_meta content is ≤~200B, so this
// should never trip; if it does we still inline (one large row beats a dangling
// out-of-band ref) but record the max so the operator is warned.
const inlineSizeCapBytes = 16 * 1024

// readContentAuthoritative reads the content addressed by |h| using the
// all-generation read path (vs.ReadBytes sees oldgen+newgen+memtables; ddb.Has
// can false-negative per a review finding, so it must NOT be used for a
// present/absent decision). It distinguishes THREE outcomes — critically, a read
// ERROR is NOT the same as an absence:
//   - (content, true, nil)  — the chunk exists; content returned.
//   - (nil, false, nil)     — the chunk is GENUINELY ABSENT: vs.ReadBytes hit the
//     NodeStore's "empty chunk" assertion (a missing chunk), which we recover so
//     we never crash. Safe for callers to treat as "not a legacy hash".
//   - (nil, false, err)     — a READ ERROR (transient or otherwise). The caller
//     MUST fail loud and must NOT treat this as an absence — doing so could
//     misclassify a present legacy row as adaptive-inline and corrupt it.
func readContentAuthoritative(ctx context.Context, vs val.ValueStore, h hash.Hash) (content []byte, present bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Recovered the empty-chunk assertion ⇒ GENUINE absence, not an error.
			content, present, err = nil, false, nil
		}
	}()
	b, rErr := vs.ReadBytes(ctx, h)
	if rErr != nil {
		// A read error is NOT an absence — propagate it so the caller fails loud.
		return nil, false, rErr
	}
	return b, true, nil
}

// underlyingContent extracts the raw content bytes a value-tuple field
// represents, regardless of its on-disk shape, using authoritative reads:
//   - legacy raw 20-byte hash → ReadBytes the chunk.
//   - adaptive addressed [varint][20-byte hash] → ReadBytes the chunk.
//   - adaptive inline [0x00][content] → content = bytes[1:].
//   - 0x00-leading 20-byte (ambiguous) → ReadBytes(hash): present ⇒ legacy hash
//     (use its content); absent ⇒ adaptive-inline (content = bytes[1:]).
//
// Returns present=false with err=nil ONLY when a legacy/addressed hash's content
// chunk is GENUINELY absent (recovered empty-chunk assertion) — the caller FAILS
// LOUD on this (no tolerate-missing; the loss-census confirmed zero absent
// chunks). Returns a non-nil err for a structurally unknown shape OR a chunkstore
// READ ERROR (never silently treated as an absence, which could corrupt a
// present legacy row).
func underlyingContent(ctx context.Context, vs val.ValueStore, b []byte) (content []byte, present bool, err error) {
	if len(b) == 0 {
		return nil, true, nil // NULL — caller maps to NULL
	}
	if b[0] != 0 {
		switch {
		case len(b) == hash.ByteLen:
			// Legacy raw hash: a read error propagates (fail loud); a genuine
			// absence returns present=false (caller fails loud — no
			// tolerate-missing).
			return readContentAuthoritative(ctx, vs, hash.New(b))
		case len(b) > hash.ByteLen:
			_, varintSize := uvarint.Uvarint(b)
			if varintSize > 0 && varintSize+hash.ByteLen == len(b) {
				return readContentAuthoritative(ctx, vs, hash.New(b[varintSize:]))
			}
			return nil, false, fmt.Errorf("field has unknown shape: %d bytes, non-zero leader, malformed adaptive-addressed trailer", len(b))
		default:
			return nil, false, fmt.Errorf("field has unknown shape: %d bytes, non-zero leader, too short", len(b))
		}
	}
	// Leading byte 0x00.
	if len(b) == hash.ByteLen {
		// Ambiguous: a legacy raw hash whose first byte is coincidentally 0x00,
		// or an adaptive-inline value with 19 content bytes. Resolve via an
		// authoritative read — never ddb.Has (a review finding). A READ ERROR
		// must fail loud (not be mistaken for an absence); only a GENUINE absence
		// (recovered empty-chunk assertion) means "adaptive inline".
		c, ok, rErr := readContentAuthoritative(ctx, vs, hash.New(b))
		if rErr != nil {
			return nil, false, fmt.Errorf("authoritative read for 0x00-leading 20-byte field: %w", rErr)
		}
		if ok {
			return c, true, nil // legacy raw hash
		}
		return b[1:], true, nil // adaptive inline (19 content bytes)
	}
	// Adaptive inline: [0x00] (empty) or [0x00][content].
	return b[1:], true, nil
}

// migrateIgnoredTableInline force-inlines EVERY out-of-band-capable column
// (text/blob/json/geometry, legacy or adaptive) of a dolt_ignore'd table to
// adaptive-inline form in ONE atomic pass, then persists the working set.
//
// Why inline-only for ignored tables: their content chunks are never committed
// (bd never `dolt add`s them), so an out-of-band adaptive value would reference
// a chunk the working-set persist's refCheck cannot root — and dolt gc, which
// roots from commits, can reclaim those unrooted chunks. Inlining puts every
// value's content in the tuple itself: the rewritten map references ZERO content
// chunks and retains no legacy AddrEnc columns, so refCheck has nothing to
// dangle on and the data is immune to the gc hazard.
func migrateIgnoredTableInline(ctx context.Context, dEnv *env.DoltEnv, tbl *doltdb.Table, sch schema.Schema, dbName, resolvedName string, tn doltdb.TableName, roots doltdb.Roots, ddb *doltdb.DoltDB) (MigrateAdaptiveResult, error) {
	// The force-inline heal deliberately inlines content of ANY size (gc-safety
	// for never-committed chunks trumps the engine's spill heuristic). On a
	// keyless table that would leave rows whose bytes differ from what the
	// engine's TupleBuilder builds for the same content, so content-derived
	// hash lookups (UPDATE/DELETE) would miss them. Keyless + dolt_ignore'd
	// stays refused until the force-inline path can re-key engine-canonically.
	if schema.IsKeyless(sch) {
		return MigrateAdaptiveResult{}, fmt.Errorf("table %s is keyless and dolt_ignore'd; the force-inline heal cannot produce engine-canonical keyless rows (force-inlining diverges from the engine's spill heuristic, breaking content-derived hash lookups)", resolvedName)
	}

	// Identify every non-PK, non-virtual out-of-band-capable column and its
	// target adaptive encoding; build the patched schema flipping them all.
	targetIdx := map[int]bool{}
	var colNames []string
	newSch := sch
	var buildErr error
	_ = sch.GetNonPKCols().Iter(func(tag uint64, col schema.Column) (bool, error) {
		if col.Virtual {
			return false, nil
		}
		enc := col.TypeInfo.Encoding()
		var newEnc val.Encoding
		switch {
		case IsAdaptiveEncoding(enc):
			newEnc = enc // already adaptive; rows still force-inlined for gc-safety
		case IsLegacyEncoding(enc):
			sib, ok := AdaptiveSibling(enc)
			if !ok {
				return false, nil
			}
			newEnc = sib
		default:
			return false, nil // not out-of-band-capable
		}
		idx, ok := valueTupleIndexForColumn(sch, col.Name)
		if !ok {
			return false, nil
		}
		targetIdx[idx] = true
		colNames = append(colNames, col.Name)
		nc := col
		nc.TypeInfo = col.TypeInfo.WithEncoding(newEnc)
		nc.Kind = nc.TypeInfo.NomsKind()
		newSch, buildErr = replaceColumnByTag(newSch, nc)
		if buildErr != nil {
			return true, buildErr
		}
		return false, nil
	})
	if buildErr != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("rebuild schema for %s: %w", resolvedName, buildErr)
	}
	if len(targetIdx) == 0 {
		// No out-of-band columns — nothing to inline.
		return MigrateAdaptiveResult{Outcome: RecoverRowsAlreadyOK, Database: dbName, Table: resolvedName}, nil
	}

	durableIdx, err := tbl.GetRowData(ctx)
	if err != nil {
		return MigrateAdaptiveResult{}, err
	}
	pm, err := durable.ProllyMapFromIndex(durableIdx)
	if err != nil {
		return MigrateAdaptiveResult{}, err
	}
	ns := pm.NodeStore()
	pool := pm.Pool()
	newValDesc := newSch.GetValueDescriptor(ns)
	mut := pm.Rewriter(pm.KeyDesc(), newValDesc)
	keyDesc := pm.KeyDesc()

	empty, err := durableIdx.Empty()
	if err != nil {
		return MigrateAdaptiveResult{}, err
	}
	scanned, rewritten, maxLen, inlineCount := 0, 0, 0, 0
	if !empty {
		iter, err := pm.IterAll(ctx)
		if err != nil {
			return MigrateAdaptiveResult{}, err
		}
		for {
			key, value, iterErr := iter.Next(ctx)
			if iterErr == io.EOF {
				break
			}
			if iterErr != nil {
				return MigrateAdaptiveResult{}, iterErr
			}
			scanned++
			fieldCount := value.Count()
			if dc := newValDesc.Count(); dc > fieldCount {
				fieldCount = dc
			} else if value.Count() > dc {
				return MigrateAdaptiveResult{}, fmt.Errorf("schema-row width mismatch at %s: tuple has %d fields, descriptor expects %d (concurrent schema change?)", formatKeyForError(key, keyDesc), value.Count(), dc)
			}
			fields := make([][]byte, fieldCount)
			rowChanged := false
			for i := 0; i < fieldCount; i++ {
				orig := value.GetField(i)
				if !targetIdx[i] {
					fields[i] = orig
					continue
				}
				if len(orig) == 0 {
					fields[i] = nil // NULL stays NULL
					continue
				}
				content, ePresent, cErr := underlyingContent(ctx, ns, orig)
				if cErr != nil {
					return MigrateAdaptiveResult{}, fmt.Errorf("row %s, field %d: %w", formatKeyForError(key, keyDesc), i, cErr)
				}
				if !ePresent {
					// A legacy/addressed hash whose content chunk is genuinely
					// absent — the authoritative all-generation read failed (the
					// NodeStore empty-chunk assertion, recovered in
					// readContentAuthoritative, so we do NOT crash). This is real
					// data loss: FAIL LOUD with the row's primary key rather than
					// silently emptying it. We do not tolerate-missing — the
					// loss-census confirmed zero genuinely-absent chunks, so this
					// must never trigger; if it does, the operator investigates
					// before healing.
					return MigrateAdaptiveResult{}, fmt.Errorf(
						"row %s, field %d: content chunk is genuinely absent (authoritative read failed); refusing to silently empty it — investigate before healing %s",
						formatKeyForError(key, keyDesc), i, resolvedName)
				}
				if len(content) > maxLen {
					maxLen = len(content)
				}
				inlined := val.AdaptiveValueInlineBytes(content)
				// [invariant — arch-detective] force-inline MUST produce an
				// inline adaptive value (first byte 0x00) — never an out-of-line
				// ref — so the rewritten map references zero content chunks and
				// cannot dangle on the ignored-table persist. Assert it at the
				// source (fail-fast) rather than relying only on a test.
				if len(inlined) == 0 || inlined[0] != 0x00 {
					return MigrateAdaptiveResult{}, fmt.Errorf("internal invariant violated: force-inline produced a non-inline value (len=%d) at row %s field %d", len(inlined), formatKeyForError(key, keyDesc), i)
				}
				fields[i] = inlined
				inlineCount++
				if !bytes.Equal(inlined, orig) {
					rowChanged = true
				}
			}
			if rowChanged {
				rewritten++
			}
			if putErr := mut.Put(ctx, key, val.NewTuple(pool, fields...)); putErr != nil {
				return MigrateAdaptiveResult{}, fmt.Errorf("put row %s: %w", formatKeyForError(key, keyDesc), putErr)
			}
		}
	}

	newMap, err := mut.Map(ctx)
	if err != nil {
		return MigrateAdaptiveResult{}, err
	}
	updatedTbl, err := tbl.UpdateSchema(ctx, newSch)
	if err != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("apply schema to %s: %w", resolvedName, err)
	}
	updatedTbl, err = updatedTbl.UpdateRows(ctx, durable.IndexFromProllyMap(newMap))
	if err != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("apply rows to %s: %w", resolvedName, err)
	}
	newWorking, err := roots.Working.PutTable(ctx, tn, updatedTbl)
	if err != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("put table %s: %w", resolvedName, err)
	}
	roots.Working = newWorking
	// dolt_ignore'd table: persist the working set (no commit; bd reads here).
	if err := persistWorkingSetRoot(ctx, dEnv, roots.Working); err != nil {
		return MigrateAdaptiveResult{}, fmt.Errorf("persist working set for dolt_ignore'd table %s: %w", resolvedName, err)
	}

	msg := fmt.Sprintf("admin: schema-encoding-drift migrate-adaptive (inline) on dolt_ignore'd %s [%d cols]: %d/%d rows force-inlined to adaptive; content unchanged", resolvedName, len(colNames), rewritten, scanned)
	return MigrateAdaptiveResult{
		Outcome:        RecoverRowsMigrated,
		Database:       dbName,
		Table:          resolvedName,
		Column:         fmt.Sprintf("%v (force-inline, dolt_ignore'd table)", colNames),
		RowsScanned:    scanned,
		RowsRewritten:  rewritten,
		CommitMessage:  msg,
		MaxContentLen:  maxLen,
		InlineCount:    inlineCount,
		AddressedCount: 0, // force-inline guarantees zero out-of-band values
	}, nil
}

// migrationClassCounts tallies a column's value-tuple fields by migration class
// for the --dry-run report.
type migrationClassCounts struct {
	legacyToRewrite int // FieldLegacyRawHash — rewritten forward to adaptive
	alreadyAdaptive int // adaptive inline (non-empty) / addressed — pass through
	emptyOrNull     int // NULL or empty-inline [0x00] — no-op
	unknown         int // unclassifiable — the real run would error
}

// classifyColumnForMigration walks every row and classifies the target field by
// SHAPE only (via the chunkstore-aware classifier; it never dereferences
// content), so it is safe to run on a column whose content chunks or secondary
// indexes are damaged. It powers --dry-run.
func classifyColumnForMigration(ctx context.Context, tbl *doltdb.Table, tupleIndex int, cs ChunkPresenceChecker) (counts migrationClassCounts, scanned int, err error) {
	durableIdx, err := tbl.GetRowData(ctx)
	if err != nil {
		return counts, 0, err
	}
	if durableIdx == nil {
		return counts, 0, nil
	}
	pm, err := durable.ProllyMapFromIndex(durableIdx)
	if err != nil {
		return counts, 0, err
	}
	iter, err := pm.IterAll(ctx)
	if err != nil {
		return counts, 0, err
	}
	for {
		_, value, iterErr := iter.Next(ctx)
		if iterErr == io.EOF {
			break
		}
		if iterErr != nil {
			return counts, scanned, iterErr
		}
		scanned++
		b := value.GetField(tupleIndex)
		if len(b) == 0 {
			counts.emptyOrNull++
			continue
		}
		f, cErr := ClassifyFieldWithChunkstore(ctx, cs, b)
		if cErr != nil {
			return counts, scanned, cErr
		}
		switch f {
		case FieldLegacyRawHash:
			counts.legacyToRewrite++
		case FieldAdaptiveInline:
			if len(b) == 1 { // [0x00] — empty inline
				counts.emptyOrNull++
			} else {
				counts.alreadyAdaptive++
			}
		case FieldAdaptiveAddressed:
			counts.alreadyAdaptive++
		case FieldNULL:
			counts.emptyOrNull++
		default:
			counts.unknown++
		}
	}
	return counts, scanned, nil
}

// resolveFieldToAdaptive is the per-row workhorse for the forward migration. It
// is the inverse of resolveFieldToLegacy: given a single field's bytes, it
// returns the canonical ADAPTIVE form of those bytes plus:
//   - isLegacyRow: the input was a legacy-raw-hash value that this call
//     converted to adaptive. The caller ORs these into sawAnyLegacy.
//   - changed: the returned bytes are NOT byte-identical to the input.
//
// Errors mirror resolveFieldToLegacy: unknown shapes, and chunkstore read
// failures (a legacy hash whose chunk is missing).
func resolveFieldToAdaptive(ctx context.Context, b []byte, vs val.ValueStore, cs ChunkPresenceChecker) (newField []byte, isLegacyRow bool, changed bool, err error) {
	if len(b) == 0 {
		// NULL: same in both formats.
		return nil, false, false, nil
	}

	if b[0] != 0 {
		// Non-zero leading byte: either legacy raw hash (exactly 20 bytes) or an
		// already-canonical adaptive-addressed value (varint length + 20 hash
		// bytes, total >= 21).
		switch {
		case len(b) == hash.ByteLen:
			// Legacy raw hash. Dereference and re-encode as adaptive, reusing
			// the existing content chunk (b is its 20-byte address).
			content, rErr := vs.ReadBytes(ctx, hash.New(b))
			if rErr != nil {
				return nil, false, false, fmt.Errorf("read legacy content for 20-byte hash: %w", rErr)
			}
			return encodeContentAsAdaptive(content, b), true, true, nil
		case len(b) > hash.ByteLen:
			declaredLen, varintSize := uvarint.Uvarint(b)
			if varintSize > 0 && varintSize+hash.ByteLen == len(b) {
				_ = declaredLen
				// Already a canonical adaptive-addressed value. Leave verbatim.
				return b, false, false, nil
			}
			return nil, false, false, fmt.Errorf("field has unknown shape: %d bytes, non-zero leader, malformed adaptive-addressed trailer", len(b))
		default:
			return nil, false, false, fmt.Errorf("field has unknown shape: %d bytes, non-zero leader, too short for any known encoding", len(b))
		}
	}

	// Leading byte is 0x00 — structurally adaptive inline, but a 20-byte field
	// could be a legacy raw hash whose first byte is coincidentally 0x00
	// (~1/256). Disambiguate via an AUTHORITATIVE read (vs.ReadBytes,
	// all-generation; recover-wrapped), NOT cs.Has — so the present/absent
	// decision and the content read are consistent by construction and this
	// path carries no dependence on ddb.Has completeness (the fragility class
	// a review flagged, even though ddb.Has is correct at runtime).
	if len(b) == hash.ByteLen {
		content, ok, rErr := readContentAuthoritative(ctx, vs, hash.New(b))
		if rErr != nil {
			// A READ ERROR is not an absence — fail loud rather than silently
			// treat this as adaptive-inline (which would inline the 20 hash
			// bytes as if they were content and corrupt a present legacy row).
			return nil, false, false, fmt.Errorf("authoritative read for 0x00-leading 20-byte field: %w", rErr)
		}
		if ok {
			// Legacy raw hash with a coincidental 0x00 leader (chunk present).
			// Re-encode as adaptive, reusing the existing content chunk.
			return encodeContentAsAdaptive(content, b), true, true, nil
		}
		// Chunk GENUINELY absent ⇒ a real adaptive-inline value with 19 content
		// bytes, not a legacy hash. Fall through to keep it verbatim.
	}

	// Genuine adaptive inline (`[0x00][content]`). Already canonical — verbatim.
	return b, false, false, nil
}

// maxAdaptiveVarIntLen is the maximum width of a SQLite4 variable-length int as
// produced by uvarint.Encode (mirrors the unexported val.maxVarIntLength).
const maxAdaptiveVarIntLen = 9

// encodeContentAsAdaptive encodes raw content (already stored out-of-band at
// |origHash|) into a canonical adaptive value, matching dolt's per-value
// inline-vs-out-of-band threshold and writing NO new chunk:
//   - content of 20 bytes or fewer (including empty) inlines as `[0x00][content]`
//     — inlining is never larger than the 20-byte address it would replace, and
//     the (now-unreferenced) source chunk is reclaimed by a later `dolt gc`.
//   - larger content stays out-of-band as `[varint(len)][origHash]`, REUSING the
//     existing content chunk verbatim (its hash is unchanged). No blob is
//     rewritten and no new chunk enters the commit's novelty set — which both
//     preserves data exactly and avoids dangling-reference faults on tables
//     whose content chunks are not independently rooted.
//
// Because every produced value is at most 1+20 (inline) or varint+20 (OOB)
// bytes — never materially larger than the legacy 20-byte address it replaces —
// the rebuilt value tuple can never overflow the prolly tuple size target, so no
// per-tuple out-of-band reshuffle is required.
func encodeContentAsAdaptive(content []byte, origHash []byte) []byte {
	if len(content) <= hash.ByteLen {
		// Inline. AdaptiveValueInlineBytes handles empty content as `[0x00]`.
		return val.AdaptiveValueInlineBytes(content)
	}
	// Out-of-band, reusing the existing chunk: [varint(len)][origHash].
	buf := make([]byte, maxAdaptiveVarIntLen)
	n := uvarint.Encode(buf, uint64(len(content)))
	out := make([]byte, n+hash.ByteLen)
	copy(out, buf[:n])
	copy(out[n:], origHash)
	return out
}

// emitMigrateAdaptiveResult writes a human-readable summary of the migration.
func emitMigrateAdaptiveResult(w io.Writer, r MigrateAdaptiveResult) {
	if r.DryRun {
		action := fmt.Sprintf("would rewrite %d legacy row(s) → adaptive and flip schema %s → %s",
			r.ClassLegacyToRewrite, encodingName(r.OldEncoding), encodingName(r.NewEncoding))
		if r.Outcome == RecoverRowsAlreadyOK {
			action = "no-op (already canonical adaptive; nothing to rewrite)"
		}
		_, _ = fmt.Fprintf(w, "[DRY RUN] %s.%s (%d rows scanned): %s\n", r.Table, r.Column, r.RowsScanned, action)
		_, _ = fmt.Fprintf(w, "  legacy→rewrite: %d   already-adaptive: %d   empty/null: %d   unknown: %d\n",
			r.ClassLegacyToRewrite, r.ClassAlreadyAdaptive, r.ClassEmptyOrNull, r.ClassUnknown)
		if r.ClassUnknown > 0 {
			_, _ = fmt.Fprintf(w, "  WARNING: %d unclassifiable row(s); the real run would abort on these.\n", r.ClassUnknown)
		}
		return
	}
	switch r.Outcome {
	case RecoverRowsMigrated:
		if r.CommitHash == "" {
			// dolt_ignore'd force-inline path: persisted to the working set, no commit.
			_, _ = fmt.Fprintf(w, "migrate-adaptive (inline) %s: %d/%d rows force-inlined to adaptive (dolt_ignore'd table; working-set persist, no commit)\n",
				r.Table, r.RowsRewritten, r.RowsScanned)
			_, _ = fmt.Fprintf(w, "  columns: %s\n", r.Column)
			_, _ = fmt.Fprintf(w, "  storage: %d values inline, %d addressed (addressed MUST be 0 for a dolt_ignore'd table)\n", r.InlineCount, r.AddressedCount)
			if r.MaxContentLen > inlineSizeCapBytes {
				_, _ = fmt.Fprintf(w, "  WARNING: largest inlined content is %d bytes (> %d-byte soft cap)\n", r.MaxContentLen, inlineSizeCapBytes)
			}
			_, _ = fmt.Fprintf(w, "  message: %s\n", r.CommitMessage)
			return
		}
		_, _ = fmt.Fprintf(w, "migrate-adaptive %s.%s: %d/%d legacy rows rewritten, schema %s → %s\n",
			r.Table, r.Column, r.RowsRewritten, r.RowsScanned,
			encodingName(r.OldEncoding), encodingName(r.NewEncoding))
		if r.CollateralRewrites > 0 {
			_, _ = fmt.Fprintf(w, "  %d rows had non-target AddrEnc fields healed as collateral\n", r.CollateralRewrites)
		}
		if r.RowsMerged > 0 {
			_, _ = fmt.Fprintf(w, "  %d duplicate keyless rows merged by cardinality\n", r.RowsMerged)
		}
		_, _ = fmt.Fprintf(w, "  commit: %s\n", r.CommitHash)
		_, _ = fmt.Fprintf(w, "  message: %s\n", r.CommitMessage)
	case RecoverRowsAlreadyOK:
		_, _ = fmt.Fprintf(w, "no-op for %s.%s: already canonical adaptive at %s (idempotent success)\n",
			r.Table, r.Column, encodingName(r.OldEncoding))
	default:
	}
}
