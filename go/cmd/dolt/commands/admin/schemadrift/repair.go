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
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

// RepairCmd is the write side of the schema-encoding-drift admin pair. It
// rewrites exactly one column's persisted encoding back to its pre-adaptive
// legacy sibling and records the change as a real dolt commit on the working
// branch — leaving the row data byte-identical.
type RepairCmd struct{}

var _ cli.Command = RepairCmd{}

const (
	tableFlag        = "table"
	colFlag          = "column"
	includeEmptyFlag = "include-empty"
)

var repairDocs = cli.CommandDocumentationContent{
	ShortDesc: "Repair a single column's drifted persisted-encoding tag",
	LongDesc: `Atomically flips the persisted encoding of one column from its drifted adaptive tag (e.g. ` + "`StringAdaptiveEnc`" + `) back to the matching legacy address tag (e.g. ` + "`StringAddrEnc`" + `) so that on-disk row data and schema-record agree.

Row data is NOT modified. The bytes the column held before the repair are exactly the bytes it holds after — repair only changes how the schema TELLS the reader to interpret them.

The repair is refused (with a clear error and a non-zero exit code) when the column's first non-NULL row is in adaptive format — that means the column is internally consistent and a "repair" would corrupt it.

Each repair is committed as its own dolt commit with an auto-generated message of the form ` + "`admin: schema-encoding-drift repair on T.C (declared <adaptive> → restored <legacy>; data unchanged)`" + ` so the operation is recoverable via ` + "`dolt log`" + `.

Required flags: ` + "`--table`" + ` and ` + "`--column`" + ` — one column per invocation, to keep each commit a single auditable change.`,
	Synopsis: []string{"--table <name> --column <name>"},
}

// Name returns the subcommand name as registered in admin's command list.
func (cmd RepairCmd) Name() string { return "repair" }

// Description is the short summary shown in `dolt admin` help.
func (cmd RepairCmd) Description() string {
	return "Repair a single column's drifted persisted-encoding tag (atomic per-column flip + dolt commit)"
}

// RequiresRepo is true: we need an initialized dolt environment to write to.
func (cmd RepairCmd) RequiresRepo() bool { return true }

// Docs is the full documentation surfaced by `dolt admin schema-encoding-drift repair --help`.
func (cmd RepairCmd) Docs() *cli.CommandDocumentation {
	return cli.NewCommandDocumentation(repairDocs, cmd.ArgParser())
}

// ArgParser declares the flag surface.
func (cmd RepairCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsString(tableFlag, "", "name", "the table to repair")
	ap.SupportsString(colFlag, "", "name", "the column whose drifted encoding tag should be restored to the legacy sibling")
	ap.SupportsFlag(includeEmptyFlag, "", "Also accept the homogeneous-empty bucket as a valid repair target. By default repair refuses any column without strong legacy-raw-hash evidence; --include-empty trusts a forensic verdict that an all-zero payload on an adaptive schema is safe to flip back to legacy.")
	return ap
}

// Hidden mirrors the rest of the admin commands.
func (cmd RepairCmd) Hidden() bool { return true }

// Exec is the CLI entry point.
func (cmd RepairCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, _ cli.CliContext) int {
	ap := cmd.ArgParser()
	usage, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, repairDocs, ap))
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

	includeEmpty := apr.Contains(includeEmptyFlag)
	result, err := repairColumnWithOptions(ctx, dEnv, tableName, colName, includeEmpty)
	if err != nil {
		verr := errhand.BuildDError("repair failed").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	emitRepairResult(cli.OutStream, result)
	return 0
}

// RepairOutcome enumerates the four terminal states of a repair call:
//
//   - OutcomeFlipped     — the column's encoding was an adaptive tag with a
//     legacy on-disk payload; we flipped the tag and committed.
//   - OutcomeAlreadyOK   — the column is already at a legacy encoding; nothing
//     to do. Returned as a successful no-op so scripted repair loops are
//     idempotent.
//   - OutcomeNotAdaptive — the column has neither an adaptive nor a legacy
//     encoding (e.g. an INT column was passed); refuse with a clear error
//     rather than corrupting an unrelated schema record.
//   - OutcomeGenuineAdaptive — the column's first non-NULL row is in adaptive
//     format. The column is internally consistent. Refusing protects against
//     the catastrophic "repair caused the corruption" case.
type RepairOutcome int

const (
	OutcomeFlipped RepairOutcome = iota
	OutcomeAlreadyOK
	OutcomeNotAdaptive
	OutcomeGenuineAdaptive
)

// RepairResult is the structured outcome of a repair call, used by the CLI
// emitter and by the integration tests.
type RepairResult struct {
	Outcome           RepairOutcome
	Database          string
	Table             string
	Column            string
	OldEncoding       val.Encoding
	NewEncoding       val.Encoding
	ObservedFormat    FieldFormat
	CommitHash        string // empty for OutcomeAlreadyOK
	CommitMessage     string // empty for OutcomeAlreadyOK
	WitnessRowSampled bool   // true if at least one non-NULL row was inspected
}

// repairColumn invokes repairColumnWithOptions with the default conservative
// posture (includeEmpty=false): refuse the homogeneous-empty bucket. Tests
// and callers that want to accept the empty bucket should call
// repairColumnWithOptions directly.
func repairColumn(ctx context.Context, dEnv *env.DoltEnv, tableName, colName string) (RepairResult, error) {
	return repairColumnWithOptions(ctx, dEnv, tableName, colName, false)
}

// repairColumnWithOptions is the entire repair pipeline. Broken out so unit
// and integration tests can call it without invoking the CLI wrapper. The
// includeEmpty flag toggles whether the homogeneous-empty bucket (the forensic analysis
// "safe-empty" case — every scanned row is NULL, `[0x00]` inline empty, or
// the 20-byte all-zero shape) counts as a valid repair target.
func repairColumnWithOptions(ctx context.Context, dEnv *env.DoltEnv, tableName, colName string, includeEmpty bool) (RepairResult, error) {
	ddb := dEnv.DoltDB(ctx)
	dbName := ddb.GetDatabaseName()

	rsr := dEnv.RepoStateReader()
	headRef, err := rsr.CWBHeadRef(ctx)
	if err != nil {
		return RepairResult{}, fmt.Errorf("get current branch: %w", err)
	}

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return RepairResult{}, fmt.Errorf("get roots: %w", err)
	}

	// Resolve the table case-insensitively against the working root so that
	// `--table issues` works whether the persisted name is `issues`, `Issues`,
	// or `ISSUES`.
	tn := doltdb.TableName{Name: tableName, Schema: doltdb.DefaultSchemaName}
	tbl, resolvedName, ok, err := doltdb.GetTableInsensitive(ctx, roots.Working, tn)
	if err != nil {
		return RepairResult{}, fmt.Errorf("get table %s: %w", tableName, err)
	}
	if !ok {
		return RepairResult{}, fmt.Errorf("table %q not found on current working root", tableName)
	}
	tn.Name = resolvedName

	// repair commits its schema flip, which dangling-faults on a
	// dolt_ignore'd table (never-committed content chunks are not commit-rooted).
	// Refuse such tables and point the operator at the force-inline heal.
	ignored, ignErr := doltdb.IdentifyIgnoredTables(ctx, roots, []doltdb.TableName{tn})
	if ignErr != nil {
		return RepairResult{}, fmt.Errorf("check dolt_ignore status for %s: %w", resolvedName, ignErr)
	}
	if len(ignored) > 0 {
		return RepairResult{}, fmt.Errorf(
			"table %s is dolt_ignore'd; repair commits its schema flip and would dangling-fault on its never-committed content chunks. Use `dolt admin schema-encoding-drift migrate-adaptive --table %s --column <col>` (force-inline heal) instead",
			resolvedName, resolvedName,
		)
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return RepairResult{}, fmt.Errorf("get schema for %s: %w", resolvedName, err)
	}

	// Find the column by case-insensitive name match.
	existingCol, ok := sch.GetAllCols().GetByNameCaseInsensitive(colName)
	if !ok {
		return RepairResult{}, fmt.Errorf("column %q not found in table %s", colName, resolvedName)
	}

	oldEnc := existingCol.TypeInfo.Encoding()

	// Fast-path: already at a legacy encoding. Nothing to do — return success
	// as an idempotent no-op.
	if IsLegacyEncoding(oldEnc) {
		return RepairResult{
			Outcome:     OutcomeAlreadyOK,
			Database:    dbName,
			Table:       resolvedName,
			Column:      existingCol.Name,
			OldEncoding: oldEnc,
			NewEncoding: oldEnc,
		}, nil
	}

	// Refuse columns outside the adaptive family entirely. We never want to
	// silently rewrite an INT or DECIMAL column's TypeInfo encoding — those
	// have fixed encodings by contract and there is no "legacy sibling" to
	// flip to.
	newEnc, hasLegacy := LegacySibling(oldEnc)
	if !IsAdaptiveEncoding(oldEnc) || !hasLegacy {
		return RepairResult{
				Outcome:     OutcomeNotAdaptive,
				Database:    dbName,
				Table:       resolvedName,
				Column:      existingCol.Name,
				OldEncoding: oldEnc,
			}, fmt.Errorf(
				"column %s.%s has encoding %s which is not an adaptive TEXT/BLOB/JSON/GEOMETRY variant; "+
					"there is no legacy sibling to flip to",
				resolvedName, existingCol.Name, encodingName(oldEnc),
			)
	}

	// Locate the column's position in the value tuple so we can inspect the
	// first non-NULL row's raw bytes. PK columns receive forced encodings
	// (VarChar/VarBinary/Cell) on the key side and are not adaptive — so a
	// column reaching this point should never be a PK. Guard defensively.
	if existingCol.IsPartOfPK {
		return RepairResult{
			Outcome:     OutcomeNotAdaptive,
			Database:    dbName,
			Table:       resolvedName,
			Column:      existingCol.Name,
			OldEncoding: oldEnc,
		}, fmt.Errorf("column %s.%s is part of the primary key; PK columns do not carry adaptive encodings", resolvedName, existingCol.Name)
	}

	tupleIndex, ok := valueTupleIndexForColumn(sch, existingCol.Name)
	if !ok {
		return RepairResult{}, fmt.Errorf("column %s.%s does not appear in the value tuple (maybe virtual?)", resolvedName, existingCol.Name)
	}

	// The load-bearing safety check: walk every row and gather DEFINITIVE
	// evidence for both directions plus the homogeneous-empty signal. The
	// five outcomes are:
	//
	//   - definitive-legacy only            → drift confirmed; flip is correct.
	//   - definitive-adaptive only          → genuine adaptive column; refuse.
	//   - both                              → heterogeneous; refuse (row-by-row
	//                                         migration needed, see the empirical
	//                                         `recover-rows` tool).
	//   - neither, but at least one ambiguous-non-empty row was seen
	//                                       → cannot prove drift; refuse.
	//   - neither, AND every row was NULL/empty-inline/all-zero-20-byte
	//                                       → homogeneous-empty; flip iff
	//                                         --include-empty (forensic analysis verdict).
	findings, err := scanColumnFullPayload(ctx, tbl, tupleIndex, ddb)
	if err != nil {
		return RepairResult{}, fmt.Errorf("inspect row payload for %s.%s: %w", resolvedName, existingCol.Name, err)
	}
	switch {
	case findings.sawDefinitiveAdaptive && !findings.sawDefinitiveLegacy:
		return RepairResult{
				Outcome:           OutcomeGenuineAdaptive,
				Database:          dbName,
				Table:             resolvedName,
				Column:            existingCol.Name,
				OldEncoding:       oldEnc,
				ObservedFormat:    FieldAdaptiveAddressed,
				WitnessRowSampled: true,
			}, fmt.Errorf(
				"column %s.%s has genuine adaptive-format data (no legacy rows observed); repair would corrupt it (refusing)",
				resolvedName, existingCol.Name,
			)
	case findings.sawDefinitiveAdaptive && findings.sawDefinitiveLegacy:
		return RepairResult{
				Outcome:           OutcomeGenuineAdaptive,
				Database:          dbName,
				Table:             resolvedName,
				Column:            existingCol.Name,
				OldEncoding:       oldEnc,
				ObservedFormat:    FieldUnknown, // mixed; no single canonical
				WitnessRowSampled: true,
			}, fmt.Errorf(
				"column %s.%s has BOTH legacy AND genuine adaptive rows (heterogeneous payload); repair cannot safely flip the schema in either direction without row-level migration (see `dolt admin schema-encoding-drift recover-rows`)",
				resolvedName, existingCol.Name,
			)
	case findings.sawDefinitiveLegacy && findings.sawNonEmptyAmbiguous:
		// [review] Legacy witnesses coexisting with ambiguous non-empty rows (a
		// 20-byte 0x00-leading value the chunkstore could not confirm as a
		// legacy hash — it may be a genuine adaptive-inline value with 19
		// content bytes). A schema-only flip would strand those rows if they are
		// adaptive. We cannot classify them without a row-level resolve, so
		// repair REFUSES conservatively rather than risk orphaning content.
		return RepairResult{
				Outcome:           OutcomeGenuineAdaptive,
				Database:          dbName,
				Table:             resolvedName,
				Column:            existingCol.Name,
				OldEncoding:       oldEnc,
				ObservedFormat:    FieldUnknown,
				WitnessRowSampled: true,
			}, fmt.Errorf(
				"column %s.%s has legacy witnesses AND ambiguous non-empty 0x00-leading 20-byte rows (the ambiguous rows may be adaptive-inline content a schema-only flip would strand); repair refuses — use `dolt admin schema-encoding-drift recover-rows` (to legacy) or `migrate-adaptive` (to v2 adaptive) for a row-level migration",
				resolvedName, existingCol.Name,
			)
	case !findings.sawDefinitiveLegacy && !findings.sawDefinitiveAdaptive && findings.sawNonEmptyAmbiguous:
		return RepairResult{
				Outcome:           OutcomeGenuineAdaptive,
				Database:          dbName,
				Table:             resolvedName,
				Column:            existingCol.Name,
				OldEncoding:       oldEnc,
				ObservedFormat:    FieldUnknown,
				WitnessRowSampled: false,
			}, fmt.Errorf(
				"column %s.%s has ambiguous payload (no legacy-raw-hash witnesses, no adaptive-addressed witnesses, but at least one non-trivial non-empty row); cannot prove drift, refusing to repair",
				resolvedName, existingCol.Name,
			)
	case !findings.sawDefinitiveLegacy && !findings.sawDefinitiveAdaptive && !findings.sawNonEmptyAmbiguous:
		// Homogeneous-empty: every scanned row was NULL, inline-empty, or
		// the all-zero 20-byte shape. forensic analysis verified these are safe to flip.
		if !includeEmpty {
			return RepairResult{
					Outcome:           OutcomeGenuineAdaptive,
					Database:          dbName,
					Table:             resolvedName,
					Column:            existingCol.Name,
					OldEncoding:       oldEnc,
					ObservedFormat:    FieldNULL,
					WitnessRowSampled: false,
				}, fmt.Errorf(
					"column %s.%s shows the homogeneous-empty payload pattern (safe-empty bucket); rerun with --include-empty to accept this bucket per the forensic verdict",
					resolvedName, existingCol.Name,
				)
		}
		// fall through to perform the flip with the safe-empty witness.
	}
	// One of:
	//   - sawDefinitiveLegacy && !sawDefinitiveAdaptive  → drift confirmed.
	//   - homogeneous-empty + --include-empty            → operator opted in.
	observed := FieldLegacyRawHash
	if !findings.sawDefinitiveLegacy {
		observed = FieldNULL // safe-empty branch
	}
	sampled := findings.sawDefinitiveLegacy

	// Build the patched schema. We replace the column in place by tag, so
	// column order, indexes, checks, defaults, and constraints all carry over.
	newCol := existingCol
	newCol.TypeInfo = existingCol.TypeInfo.WithEncoding(newEnc)
	newCol.Kind = newCol.TypeInfo.NomsKind()

	newSch, err := replaceColumnByTag(sch, newCol)
	if err != nil {
		return RepairResult{}, fmt.Errorf("rebuild schema for %s: %w", resolvedName, err)
	}

	updatedTbl, err := tbl.UpdateSchema(ctx, newSch)
	if err != nil {
		return RepairResult{}, fmt.Errorf("apply schema change to %s: %w", resolvedName, err)
	}

	newWorking, err := roots.Working.PutTable(ctx, tn, updatedTbl)
	if err != nil {
		return RepairResult{}, fmt.Errorf("put updated table %s: %w", resolvedName, err)
	}
	roots.Working = newWorking
	roots.Staged = newWorking

	// Commit the change as a single audit-loggable dolt commit. We bypass the
	// SQL queryist deliberately: SQL ALTER is the path that produced this
	// corruption, so we're routing around it on principle.
	commitMsg := fmt.Sprintf(
		"admin: schema-encoding-drift repair on %s.%s (declared %s → restored %s; data unchanged)",
		resolvedName, existingCol.Name, encodingName(oldEnc), encodingName(newEnc),
	)
	commitHash, err := commitWorkingRoot(ctx, dEnv, headRef, roots, commitMsg)
	if err != nil {
		return RepairResult{}, fmt.Errorf("commit repair: %w", err)
	}

	return RepairResult{
		Outcome:           OutcomeFlipped,
		Database:          dbName,
		Table:             resolvedName,
		Column:            existingCol.Name,
		OldEncoding:       oldEnc,
		NewEncoding:       newEnc,
		ObservedFormat:    observed,
		CommitHash:        commitHash.String(),
		CommitMessage:     commitMsg,
		WitnessRowSampled: sampled,
	}, nil
}

// valueTupleIndexForColumn returns the column's index in the value tuple, or
// (-1, false) if the column is a PK, virtual, or absent. Order matches
// schemaImpl.GetValueDescriptor: NonPKCols.Iter, skipping Virtual, with the
// keyless cardinality field occupying index 0 on keyless tables.
func valueTupleIndexForColumn(sch schema.Schema, colName string) (int, bool) {
	idx := -1
	i := 0
	// Keyless tables prepend a cardinality field to the value tuple, so user
	// columns start at index 1. Without this offset the payload scan reads a
	// neighboring field and refuses an otherwise-valid repair as ambiguous.
	if schema.IsKeyless(sch) {
		i = 1
	}
	_ = sch.GetNonPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.Virtual {
			return false, nil
		}
		if equalsCaseInsensitive(col.Name, colName) {
			idx = i
			return true, nil
		}
		i++
		return false, nil
	})
	return idx, idx >= 0
}

// equalsCaseInsensitive matches the column-resolver semantics used elsewhere
// in dolt (schema lookups are case-insensitive).
func equalsCaseInsensitive(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// payloadFindings captures the per-column verdict the repair safety guard
// derives from a full row-by-row walk. The four fields are independent
// flags that the call site composes into one of five repair outcomes:
//
//	| legacy | adaptive | nonEmptyAmbiguous | verdict                |
//	|--------|----------|-------------------|------------------------|
//	| true   | false    | n/a               | drift → flip is safe   |
//	| false  | true     | n/a               | clean → refuse         |
//	| true   | true     | n/a               | heterogeneous → refuse |
//	| false  | false    | true              | unknown → refuse       |
//	| false  | false    | false             | homogeneous-empty →    |
//	|        |          |                   | flip iff --include-empty|
//
// The "nonEmptyAmbiguous" signal is set when we see any row that's
// neither strong legacy nor strong adaptive AND not consistent with the
// homogeneous-empty pattern (i.e. it's a real non-trivial inline value, a
// 20-byte 0x00-leading hash with chunkstore miss but non-zero content
// bytes, or FieldUnknown). It exists to distinguish "we couldn't tell
// because the column was empty everywhere" (safe-empty) from "we couldn't
// tell because the rows had weird shapes" (refuse).
type payloadFindings struct {
	sawDefinitiveLegacy   bool
	sawDefinitiveAdaptive bool
	sawNonEmptyAmbiguous  bool
}

// scanColumnFullPayload walks every row in |tbl|'s row data and returns the
// payloadFindings for the column at |tupleIndex|. See payloadFindings'
// table for how the call site composes the flags into a repair verdict.
//
// This is intentionally more conservative than the check-side scanner, which
// only needs to surface "this column shows drift" as a diagnostic. Repair
// makes a destructive (well, audit-loggable but persistent) change, so it
// requires strictly stronger evidence.
func scanColumnFullPayload(ctx context.Context, tbl *doltdb.Table, tupleIndex int, cs ChunkPresenceChecker) (findings payloadFindings, err error) {
	durableIdx, err := tbl.GetRowData(ctx)
	if err != nil {
		return findings, err
	}
	if durableIdx == nil {
		return findings, nil
	}
	empty, err := durableIdx.Empty()
	if err != nil {
		return findings, err
	}
	if empty {
		return findings, nil
	}
	pm, err := durable.ProllyMapFromIndex(durableIdx)
	if err != nil {
		return findings, err
	}
	iter, err := pm.IterAll(ctx)
	if err != nil {
		return findings, err
	}
	for {
		_, value, iterErr := iter.Next(ctx)
		if iterErr == io.EOF {
			return findings, nil
		}
		if iterErr != nil {
			return findings, iterErr
		}
		b := value.GetField(tupleIndex)
		structural := ClassifyFieldBytes(b)
		switch structural {
		case FieldLegacyRawHash:
			findings.sawDefinitiveLegacy = true
		case FieldAdaptiveAddressed:
			findings.sawDefinitiveAdaptive = true
		case FieldAdaptiveInline:
			if len(b) == hashByteLen {
				if cs != nil {
					// Try the chunkstore — present means it's a legacy raw
					// hash whose leading byte happens to be 0x00.
					h := newHashFromField(b)
					present, hasErr := cs.Has(ctx, h)
					if hasErr != nil {
						return findings, hasErr
					}
					if present {
						findings.sawDefinitiveLegacy = true
						break
					}
				}
				// 20-byte 0x00-leading field with no chunkstore hit (or no
				// chunkstore available): genuinely ambiguous between an
				// adaptive-inline value with 19 content bytes and a legacy
				// raw hash that coincidentally starts 0x00. It is consistent
				// with the homogeneous-empty pattern only if every content
				// byte is also 0x00; any non-zero byte makes it non-trivial.
				if !isAllZeroBytes(b) {
					findings.sawNonEmptyAmbiguous = true
				}
				break
			}
			// len != 20: a 0x00-leading field that is NOT exactly 20 bytes
			// CANNOT be a legacy raw hash (those are always exactly 20 bytes).
			// It is unambiguously an adaptive-inline value. Non-empty content
			// makes it a DEFINITIVE adaptive witness — this is the affected
			// "inline orphan" shape (e.g. `[0x00]<58 bytes>`) that a prior
			// first-witness repair stranded. Counting it definitively is what
			// makes a legacy+inline heterogeneous column correctly refused.
			if len(b) > 1 {
				findings.sawDefinitiveAdaptive = true
			}
			// `[0x00]` (empty inline) is the safe-empty pattern; no flag.
		case FieldNULL:
			// NULL is consistent with safe-empty; no flag.
		case FieldUnknown:
			// Unknown shape is ambiguous and non-trivial.
			findings.sawNonEmptyAmbiguous = true
		}
		// Bail out early if we've seen both — no need to scan further; the
		// outcome is "refuse, heterogeneous column".
		if findings.sawDefinitiveLegacy && findings.sawDefinitiveAdaptive {
			return findings, nil
		}
	}
}

// hashByteLen / newHashFromField are aliased in check.go to keep the scanner
// hot path lean. We use the same helpers here for symmetry.
var _ = hashByteLen // keep referenced when this file is read in isolation

// replaceColumnByTag returns a new schema identical to |sch| except that the
// column with the same tag as |newCol| has been replaced. The replacement
// preserves column ORDER, all indexes, all checks, the collation, and the PK
// ordinals — everything except the column's own TypeInfo.
//
// This is the dolt-internal-only counterpart of replaceColumnInSchema in
// alterschema.go, but without the order/reorder ceremony and without going
// through any ALTER planning. We want exactly the same column collection,
// minus the bug.
func replaceColumnByTag(sch schema.Schema, newCol schema.Column) (schema.Schema, error) {
	cols := sch.GetAllCols().GetColumns()
	replaced := false
	for i := range cols {
		if cols[i].Tag == newCol.Tag {
			cols[i] = newCol
			replaced = true
			break
		}
	}
	if !replaced {
		return nil, fmt.Errorf("column tag %d not found in schema", newCol.Tag)
	}
	newCC := schema.NewColCollection(cols...)

	pkCols := sch.GetPKCols().GetColumns()
	for i := range pkCols {
		if pkCols[i].Tag == newCol.Tag {
			pkCols[i] = newCol
		}
	}
	newPKCC := schema.NewColCollection(pkCols...)

	newSch, err := schema.NewSchema(
		newCC,
		sch.GetPkOrdinals(),
		sch.GetCollation(),
		schema.NewIndexCollection(newCC, newPKCC),
		sch.Checks(),
	)
	if err != nil {
		return nil, err
	}

	// Copy all existing indexes verbatim — the column we're patching is, by
	// contract for this repair, a TEXT / BLOB / JSON / GEOMETRY adaptive
	// column, and adaptive columns can't be part of an index that references
	// them by length / hash directly. So no index-tag rewrite is needed: the
	// tags are unchanged. Re-add the indexes for ColCollection coherence.
	for _, ix := range sch.Indexes().AllIndexes() {
		_, err := newSch.Indexes().AddIndexByColTags(
			ix.Name(),
			ix.IndexedColumnTags(),
			ix.PrefixLengths(),
			schema.IndexProperties{
				IsUnique:           ix.IsUnique(),
				IsSpatial:          ix.IsSpatial(),
				IsFullText:         ix.IsFullText(),
				IsVector:           ix.IsVector(),
				IsUserDefined:      ix.IsUserDefined(),
				Comment:            ix.Comment(),
				Predicate:          ix.Predicate(),
				FullTextProperties: ix.FullTextProperties(),
				VectorProperties:   ix.VectorProperties(),
			},
		)
		if err != nil {
			return nil, fmt.Errorf("re-add index %s: %w", ix.Name(), err)
		}
	}

	return newSch, nil
}

// commitWorkingRoot stages the working root, creates a pending commit, and
// records it on |headRef| atomically with the working-set update. Returns the
// new commit's content hash.
func commitWorkingRoot(ctx context.Context, dEnv *env.DoltEnv, headRef ref.DoltRef, roots doltdb.Roots, message string) (hash.Hash, error) {
	ddb := dEnv.DoltDB(ctx)

	name, email, err := env.GetNameAndEmail(dEnv.GetConfig())
	if err != nil {
		// Fall back to a stable identity if the operator hasn't set
		// user.name / user.email globally — admin-driven schema repairs
		// shouldn't be blocked on git-style config.
		name, email = "dolt-admin", "dolt-admin@localhost"
	}
	meta, err := datas.NewCommitMeta(name, email, message)
	if err != nil {
		return hash.Hash{}, fmt.Errorf("build commit metadata: %w", err)
	}

	ws, err := dEnv.WorkingSet(ctx)
	if errors.Is(err, doltdb.ErrWorkingSetNotFound) {
		wsRef, refErr := ref.WorkingSetRefForHead(headRef)
		if refErr != nil {
			return hash.Hash{}, refErr
		}
		ws = doltdb.EmptyWorkingSet(wsRef)
	} else if err != nil {
		return hash.Hash{}, fmt.Errorf("get working set: %w", err)
	}

	prevWSHash, err := ws.HashOf()
	if err != nil {
		return hash.Hash{}, fmt.Errorf("hash working set: %w", err)
	}

	pending, err := ddb.NewPendingCommit(ctx, roots, nil, false, meta)
	if err != nil {
		return hash.Hash{}, fmt.Errorf("create pending commit: %w", err)
	}

	// After commit, working == staged == HEAD's new root, so the working set
	// itself has no "uncommitted" delta on top of the repair.
	newWS := ws.WithWorkingRoot(roots.Working).WithStagedRoot(roots.Staged).ClearMerge()

	wsMeta := dEnv.NewWorkingSetMeta(message)
	commit, err := ddb.CommitWithWorkingSet(ctx, headRef, ws.Ref(), pending, newWS, prevWSHash, wsMeta, nil)
	if err != nil {
		return hash.Hash{}, fmt.Errorf("commit + update working set: %w", err)
	}

	commitHash, err := commit.HashOf()
	if err != nil {
		return hash.Hash{}, fmt.Errorf("hash new commit: %w", err)
	}
	return commitHash, nil
}

// emitRepairResult writes a human-readable summary of the repair to |w|.
func emitRepairResult(w io.Writer, r RepairResult) {
	switch r.Outcome {
	case OutcomeFlipped:
		_, _ = fmt.Fprintf(w, "repaired %s.%s: %s → %s\n", r.Table, r.Column, encodingName(r.OldEncoding), encodingName(r.NewEncoding))
		_, _ = fmt.Fprintf(w, "  observed payload format: %s\n", r.ObservedFormat.String())
		_, _ = fmt.Fprintf(w, "  commit: %s\n", r.CommitHash)
		_, _ = fmt.Fprintf(w, "  message: %s\n", r.CommitMessage)
	case OutcomeAlreadyOK:
		_, _ = fmt.Fprintf(w, "no-op for %s.%s: already at %s (idempotent success)\n", r.Table, r.Column, encodingName(r.OldEncoding))
	default:
		// OutcomeNotAdaptive / OutcomeGenuineAdaptive surface via the
		// returned error, so the CLI emitter never reaches them here.
	}
}
