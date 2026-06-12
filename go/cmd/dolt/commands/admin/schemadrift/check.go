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
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

// hashByteLen is the address length used by every legacy AddrEnc family. We
// alias it locally so the scanner's hot path doesn't carry an extra import.
const hashByteLen = hash.ByteLen

// newHashFromField wraps hash.New so the scanner can call it without
// pulling the hash import into its inner loop more than once.
func newHashFromField(b []byte) hash.Hash { return hash.New(b) }

// CheckCmd is the read-only side of the schema-encoding-drift admin pair. It
// walks every table on the current working root, identifies columns whose
// persisted TypeInfo.Encoding() is an adaptive variant, and inspects the first
// non-NULL row's field bytes via val.Tuple.GetField (NOT via the adaptive
// decoder, which is exactly what panics on legacy data tagged as adaptive).
type CheckCmd struct{}

var _ cli.Command = CheckCmd{}

const (
	jsonFlag         = "json"
	allDatabasesFlag = "all-databases"
	databaseFlag     = "database"
)

var checkDocs = cli.CommandDocumentationContent{
	ShortDesc: "Detect persisted-schema / on-disk-row encoding drift",
	LongDesc: `Walks every table on the current working root and reports columns whose persisted ` +
		`adaptive encoding tag does not match the on-disk byte layout of the row data.

The intended drift signature: a column whose schema record says ` + "`StringAdaptiveEnc`" + ` / ` +
		"`BytesAdaptiveEnc`" + ` / ` + "`JsonAdaptiveEnc`" + ` / ` + "`GeomAdaptiveEnc`" + ` / ` +
		"`ExtendedAdaptiveEnc`" + ` but whose first row's field bytes are exactly 20 bytes with a non-zero leading byte — the legacy raw-hash address that 1.x ` + "`*AddrEnc`" + ` columns persisted, and which adaptive dispatch panics on with ` + "`invalid hash length: 19`" + `.

This command never invokes adaptive dispatch and never modifies state. It is safe to run against a corrupted database.

Exit codes:
  0  no drift detected, or only adaptive-formatted data on adaptive-tagged columns
  1  at least one column shows the legacy-raw-hash payload under an adaptive tag

Use ` + "`--json`" + ` to emit a structured array suitable for piping to ` + "`schema-encoding-drift repair`" + `.`,
	Synopsis: []string{"[--json]"},
}

// Name returns the subcommand name as registered in admin's command list.
func (cmd CheckCmd) Name() string { return "check" }

// Description is the short summary shown in `dolt admin` help.
func (cmd CheckCmd) Description() string {
	return "Detect persisted-schema / on-disk-row encoding drift"
}

// RequiresRepo is true: we need an initialized dolt environment to read tables.
func (cmd CheckCmd) RequiresRepo() bool { return true }

// Docs is the full command documentation surfaced by `dolt admin schema-encoding-drift check --help`.
func (cmd CheckCmd) Docs() *cli.CommandDocumentation {
	return cli.NewCommandDocumentation(checkDocs, cmd.ArgParser())
}

// ArgParser declares the command's flag surface.
func (cmd CheckCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsFlag(jsonFlag, "", "Emit JSON output instead of a human-readable table")
	ap.SupportsFlag(allDatabasesFlag, "", "Walk every database under the dolt data directory (default behaviour; specify --database to target a single DB)")
	ap.SupportsString(databaseFlag, "", "name", "Restrict the scan to a single database by name. Mutually exclusive with --all-databases.")
	return ap
}

// Hidden mirrors the rest of the admin commands.
func (cmd CheckCmd) Hidden() bool { return true }

// DriftRow is one persisted-schema / on-disk-row mismatch — or, in the JSON
// case, one row of the report. It also carries the suggested legacy sibling
// encoding so a downstream `repair` invocation can be reconstructed without
// re-deriving it.
//
// The Severity field classifies the row into one of three buckets:
//
//   - "drift"      — strong evidence of legacy raw hashes on an adaptive
//     schema tag. Repair will flip without flags.
//   - "safe-empty" — no strong evidence either way, but the entire scanned
//     payload was structurally consistent with the legacy
//     homogeneous-empty pattern (rows are all NULL, all 0x00
//     inline empties, or ambiguous 20-byte 0x00-leading where
//     the hash is the all-zero/empty-content address). Per
//     a forensic verdict these columns are safe to
//     flip; repair surfaces them only with --include-empty.
//   - (omitted)    — columns that cannot be classified into either bucket
//     are dropped from the report (not surfaced).
//
// The SafeToRepair flag pairs with Severity for JSON consumers piping into
// the repair tool — "drift" rows always set true; "safe-empty" rows set
// true only when repair has been opted into for the empty bucket.
type DriftRow struct {
	Database          string `json:"database"`
	Table             string `json:"table"`
	Column            string `json:"column"`
	DeclaredEncoding  string `json:"declared_encoding"`
	ObservedFormat    string `json:"observed_format"`
	SuggestedEncoding string `json:"suggested_encoding,omitempty"`
	Severity          string `json:"severity"`
	SafeToRepair      bool   `json:"safe_to_repair"`
	// Hint is the literal command an operator should run to remediate this
	// row — e.g. `dolt admin schema-encoding-drift repair --table X --column Y`
	// for a drift row, or the `recover-rows` analogue for a heterogeneous
	// row. Surfaced in both JSON and the TTY table so the next action is
	// always one copy-paste away.
	Hint string `json:"hint,omitempty"`
}

// SeverityDrift is the bucket label for strong-evidence drift rows.
const SeverityDrift = "drift"

// SeveritySafeEmpty is the bucket label for the homogeneous-empty pattern —
// columns where every scanned row was either NULL or the canonical 20-byte
// all-zero shape that arises when a legacy raw hash points at an empty /
// sparse chunk. forensic analysis verified these are safe to flip; repair requires
// --include-empty to act on them.
const SeveritySafeEmpty = "safe-empty"

// SeverityHeterogeneous is the bucket label for columns whose payload mixes
// legacy raw-hash rows and genuine adaptive-format rows. The schema record
// can't be flipped in either direction without corrupting one side; the only
// correct remediation is row-by-row migration. `repair` refuses these;
// `recover-rows` performs the migration.
//
// a later fix/the fix: this bucket exists because the earlier `scanTableForDrift` used to
// short-circuit on the first strong-evidence row and silently misclassify
// heterogeneous columns as either pure drift or pure clean. The empirical analysis
// empirical numbers (real-world TEXT columns all
// heterogeneous) made it clear that operators needed an explicit surface
// for "this column has both kinds of payload and `repair` won't touch it".
const SeverityHeterogeneous = "heterogeneous"

// Exec is the CLI entry point.
func (cmd CheckCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, _ cli.CliContext) int {
	ap := cmd.ArgParser()
	usage, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, checkDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, usage)

	jsonOut := apr.Contains(jsonFlag)
	singleDB, hasSingleDB := apr.GetValue(databaseFlag)
	allDBs := apr.Contains(allDatabasesFlag)

	if hasSingleDB && allDBs {
		verr := errhand.BuildDError("--all-databases and --database are mutually exclusive").SetPrintUsage().Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	// Default posture: walk all sibling databases under the current dEnv's
	// data directory. The single-DB invocation is only used when the operator
	// explicitly opts in via --database.
	walkAll := !hasSingleDB

	drifts, err := scanForDriftMultiDB(ctx, dEnv, walkAll, singleDB)
	if err != nil {
		verr := errhand.BuildDError("failed to scan for schema-encoding drift").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	if jsonOut {
		return emitDriftJSON(cli.OutStream, drifts)
	}
	return emitDriftTable(cli.OutStream, drifts)
}

// scanForDriftMultiDB dispatches the drift scan across one or more databases
// rooted at the dEnv's data directory. When walkAll is true, every sibling
// database under the data directory is scanned; when false, only the database
// matching singleDBName is scanned (case-sensitive against the env name dolt
// derives from the directory). The result is the concatenated drift list
// across all visited databases, sorted by (database, table, column).
//
// Operationally, "data directory" is the parent of the dEnv's working
// directory — that's how the managed-dolt deployment lays out multiple
// databases as siblings: `<datadir>/db1/.dolt`, `<datadir>/db2/.dolt`,
// `<datadir>/db3/.dolt`, etc. The default `MultiEnvForSingleEnv` would
// otherwise iterate inside the current database directory and find nothing
// (only `.dolt`); we explicitly walk the parent so sibling DBs are visible.
func scanForDriftMultiDB(ctx context.Context, dEnv *env.DoltEnv, walkAll bool, singleDBName string) ([]DriftRow, error) {
	mrEnv, err := buildMultiEnvWalkingParent(ctx, dEnv)
	if err != nil {
		return nil, fmt.Errorf("build multi-database env: %w", err)
	}

	var all []DriftRow
	// MultiEnvForDirectory adds the originating dEnv into the iteration AND
	// adds it again under its actual sibling dir name. Dedup by the canonical
	// filesystem path of the `.dolt` directory rather than the database name:
	//
	//   - DoltDB.GetDatabaseName() is path-derived, not symlink-resolved.
	//     If the datadir has `app2 -> db2/`, both surface with distinct
	//     db-name strings even though they're the same underlying physical
	//     database — without canonical-path dedup we'd report every drift
	//     row twice for the symlinked alias.
	//   - The dEnv-added-twice case (the original reason for dedup) is still
	//     caught because both copies resolve to the same canonical .dolt
	//     directory.
	//
	// `filepath.EvalSymlinks` failures (path doesn't exist, permission
	// errors) fall back to the raw db-name key so the dedup degrades to the
	// earlier behaviour rather than crashing the walk.
	visited := map[string]bool{}
	err = mrEnv.Iter(func(name string, sibling *env.DoltEnv) (stop bool, iterErr error) {
		if !walkAll && name != singleDBName {
			return false, nil
		}
		if sibling.DBLoadError != nil {
			// Skip unloadable databases (e.g. a sibling dir without a
			// .dolt subfolder; or a partially-initialised DB). Don't fail
			// the whole walk for an unloadable sibling — record nothing
			// and continue.
			return false, nil
		}
		ddb := sibling.DoltDB(ctx)
		if ddb == nil {
			return false, nil
		}
		key := canonicalDBKey(sibling)
		if visited[key] {
			return false, nil
		}
		visited[key] = true
		rows, scanErr := scanForDrift(ctx, sibling)
		if scanErr != nil {
			return true, fmt.Errorf("scan database %s: %w", name, scanErr)
		}
		all = append(all, rows...)
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Database != all[j].Database {
			return all[i].Database < all[j].Database
		}
		if all[i].Table != all[j].Table {
			return all[i].Table < all[j].Table
		}
		return all[i].Column < all[j].Column
	})
	return all, nil
}

// canonicalDBKey returns a symlink-resolved key for the given dEnv suitable
// for deduplicating two iteration entries that refer to the same underlying
// database. The preferred key is the absolute path of the `.dolt` directory
// after symlink resolution; if that path cannot be derived or resolved
// (e.g. the dEnv has no FS, the path doesn't exist, or EvalSymlinks errors),
// we fall back to `DoltDB.GetDatabaseName()` so the dedup degrades to the
// earlier path-name behaviour rather than crashing the walk.
func canonicalDBKey(dEnv *env.DoltEnv) string {
	if dEnv == nil {
		return ""
	}
	if doltDir := dEnv.GetDoltDir(); doltDir != "" {
		if dEnv.FS != nil {
			if abs, err := dEnv.FS.Abs(doltDir); err == nil {
				if resolved, evalErr := filepath.EvalSymlinks(abs); evalErr == nil {
					return resolved
				}
				return abs
			}
		}
	}
	if ddb := dEnv.DoltDB(context.Background()); ddb != nil {
		return ddb.GetDatabaseName()
	}
	return ""
}

// buildMultiEnvWalkingParent constructs a MultiRepoEnv whose iteration walks
// the *parent* of the current dEnv's FS. This is the layout the managed-dolt
// data dir uses: every database is a sibling subdir
// (`<datadir>/db1/`, `<datadir>/db2/`, ...). MultiEnvForSingleEnv iterates the
// CURRENT dir, which from inside one of these DBs only sees `.dolt`. We
// rebase the FS to the parent so all siblings are discovered.
//
// If the parent cannot be walked (e.g. running from a temp test dir, or
// from a dolt env not laid out as a managed-dolt sibling tree), fall back
// to the default single-env iteration so the command still works for
// developer one-offs.
func buildMultiEnvWalkingParent(ctx context.Context, dEnv *env.DoltEnv) (*env.MultiRepoEnv, error) {
	if dEnv == nil || dEnv.FS == nil {
		return env.MultiEnvForSingleEnv(ctx, dEnv)
	}
	parentFS, err := dEnv.FS.WithWorkingDir("..")
	if err != nil {
		return env.MultiEnvForSingleEnv(ctx, dEnv)
	}
	parentAbs, err := parentFS.Abs("")
	if err != nil {
		return env.MultiEnvForSingleEnv(ctx, dEnv)
	}
	currentAbs, err := dEnv.FS.Abs("")
	if err != nil {
		return env.MultiEnvForSingleEnv(ctx, dEnv)
	}
	// Refuse to walk if "parent" equals current (we're already at FS root)
	// or if it doesn't actually exist.
	if parentAbs == currentAbs {
		return env.MultiEnvForSingleEnv(ctx, dEnv)
	}
	if exists, _ := parentFS.Exists("."); !exists {
		return env.MultiEnvForSingleEnv(ctx, dEnv)
	}
	return env.MultiEnvForDirectory(ctx, parentFS, dEnv)
}

// scanForDrift walks the dolt env's current working root and returns one
// DriftRow per detected (table, column) mismatch. A column is reported when
// either:
//
//   - Its TypeInfo.Encoding() is an adaptive variant AND at least one row's
//     field bytes carry strong evidence of legacy raw-hash format → severity
//     "drift", SafeToRepair=true.
//   - The same encoding gate plus a row payload that is uniformly
//     consistent with the homogeneous-empty pattern (every scanned row is
//     NULL, a one-byte `[0x00]` inline empty, or the structurally-ambiguous
//     20-byte all-zero shape that arises when a legacy hash points at an
//     empty / sparse chunk) AND ZERO rows of definitively-adaptive evidence
//     → severity "safe-empty", SafeToRepair=true. These are the safe-empty bucket
//     for which row-level payload is uniformly the all-zero artefact the
//     adaptive reader synthesises for an empty content blob.
//
// Columns with strong evidence AGAINST drift (any FieldAdaptiveAddressed
// witness) AND no legacy witness are internally consistent — silently
// skipped. Columns with both legacy and adaptive witnesses (heterogeneous
// payloads) are also reported as "drift" with SafeToRepair=true; repair's
// own safety gate independently catches heterogeneous payloads with a
// stricter refusal at write time, so check stays diagnostic and lenient.
//
// The traversal uses includeRootObjects=true so that working-set-only
// tables (those `dolt_ignore`'d or otherwise excluded from the normal
// public schema view) are not silently skipped on the read side. The
// rootValue impl in `go/libraries/doltcore/doltdb/root_val.go` currently
// ignores the flag, but other RootValue implementations honour it; we
// pass the most permissive value to keep the scanner future-proof.
func scanForDrift(ctx context.Context, dEnv *env.DoltEnv) ([]DriftRow, error) {
	ddb := dEnv.DoltDB(ctx)
	dbName := ddb.GetDatabaseName()

	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return nil, fmt.Errorf("read working root: %w", err)
	}

	tblNames, err := root.GetAllTableNames(ctx, true /* includeRootObjects */)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	sort.Slice(tblNames, func(i, j int) bool { return tblNames[i].Name < tblNames[j].Name })

	// Identify dolt_ignore'd tables so hints steer the operator to
	// migrate-adaptive (the safe force-inline heal) rather than repair /
	// recover-rows, both of which commit and would dangling-fault on a
	// never-committed table.
	ignoredSet := map[string]bool{}
	if roots, rErr := dEnv.Roots(ctx); rErr == nil {
		if ign, iErr := doltdb.IdentifyIgnoredTables(ctx, roots, tblNames); iErr == nil {
			for _, t := range ign {
				ignoredSet[t.Name] = true
			}
		}
	}

	var out []DriftRow
	for _, tn := range tblNames {
		tbl, _, err := root.GetTable(ctx, tn)
		if err != nil {
			return nil, fmt.Errorf("get table %s: %w", tn.Name, err)
		}
		if tbl == nil {
			continue
		}
		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return nil, fmt.Errorf("get schema for %s: %w", tn.Name, err)
		}

		// Identify the indices of every adaptive-encoded NON-virtual NON-PK
		// column in the value tuple. We use schema_impl's value-descriptor
		// ordering (NonPKCols.Iter, skipping virtual). PK columns receive
		// adaptive-incompatible encodings (the schema impl forces VarChar /
		// VarBinary / Cell for BLOB / TEXT / GEOMETRY in the key tuple), so
		// they're not drift candidates.
		candidates := collectAdaptiveValueColumns(sch)
		if len(candidates) == 0 {
			continue
		}

		idx, err := tbl.GetRowData(ctx)
		if err != nil {
			return nil, fmt.Errorf("get row data for %s: %w", tn.Name, err)
		}
		if idx == nil {
			continue
		}
		empty, err := idx.Empty()
		if err != nil {
			return nil, fmt.Errorf("check empty for %s: %w", tn.Name, err)
		}
		if empty {
			// Empty table — no payload to compare against the declared
			// encoding. Skip silently; this is not drift.
			continue
		}

		pm, err := durable.ProllyMapFromIndex(idx)
		if err != nil {
			return nil, fmt.Errorf("open prolly map for %s: %w", tn.Name, err)
		}

		buckets, err := scanTableForDrift(ctx, pm, candidates, ddb)
		if err != nil {
			return nil, fmt.Errorf("scan rows of %s: %w", tn.Name, err)
		}

		for _, c := range buckets.drift {
			// On a dolt_ignore'd table, repair (which commits) would
			// dangling-fault — steer to migrate-adaptive only, and don't mark
			// it SafeToRepair.
			hint := fmt.Sprintf("dolt admin schema-encoding-drift repair --table %s --column %s (restore legacy tag), or migrate-adaptive --table %s --column %s (forward to v2-native adaptive)", tn.Name, c.colName, tn.Name, c.colName)
			safeToRepair := true
			if ignoredSet[tn.Name] {
				hint = fmt.Sprintf("dolt admin schema-encoding-drift migrate-adaptive --table %s --column %s (dolt_ignore'd table: repair/recover-rows would dangling-fault; force-inline heal only)", tn.Name, c.colName)
				safeToRepair = false
			}
			out = append(out, DriftRow{
				Database:          dbName,
				Table:             tn.Name,
				Column:            c.colName,
				DeclaredEncoding:  encodingName(c.declared),
				ObservedFormat:    FieldLegacyRawHash.String(),
				SuggestedEncoding: encodingName(c.suggested),
				Severity:          SeverityDrift,
				SafeToRepair:      safeToRepair,
				Hint:              hint,
			})
		}
		for _, c := range buckets.heterogeneous {
			out = append(out, DriftRow{
				Database:          dbName,
				Table:             tn.Name,
				Column:            c.colName,
				DeclaredEncoding:  encodingName(c.declared),
				ObservedFormat:    "mixed-legacy-and-adaptive",
				SuggestedEncoding: encodingName(c.suggested),
				Severity:          SeverityHeterogeneous,
				// SafeToRepair stays FALSE for heterogeneous: `repair` would
				// corrupt one side of the mix in either direction. The only
				// correct remediation is row-level migration. On a
				// dolt_ignore'd table, recover-rows (which commits) would
				// dangling-fault — hint migrate-adaptive only.
				SafeToRepair: false,
				Hint:         heterogeneousHint(ignoredSet[tn.Name], tn.Name, c.colName),
			})
		}
		for _, c := range buckets.safeEmpty {
			out = append(out, DriftRow{
				Database:          dbName,
				Table:             tn.Name,
				Column:            c.colName,
				DeclaredEncoding:  encodingName(c.declared),
				ObservedFormat:    "homogeneous-empty",
				SuggestedEncoding: encodingName(c.suggested),
				Severity:          SeveritySafeEmpty,
				// SafeToRepair stays true: forensic analysis verdict + the structural
				// guarantee that an all-zero 20-byte hash resolves to the
				// empty-chunk address means the flip is safe. Repair gates
				// this bucket behind --include-empty for operator discretion.
				SafeToRepair: true,
				Hint:         fmt.Sprintf("dolt admin schema-encoding-drift repair --table %s --column %s --include-empty", tn.Name, c.colName),
			})
		}
	}

	return out, nil
}

// adaptiveCandidate is a single column flagged as worth inspecting on the
// scan side: it's an adaptive-encoded non-virtual non-PK column, with its
// position in the value tuple precomputed.
type adaptiveCandidate struct {
	colName    string
	colTag     uint64
	tupleIndex int
	declared   val.Encoding
	suggested  val.Encoding // pre-resolved via LegacySibling for the drift report
}

// collectAdaptiveValueColumns returns one candidate per adaptive-encoded
// non-virtual non-PK column, in value-tuple order. Returns nil if none.
func collectAdaptiveValueColumns(sch schema.Schema) []adaptiveCandidate {
	var cands []adaptiveCandidate
	i := 0
	// Keyless tables prepend a cardinality field to the value tuple
	// (schemaImpl.GetValueDescriptor inserts val.KeylessCardType at index 0),
	// so every user column sits one position later than its NonPKCols
	// ordinal. Without this offset the scanner classifies the wrong field's
	// bytes and silently reports drifted keyless tables as clean.
	if schema.IsKeyless(sch) {
		i = 1
	}
	_ = sch.GetNonPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.Virtual {
			return false, nil
		}
		enc := col.TypeInfo.Encoding()
		if IsAdaptiveEncoding(enc) {
			legacy, ok := LegacySibling(enc)
			if ok {
				cands = append(cands, adaptiveCandidate{
					colName:    col.Name,
					colTag:     tag,
					tupleIndex: i,
					declared:   enc,
					suggested:  legacy,
				})
			}
		}
		i++
		return false, nil
	})
	return cands
}

// scanBuckets is the bucketed return of scanTableForDrift.
//
//   - drift         — columns with strong-evidence legacy-raw-hash witnesses
//     AND no genuine-adaptive witnesses. Schema record disagrees
//     with on-disk row payload; repair fixes it.
//   - heterogeneous — columns with BOTH strong-evidence legacy AND strong-
//     evidence adaptive witnesses. Schema and payload disagree
//     for some rows, agree for others. Repair refuses; the
//     correct remediation is row-by-row migration via
//     `recover-rows`.
//   - safeEmpty     — no strong evidence either way, but every row was
//     structurally consistent with the homogeneous-empty
//     pattern (NULL or the 20-byte all-zero-bytes shape).
//     Per forensic analysis verdict these are safe to flip; surfaced under
//     SeveritySafeEmpty.
//
// NOTE: a later fix/the fix — single-byte `[0x00]` inline empties USED to be admitted
// into safeEmpty, but they crash the legacy reader post-flip (hash.New on a
// 1-byte slice panics with `invalid hash length: 1`). forensic analysis only validated the
// 20-byte all-zero shape. Single-byte `[0x00]` rows now disqualify safeEmpty.
type scanBuckets struct {
	drift         []adaptiveCandidate
	heterogeneous []adaptiveCandidate
	safeEmpty     []adaptiveCandidate
}

// scanTableForDrift walks the prolly map and classifies each candidate
// column into one of four exclusive outcomes (drift / heterogeneous /
// safeEmpty / clean-and-silently-dropped).
//
// NOTE: a later fix/the fix — the previous implementation short-circuited per column
// on the first strong-evidence row. That mislabelled heterogeneous columns
// (some legacy rows, some adaptive rows) as clean if the first strong row
// happened to be adaptive, or as pure drift if the first strong row happened
// to be legacy. empirical data (real-world TEXT columns (empirical sampling):
// showed every a real-world database
// heterogeneous column was silently misclassified that way. The scanner now
// walks every row for every column, tracks sawLegacy + sawAdaptive
// independently, and only computes the final verdict at end-of-table:
//
//   - sawLegacy && !sawAdaptive       → drift
//   - sawLegacy &&  sawAdaptive       → heterogeneous
//   - !sawLegacy &&  sawAdaptive      → clean (silently dropped)
//   - !sawLegacy && !sawAdaptive && sawHomogeneousEmptyOnly → safeEmpty
//   - otherwise                       → silently dropped
//
// Cost: ALWAYS walks the full table per scanned database. Acceptable: a large database at
// a large number of rows takes ~tens of seconds on cold cache, and the alternative
// (false-negative on heterogeneous columns) is the kind of "silently miss
// half the corruption" failure that motivated the postgres-tier directive.
func scanTableForDrift(ctx context.Context, pm prolly.Map, cands []adaptiveCandidate, cs ChunkPresenceChecker) (scanBuckets, error) {
	// State per column — flags are independent and ALL set during iteration;
	// the verdict is computed only at end-of-table from the combined flags.
	type colState struct {
		cand                    adaptiveCandidate
		sawLegacy               bool
		sawAdaptive             bool
		sawHomogeneousEmptyOnly bool
	}
	states := make(map[int]*colState, len(cands))
	for _, c := range cands {
		states[c.tupleIndex] = &colState{
			cand:                    c,
			sawHomogeneousEmptyOnly: true,
		}
	}

	iter, err := pm.IterAll(ctx)
	if err != nil {
		return scanBuckets{}, err
	}
	for {
		_, value, iterErr := iter.Next(ctx)
		if iterErr == io.EOF {
			break
		}
		if iterErr != nil {
			return scanBuckets{}, iterErr
		}
		for _, st := range states {
			// Per-column micro-skip: once a column is known to be BOTH
			// legacy and adaptive (heterogeneous), additional witnesses
			// can't change the verdict. Skip the structural classification
			// to save the per-row cost on the long tail.
			//
			// We do NOT skip on "saw legacy only" or "saw adaptive only" —
			// a subsequent row could promote the column to heterogeneous,
			// and that's exactly the verdict this check exists to surface.
			if st.sawLegacy && st.sawAdaptive {
				continue
			}
			b := value.GetField(st.cand.tupleIndex)
			structural := ClassifyFieldBytes(b)
			switch structural {
			case FieldLegacyRawHash:
				st.sawLegacy = true
				// Legacy raw hash is, by construction, not the homogeneous-
				// empty shape; that pattern would have classified as
				// FieldAdaptiveInline+all-zero-bytes-20-len.
				st.sawHomogeneousEmptyOnly = false
			case FieldAdaptiveAddressed:
				st.sawAdaptive = true
				st.sawHomogeneousEmptyOnly = false
			case FieldNULL:
				// NULL is consistent with homogeneous-empty; no flag flip.
			case FieldAdaptiveInline:
				if len(b) == hashByteLen && cs != nil {
					// Try the chunkstore — if the hash IS present, it's
					// a legacy raw address (0x00-leader coincidence) and
					// the row is a definitive legacy witness.
					h := newHashFromField(b)
					present, hasErr := cs.Has(ctx, h)
					if hasErr != nil {
						return scanBuckets{}, fmt.Errorf("chunkstore presence check for column %s: %w", st.cand.colName, hasErr)
					}
					if present {
						st.sawLegacy = true
						st.sawHomogeneousEmptyOnly = false
						continue
					}
					// Chunkstore miss: the 20-byte 0x00-leading shape.
					// Stay homogeneous-empty only if the field is the
					// canonical all-zero shape (matches the verified
					// pattern). A non-zero content byte means it's an
					// ambiguous row we can't classify either way.
					if !isAllZeroBytes(b) {
						st.sawHomogeneousEmptyOnly = false
					}
					continue
				}
				// len != 20: a 0x00-leading field that is NOT exactly 20 bytes
				// cannot be a legacy raw hash (those are always exactly 20
				// bytes) — it is unambiguously adaptive-inline. Non-empty
				// content (`[0x00]<content>`, the documented "inline orphan"
				// shape) is a DEFINITIVE adaptive witness, mirroring repair.go's
				// scanColumnFullPayload. Without this, an inline-heterogeneous
				// column (legacy + inline) sets only sawLegacy and is
				// mis-bucketed as "drift / safe-to-repair" — and check then
				// emits a `repair` hint that repair itself correctly REFUSES
				// (check must not contradict repair). [review finding]
				if len(b) != hashByteLen && len(b) > 1 {
					st.sawAdaptive = true
				}
				// a later fix/the fix: short non-NULL inline payloads also disqualify
				// safeEmpty. The 1-byte `[0x00]` (adaptive empty-inline) USED to
				// be admitted as safe-empty in the earlier version, but the post-flip legacy
				// reader does hash.New([1 byte]) → panic `invalid hash length:
				// 1`. forensic analysis only validated the 20-byte all-zero shape.
				st.sawHomogeneousEmptyOnly = false
			case FieldUnknown:
				// Unknown shape — disqualifies safeEmpty. No flag set on
				// either legacy or adaptive, so a column with only
				// FieldUnknown rows lands in "silently dropped".
				st.sawHomogeneousEmptyOnly = false
			}
		}
	}

	var buckets scanBuckets
	for _, st := range states {
		switch {
		case st.sawLegacy && st.sawAdaptive:
			buckets.heterogeneous = append(buckets.heterogeneous, st.cand)
		case st.sawLegacy && !st.sawAdaptive:
			buckets.drift = append(buckets.drift, st.cand)
		case !st.sawLegacy && st.sawAdaptive:
			// Clean: silently drop.
		case !st.sawLegacy && !st.sawAdaptive && st.sawHomogeneousEmptyOnly:
			buckets.safeEmpty = append(buckets.safeEmpty, st.cand)
		}
	}
	sort.Slice(buckets.drift, func(i, j int) bool { return buckets.drift[i].tupleIndex < buckets.drift[j].tupleIndex })
	sort.Slice(buckets.heterogeneous, func(i, j int) bool { return buckets.heterogeneous[i].tupleIndex < buckets.heterogeneous[j].tupleIndex })
	sort.Slice(buckets.safeEmpty, func(i, j int) bool { return buckets.safeEmpty[i].tupleIndex < buckets.safeEmpty[j].tupleIndex })
	return buckets, nil
}

// isAllZeroBytes reports whether every byte in b is 0x00. Used by the
// scanner to disambiguate the legacy-empty-content homogeneous shape from
// other 0x00-leading inline shapes.
func isAllZeroBytes(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

// emitDriftTable renders a TTY-friendly table to w. Returns the appropriate
// process exit code (1 on any drift or safe-empty entries, 0 on clean).
func emitDriftTable(w io.Writer, drifts []DriftRow) int {
	if len(drifts) == 0 {
		_, _ = fmt.Fprintln(w, "no schema-encoding drift detected")
		return 0
	}

	headers := []string{"database", "table", "column", "declared", "observed", "suggested", "severity"}
	rows := make([][]string, 0, len(drifts))
	driftCount, heteroCount, safeEmptyCount := 0, 0, 0
	for _, d := range drifts {
		rows = append(rows, []string{
			d.Database, d.Table, d.Column,
			d.DeclaredEncoding, d.ObservedFormat, d.SuggestedEncoding,
			d.Severity,
		})
		switch d.Severity {
		case SeverityDrift:
			driftCount++
		case SeverityHeterogeneous:
			heteroCount++
		case SeveritySafeEmpty:
			safeEmptyCount++
		}
	}

	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, r := range rows {
		for i, c := range r {
			if len(c) > widths[i] {
				widths[i] = len(c)
			}
		}
	}

	writeRow := func(cols []string) {
		parts := make([]string, len(cols))
		for i, c := range cols {
			parts[i] = c + strings.Repeat(" ", widths[i]-len(c))
		}
		_, _ = fmt.Fprintln(w, strings.Join(parts, "  "))
	}

	writeRow(headers)
	sep := make([]string, len(headers))
	for i := range sep {
		sep[i] = strings.Repeat("-", widths[i])
	}
	writeRow(sep)
	for _, r := range rows {
		writeRow(r)
	}
	_, _ = fmt.Fprintf(w,
		"\n%d total — %d %s (repair by default), %d %s (recover-rows), %d %s (repair --include-empty)\n",
		len(drifts), driftCount, SeverityDrift, heteroCount, SeverityHeterogeneous, safeEmptyCount, SeveritySafeEmpty)
	return 1
}

// emitDriftJSON writes the drift list as a JSON array suitable for piping
// directly into `repair`. Returns the appropriate process exit code.
func emitDriftJSON(w io.Writer, drifts []DriftRow) int {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if drifts == nil {
		// Encode an empty array (not null) so consumers can pipe unconditionally.
		drifts = []DriftRow{}
	}
	if err := enc.Encode(drifts); err != nil {
		// Fall back to plain output and signal error.
		_, _ = fmt.Fprintf(w, "{\"error\":\"%s\"}\n", err.Error())
		return 1
	}
	if len(drifts) == 0 {
		return 0
	}
	return 1
}

// encodingName returns a stable string label for a val.Encoding suitable for
// user-facing output. Falls back to %d for any encoding not in the
// adaptive/legacy pairing.
// heterogeneousHint returns the remediation hint for a heterogeneous column.
// On a dolt_ignore'd table only migrate-adaptive is safe: recover-rows commits
// and would dangling-fault on the table's never-committed content chunks.
func heterogeneousHint(ignored bool, table, col string) string {
	if ignored {
		return fmt.Sprintf("dolt admin schema-encoding-drift migrate-adaptive --table %s --column %s (dolt_ignore'd table: recover-rows would dangling-fault; force-inline heal only)", table, col)
	}
	return fmt.Sprintf("dolt admin schema-encoding-drift recover-rows --table %s --column %s (row-by-row to legacy), or migrate-adaptive --table %s --column %s (row-by-row to v2-native adaptive)", table, col, table, col)
}

func encodingName(enc val.Encoding) string {
	switch enc {
	case val.StringAddrEnc:
		return "StringAddrEnc"
	case val.BytesAddrEnc:
		return "BytesAddrEnc"
	case val.JSONAddrEnc:
		return "JSONAddrEnc"
	case val.GeomAddrEnc:
		return "GeomAddrEnc"
	case val.ExtendedAddrEnc:
		return "ExtendedAddrEnc"
	case val.StringAdaptiveEnc:
		return "StringAdaptiveEnc"
	case val.BytesAdaptiveEnc:
		return "BytesAdaptiveEnc"
	case val.JsonAdaptiveEnc:
		return "JsonAdaptiveEnc"
	case val.GeomAdaptiveEnc:
		return "GeomAdaptiveEnc"
	case val.ExtendedAdaptiveEnc:
		return "ExtendedAdaptiveEnc"
	}
	return fmt.Sprintf("Encoding(%d)", enc)
}

// Compile-time check that doltdb.Table satisfies the row-access API we use.
// We don't actually need any methods from this var — it's a guard so a
// future doltdb.Table refactor that breaks GetRowData / GetSchema is caught
// at build time here, not at first invocation.
var _ = (*doltdb.Table)(nil)
