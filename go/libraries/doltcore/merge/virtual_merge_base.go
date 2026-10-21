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

package merge

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// The commit metadata of a virtual merge base. A virtual merge base is a dangling commit that no ref points to, so
// users do not see it.
const (
	virtualMergeBaseName        = "Dolt"
	virtualMergeBaseEmail       = "virtual-merge-base@dolthub.com"
	virtualMergeBaseDescription = "virtual merge base"
)

func init() {
	doltdb.RebuildVirtualMergeBase = rebuildVirtualMergeBase
}

// virtualMergeBase is the ancestor Rootish of a merge against a virtual merge base. Conflicts record its merge bases so
// that it can be rebuilt after garbage collection.
type virtualMergeBase struct {
	*doltdb.Commit
	mergeBases []hash.Hash
	conflicted conflictedCells
}

// conflictedCells lists, by table and primary key, the cells of a virtual merge base whose merge bases conflicted.
// git keeps conflict markers in those places of its virtual merge base, which neither side of the merge matches. A
// cell cannot hold conflict markers, so the merge treats these cells as changed on both sides instead: they conflict
// unless both sides hold the same value.
type conflictedCells map[doltdb.TableName]map[string]conflictedRow

// conflictedRow lists the conflicted cells of one row by column tag. With |wholeRow|, the row itself conflicted, as
// when two merge bases added it with different values.
type conflictedRow struct {
	wholeRow bool
	tags     []uint64
}

func (c conflictedCells) add(other conflictedCells) conflictedCells {
	for table, rows := range other {
		if c == nil {
			c = make(conflictedCells)
		}
		if c[table] == nil {
			c[table] = make(map[string]conflictedRow)
		}
		for key, row := range rows {
			prev := c[table][key]
			c[table][key] = conflictedRow{wholeRow: prev.wholeRow || row.wholeRow, tags: append(prev.tags, row.tags...)}
		}
	}
	return c
}

// mergeBaseRootish returns the ancestor Rootish for merging against |base|, built from |bases|.
func mergeBaseRootish(base *doltdb.Commit, bases []*doltdb.Commit, conflicted conflictedCells) (doltdb.Rootish, error) {
	if len(bases) < 2 {
		return base, nil
	}
	addrs := make([]hash.Hash, len(bases))
	for i, b := range bases {
		var err error
		if addrs[i], err = b.HashOf(); err != nil {
			return nil, err
		}
	}
	return virtualMergeBase{Commit: base, mergeBases: addrs, conflicted: conflicted}, nil
}

// ResolveMergeBase returns the commit to use as the base when merging |left| and |right|. When they have several best
// common ancestors, it returns a virtual merge base built by merging those ancestors together, as git's merge-ort
// does. The virtual merge base is a dangling commit whose parents are the merged ancestors. It depends only on the
// merge bases, so every caller gets the same commit.
func ResolveMergeBase(ctx *sql.Context, left, right *doltdb.Commit) (*doltdb.Commit, error) {
	base, _, _, err := resolveMergeBase(ctx, left, right)
	return base, err
}

// ResolveMergeBaseRootish returns the ancestor to merge |left| and |right| against: their merge base, or a virtual
// merge base that also carries the cells where their merge bases conflicted.
func ResolveMergeBaseRootish(ctx *sql.Context, left, right *doltdb.Commit) (doltdb.Rootish, error) {
	base, bases, conflicted, err := resolveMergeBase(ctx, left, right)
	if err != nil {
		return nil, err
	}
	return mergeBaseRootish(base, bases, conflicted)
}

// resolveMergeBase returns the merge base of |left| and |right|, the best common ancestors it was built from, and the
// cells where those ancestors conflicted.
func resolveMergeBase(ctx *sql.Context, left, right *doltdb.Commit) (*doltdb.Commit, []*doltdb.Commit, conflictedCells, error) {
	optCmts, err := doltdb.GetCommitAncestors(ctx, left, right)
	if err != nil {
		return nil, nil, nil, err
	}
	bases := make([]*doltdb.Commit, len(optCmts))
	for i, optCmt := range optCmts {
		base, ok := optCmt.ToCommit()
		if !ok {
			return nil, nil, nil, doltdb.ErrGhostCommitRuntimeFailure
		}
		bases[i] = base
	}
	base, conflicted, err := mergeMergeBases(ctx, bases)
	return base, bases, conflicted, err
}

func mergeMergeBases(ctx *sql.Context, bases []*doltdb.Commit) (*doltdb.Commit, conflictedCells, error) {
	if len(bases) == 1 {
		return bases[0], nil, nil
	}
	// Oldest first: git's merge_ort_internal reverses the newest-first list of merge bases before folding it.
	if err := sortByCommitterDate(ctx, bases, false); err != nil {
		return nil, nil, err
	}
	virtualBase := bases[0]
	var conflicted conflictedCells
	for _, base := range bases[1:] {
		var foldConflicts conflictedCells
		var err error
		virtualBase, foldConflicts, err = mergeIntoVirtualBase(ctx, virtualBase, base)
		if err != nil {
			return nil, nil, err
		}
		conflicted = conflicted.add(foldConflicts)
	}
	return virtualBase, conflicted, nil
}

func rebuildVirtualMergeBase(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, mergeBases []hash.Hash) (doltdb.RootValue, error) {
	bases := make([]*doltdb.Commit, len(mergeBases))
	for i, addr := range mergeBases {
		dc, err := datas.LoadCommitAddr(ctx, vrw, addr)
		if err != nil {
			return nil, err
		}
		if dc.IsGhost() {
			return nil, doltdb.ErrGhostCommitEncountered
		}
		if bases[i], err = doltdb.NewCommit(ctx, vrw, ns, dc); err != nil {
			return nil, err
		}
	}

	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		sqlCtx = sql.NewContext(ctx)
	}
	virtualBase, _, err := mergeMergeBases(sqlCtx, bases)
	if err != nil {
		return nil, err
	}
	return virtualBase.GetRootValue(ctx)
}

// mergeIntoVirtualBase merges |x| and |y| into a dangling commit, and returns the cells where they conflicted. Cells
// that merge cleanly keep their merged value, and a conflicted cell keeps the value of the merge base of |x| and |y|.
// git's merge-ort keeps conflict markers in the conflicted lines of text files instead; a cell cannot hold them, so
// the returned cells are treated as changed on both sides when merging against the virtual merge base. A conflicted row
// of a keyless table keeps the merge base's copies, and a table whose schema conflicts is taken whole from it. Like
// git's, the virtual merge base is never checked out, so it keeps the merged rows even where they violate a
// constraint, and drops the violations.
func mergeIntoVirtualBase(ctx *sql.Context, x, y *doltdb.Commit) (*doltdb.Commit, conflictedCells, error) {
	base, bases, baseConflicts, err := resolveMergeBase(ctx, x, y)
	if err != nil {
		return nil, nil, err
	}
	ancestor, err := mergeBaseRootish(base, bases, baseConflicts)
	if err != nil {
		return nil, nil, err
	}
	xRoot, err := x.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}
	yRoot, err := y.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}
	baseRoot, err := base.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}

	result, err := MergeRoots(ctx, doltdb.SimpleTableResolver{}, xRoot, yRoot, baseRoot, y, ancestor, editor.Options{}, MergeOpts{KeepSchemaConflicts: true, AlwaysMergeJSON: true, SkipFullTextRebuild: true})
	if err != nil {
		return nil, nil, err
	}
	mergedRoot, rewritten, conflicted, err := resolveVirtualBaseConflicts(ctx, result, xRoot, yRoot, baseRoot, baseConflicts)
	if err != nil {
		return nil, nil, err
	}
	for _, name := range result.FullTextRebuilds {
		rewritten[name] = true
	}
	if mergedRoot, err = rebuildVirtualBaseFullTextIndexes(ctx, mergedRoot, rewritten); err != nil {
		return nil, nil, err
	}
	meta, err := virtualMergeBaseMeta(ctx, x, y)
	if err != nil {
		return nil, nil, err
	}
	virtualBase, err := doltdb.NewDanglingCommit(ctx, mergedRoot, []*doltdb.Commit{x, y}, meta)
	return virtualBase, conflicted, err
}

// resolveVirtualBaseConflicts gives every conflicted row of |result| its cell-by-cell merge, and removes the merge's
// artifacts, which a virtual merge base does not keep. A table whose schema conflicts between |xRoot| and |yRoot|
// keeps its schema in |baseRoot|, and its rows merge cell by cell under that schema.
// It also returns the tables it rewrote.
func resolveVirtualBaseConflicts(ctx *sql.Context, result *Result, xRoot, yRoot, baseRoot doltdb.RootValue, baseConflicts conflictedCells) (doltdb.RootValue, map[doltdb.TableName]bool, conflictedCells, error) {
	schemaConflicts := make(map[doltdb.TableName]bool)
	for _, name := range SchemaConflictTableNames(result.SchemaConflicts) {
		schemaConflicts[name] = true
	}
	tables := make(map[doltdb.TableName]bool)
	for name, stats := range result.Stats {
		if stats.HasArtifacts() {
			tables[name] = true
		}
	}
	for name := range schemaConflicts {
		tables[name] = true
	}

	root := result.Root
	conflicted := make(conflictedCells)
	for name := range tables {
		// Full-Text pseudo-index tables are rebuilt from their parent tables.
		if doltdb.IsFullTextTable(name.Name) {
			continue
		}
		var tbl *doltdb.Table
		var rows map[string]conflictedRow
		var err error
		if schemaConflicts[name] {
			tbl, rows, err = mergeRowsUnderBaseSchema(ctx, xRoot, yRoot, baseRoot, name, baseConflicts[name])
		} else {
			tbl, rows, err = mergeConflictedRowsByCell(ctx, root, xRoot, yRoot, baseRoot, name, baseConflicts[name])
		}
		if err != nil {
			return nil, nil, nil, err
		}
		if tbl == nil {
			if root, err = takeTableFromBase(ctx, root, baseRoot, name); err != nil {
				return nil, nil, nil, err
			}
			continue
		}
		if root, err = root.PutTable(ctx, name, tbl); err != nil {
			return nil, nil, nil, err
		}
		if len(rows) > 0 {
			conflicted[name] = rows
		}
	}
	return root, tables, conflicted, nil
}

// mergeConflictedRowsByCell rewrites each conflicted row of table |name| in |root| as a cell-by-cell merge of the row
// in |xRoot| and |yRoot| against |baseRoot|, keeping the base value of each cell that conflicts. Cells that
// |baseConflicts| marks in the merge base count as changed on both sides. Cells are compared as values of the merged
// schema, so the three sides may have different schemas. A conflicted row of a keyless table keeps its copies in
// |baseRoot| whole. It returns the table without artifacts and the conflicted cells, or a nil table when the table is
// missing from |xRoot| or |yRoot|.
func mergeConflictedRowsByCell(ctx *sql.Context, root, xRoot, yRoot, baseRoot doltdb.RootValue, name doltdb.TableName, baseConflicts map[string]conflictedRow) (*doltdb.Table, map[string]conflictedRow, error) {
	merged, _, err := root.GetTable(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	sch, err := merged.GetSchema(ctx)
	if err != nil {
		return nil, nil, err
	}
	sides, ok, err := loadSides(ctx, merged, sch, name, xRoot, yRoot, baseRoot, true)
	if err != nil || !ok {
		return nil, nil, err
	}
	// A keyless row is identified by its values, which a different schema changes, so such a table merges under the
	// base's schema.
	if schema.IsKeyless(sch) && !sides.sameSchemas(sch) {
		return mergeRowsUnderBaseSchema(ctx, xRoot, yRoot, baseRoot, name, baseConflicts)
	}

	idx, err := merged.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}
	mergedRows, err := durable.ProllyMapFromIndex(idx)
	if err != nil {
		return nil, nil, err
	}
	artIdx, err := merged.GetArtifacts(ctx)
	if err != nil {
		return nil, nil, err
	}
	conflicts, err := durable.ProllyMapFromArtifactIndex(artIdx).IterAllConflicts(ctx)
	if err != nil {
		return nil, nil, err
	}
	rm, err := newVirtualRowMerger(ctx, name, sch, sides, merged.NodeStore(), false)
	if err != nil {
		return nil, nil, err
	}
	edits := newTableEdits(mergedRows)
	rows := make(map[string]conflictedRow)
	for {
		art, err := conflicts.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, nil, err
		}
		x, y, b, err := sides.rows(ctx, art.Key)
		if err != nil {
			return nil, nil, err
		}
		// The merge bases of a conflicted keyless row disagree on how many copies of it there are. Like a conflicted
		// line in git's text merge, it keeps its base's copies and is marked.
		if rm.keyless {
			if err = edits.set(ctx, art.Key, b); err != nil {
				return nil, nil, err
			}
			rows[string(art.Key)] = conflictedRow{wholeRow: true}
			continue
		}
		value, row, err := rm.merge(ctx, art.Key, x, y, b, baseConflicts[string(art.Key)])
		if err != nil {
			return nil, nil, err
		}
		if err = edits.set(ctx, art.Key, value); err != nil {
			return nil, nil, err
		}
		if row.wholeRow || len(row.tags) > 0 {
			rows[string(art.Key)] = row
		}
	}
	tbl, err := finishVirtualTable(ctx, merged, sch, name, edits)
	return tbl, rows, err
}

// mergeRowsUnderBaseSchema returns table |name| of |baseRoot|, with the rows of |xRoot| and |yRoot| merged into it cell
// by cell under its schema, and the conflicted cells. Columns that |baseRoot| lacks are left out. It is for a table
// whose schema conflicts between |xRoot| and |yRoot|: a merge against the virtual merge base then compares the tips'
// schemas with the base's, as a merge with that base alone would. It returns a nil table when |baseRoot| lacks the
// table.
func mergeRowsUnderBaseSchema(ctx *sql.Context, xRoot, yRoot, baseRoot doltdb.RootValue, name doltdb.TableName, baseConflicts map[string]conflictedRow) (*doltdb.Table, map[string]conflictedRow, error) {
	baseTbl, ok, err := baseRoot.GetTable(ctx, name)
	if err != nil || !ok {
		return nil, nil, err
	}
	sch, err := baseTbl.GetSchema(ctx)
	if err != nil {
		return nil, nil, err
	}
	sides, _, err := loadSides(ctx, baseTbl, sch, name, xRoot, yRoot, baseRoot, false)
	if err != nil {
		return nil, nil, err
	}
	ns := baseTbl.NodeStore()
	if schema.IsKeyless(sch) {
		edits, conflicted, err := mergeKeylessRowsUnderBaseSchema(ctx, sch, sides, ns)
		if err != nil {
			return nil, nil, err
		}
		rows := make(map[string]conflictedRow)
		for key := range conflicted {
			rows[key] = conflictedRow{wholeRow: true}
		}
		tbl, err := finishVirtualTable(ctx, baseTbl, sch, name, edits)
		return tbl, rows, err
	}

	rm, err := newVirtualRowMerger(ctx, name, sch, sides, ns, true)
	if err != nil {
		return nil, nil, err
	}
	keys, err := changedKeys(ctx, sides.base, sides.x, sides.y)
	if err != nil {
		return nil, nil, err
	}
	edits := newTableEdits(sides.base)
	rows := make(map[string]conflictedRow)
	for _, key := range keys {
		x, y, b, err := sides.rows(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		value, row, err := rm.merge(ctx, key, x, y, b, baseConflicts[string(key)])
		if err != nil {
			return nil, nil, err
		}
		if err = edits.set(ctx, key, value); err != nil {
			return nil, nil, err
		}
		if row.wholeRow || len(row.tags) > 0 {
			rows[string(key)] = row
		}
	}
	tbl, err := finishVirtualTable(ctx, baseTbl, sch, name, edits)
	return tbl, rows, err
}

// virtualMergeSides holds the rows and schemas of a table in the two merge bases of a virtual merge base, x and y, and
// in their merge base.
type virtualMergeSides struct {
	x, y, base          prolly.Map
	xSch, ySch, baseSch schema.Schema
}

// loadSides loads table |name| from each root. A root without the table has no rows of it, with schema |sch|. With
// |requireMergeBases|, it reports false when |xRoot| or |yRoot| lacks the table.
func loadSides(ctx *sql.Context, tbl *doltdb.Table, sch schema.Schema, name doltdb.TableName, xRoot, yRoot, baseRoot doltdb.RootValue, requireMergeBases bool) (virtualMergeSides, bool, error) {
	var sides virtualMergeSides
	rows := []*prolly.Map{&sides.x, &sides.y, &sides.base}
	schemas := []*schema.Schema{&sides.xSch, &sides.ySch, &sides.baseSch}
	for i, sideRoot := range []doltdb.RootValue{xRoot, yRoot, baseRoot} {
		sideTbl, ok, err := sideRoot.GetTable(ctx, name)
		if err != nil {
			return sides, false, err
		}
		var idx durable.Index
		if ok {
			if *schemas[i], err = sideTbl.GetSchema(ctx); err != nil {
				return sides, false, err
			}
			idx, err = sideTbl.GetRowData(ctx)
		} else if requireMergeBases && sideRoot != baseRoot {
			return sides, false, nil
		} else {
			*schemas[i] = sch
			idx, err = durable.NewEmptyPrimaryIndex(ctx, tbl.ValueReadWriter(), tbl.NodeStore(), sch)
		}
		if err != nil {
			return sides, false, err
		}
		if *rows[i], err = durable.ProllyMapFromIndex(idx); err != nil {
			return sides, false, err
		}
	}
	return sides, true, nil
}

func (s virtualMergeSides) sameSchemas(sch schema.Schema) bool {
	return schema.SchemasAreEqual(sch, s.xSch) && schema.SchemasAreEqual(sch, s.ySch) && schema.SchemasAreEqual(sch, s.baseSch)
}

func (s virtualMergeSides) rows(ctx context.Context, key val.Tuple) (x, y, b val.Tuple, err error) {
	if x, err = getRow(ctx, s.x, key); err != nil {
		return
	}
	if y, err = getRow(ctx, s.y, key); err != nil {
		return
	}
	b, err = getRow(ctx, s.base, key)
	return
}

// changedKeys returns the keys of the rows that differ between |base| and |x| or |y|.
func changedKeys(ctx context.Context, base, x, y prolly.Map) ([]val.Tuple, error) {
	seen := make(map[string]bool)
	var keys []val.Tuple
	for _, side := range []prolly.Map{x, y} {
		err := prolly.DiffMaps(ctx, base, side, false, func(_ context.Context, diff tree.Diff) error {
			key := val.Tuple(diff.Key)
			if !seen[string(key)] {
				seen[string(key)] = true
				keys = append(keys, slices.Clone(key))
			}
			return nil
		})
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
	}
	return keys, nil
}

// tableEdits are the rows a virtual merge base sets in a table, with the rows they replace, so that its secondary
// indexes can be updated for only those rows.
type tableEdits struct {
	start prolly.Map
	mut   *prolly.MutableMap
	rows  []rowEdit
}

type rowEdit struct {
	key, old, new val.Tuple
}

func newTableEdits(rows prolly.Map) *tableEdits {
	return &tableEdits{start: rows, mut: rows.Mutate()}
}

// set sets row |key| to |value|, or deletes it when |value| is nil.
func (e *tableEdits) set(ctx context.Context, key, value val.Tuple) error {
	old, err := getRow(ctx, e.start, key)
	if err != nil || bytes.Equal(old, value) {
		return err
	}
	if value == nil {
		err = e.mut.Delete(ctx, key)
	} else {
		err = e.mut.Put(ctx, key, value)
	}
	if err != nil {
		return err
	}
	e.rows = append(e.rows, rowEdit{key: slices.Clone(key), old: old, new: value})
	return nil
}

// virtualRowMerger merges the rows of a table in two merge bases cell by cell into rows of schema |sch|.
type virtualRowMerger struct {
	vm        *valueMerger
	tableName string
	sch       schema.Schema
	stored    []schema.Column
	generated []sql.Expression
	builder   *val.TupleBuilder
	cells     []interface{}
	ns        tree.NodeStore
	keyless   bool
	// Under the base's schema of a table whose schema conflicts, a cell that does not convert to it conflicts.
	conversionConflicts bool
}

// newVirtualBaseValueMerger returns a value merger of the sides of a virtual merge base, which merges JSON documents
// whatever the session's setting, so that the virtual merge base does not depend on it.
func newVirtualBaseValueMerger(ctx *sql.Context, sch schema.Schema, sides virtualMergeSides, ns tree.NodeStore) *valueMerger {
	vm := NewValueMerger(ctx, sch, sides.xSch, sides.ySch, sides.baseSch, sides.x.Pool(), ns)
	vm.alwaysMergeJSON = true
	return vm
}

func newVirtualRowMerger(ctx *sql.Context, name doltdb.TableName, sch schema.Schema, sides virtualMergeSides, ns tree.NodeStore, conversionConflicts bool) (*virtualRowMerger, error) {
	rm := &virtualRowMerger{
		vm:                  newVirtualBaseValueMerger(ctx, sch, sides, ns),
		tableName:           name.Name,
		sch:                 sch,
		builder:             val.NewTupleBuilder(sch.GetValueDescriptor(ns), ns),
		ns:                  ns,
		keyless:             schema.IsKeyless(sch),
		conversionConflicts: conversionConflicts,
	}
	for _, col := range sch.GetNonPKCols().GetColumns() {
		if !col.Virtual {
			rm.stored = append(rm.stored, col)
		}
	}
	rm.cells = make([]interface{}, len(rm.stored))
	if hasStoredGeneratedColumns(sch) {
		var err error
		if rm.generated, err = resolveDefaults(ctx, name.Name, sch, sch); err != nil {
			return nil, err
		}
	}
	return rm, nil
}

// merge returns the merge of row |key|, |x| and |y| against |b|, or nil for no row, and its conflicted cells. A cell
// marked in |baseRow| counts as changed on both sides.
func (rm *virtualRowMerger) merge(ctx *sql.Context, key, x, y, b val.Tuple, baseRow conflictedRow) (val.Tuple, conflictedRow, error) {
	vm := rm.vm
	switch {
	case b == nil && x != nil && y != nil:
		// A row added twice is marked unless both added the same row, as git's text merge of two added files leaves
		// conflict markers.
		equal, err := rm.sidesEqual(ctx, x, vm.leftVD, vm.leftMapping, y, vm.rightVD, vm.rightMapping)
		if err != nil || !equal {
			return nil, conflictedRow{wholeRow: true}, rm.conversionError(err)
		}
		return rm.rowFrom(ctx, key, x, vm.leftVD, vm.leftMapping)
	case b == nil && x != nil:
		return rm.rowFrom(ctx, key, x, vm.leftVD, vm.leftMapping)
	case b == nil && y != nil:
		return rm.rowFrom(ctx, key, y, vm.rightVD, vm.rightMapping)
	case x == nil && y == nil:
		return nil, conflictedRow{}, nil
	case x == nil || y == nil:
		// A row that one merge base deleted and the other changed takes the base's row unmarked, as git's merge-ort
		// takes the base's version of a file for a modify/delete conflict.
		kept, vd, mapping := y, vm.rightVD, vm.rightMapping
		if y == nil {
			kept, vd, mapping = x, vm.leftVD, vm.leftMapping
		}
		unchanged, err := rm.sidesEqual(ctx, kept, vd, mapping, b, vm.baseVD, vm.baseMapping)
		if err = rm.conversionError(err); err != nil || unchanged {
			return nil, conflictedRow{}, err
		}
		return rm.rowFrom(ctx, key, b, vm.baseVD, vm.baseMapping)
	}

	var row conflictedRow
	for i, col := range rm.stored {
		bv, err := vm.mergedCell(ctx, vm.baseVD, vm.baseMapping, b, i)
		if err != nil {
			return nil, row, err
		}
		rm.cells[i] = bv
		// A generated value is computed from its sources below, so only they conflict.
		if col.Generated != "" {
			continue
		}
		cell, err := mergeCell(ctx, vm, i, x, y, b, baseRow.wholeRow || slices.Contains(baseRow.tags, col.Tag))
		if errors.Is(err, errCellConflict) || (err != nil && rm.conversionConflicts) {
			row.tags = append(row.tags, col.Tag)
		} else if err != nil {
			return nil, row, err
		} else {
			rm.cells[i] = cell
		}
	}
	value, err := rm.build(ctx, key)
	return value, row, err
}

// conversionError returns nil for an error converting a cell to a row of a table whose schema conflicts.
func (rm *virtualRowMerger) conversionError(err error) error {
	if rm.conversionConflicts {
		return nil
	}
	return err
}

// rowFrom returns |row|, of the schema that |vd| and |mapping| describe, as a row of the merger's schema.
func (rm *virtualRowMerger) rowFrom(ctx *sql.Context, key, row val.Tuple, vd *val.TupleDesc, mapping val.OrdinalMapping) (val.Tuple, conflictedRow, error) {
	for i := range rm.stored {
		cell, err := rm.vm.mergedCell(ctx, vd, mapping, row, i)
		if err != nil {
			if rm.conversionConflicts {
				return nil, conflictedRow{wholeRow: true}, nil
			}
			return nil, conflictedRow{}, err
		}
		rm.cells[i] = cell
	}
	value, err := rm.build(ctx, key)
	return value, conflictedRow{}, err
}

// sidesEqual reports whether |a| and |b| hold the same values in the stored columns of the merger's schema, apart
// from generated ones.
func (rm *virtualRowMerger) sidesEqual(ctx context.Context, a val.Tuple, aVD *val.TupleDesc, aMapping val.OrdinalMapping, b val.Tuple, bVD *val.TupleDesc, bMapping val.OrdinalMapping) (bool, error) {
	for i, col := range rm.stored {
		if col.Generated != "" {
			continue
		}
		av, err := rm.vm.mergedCell(ctx, aVD, aMapping, a, i)
		if err != nil {
			return false, err
		}
		bv, err := rm.vm.mergedCell(ctx, bVD, bMapping, b, i)
		if err != nil {
			return false, err
		}
		if equal, err := rm.vm.mergedCellsEqual(ctx, i, av, bv); err != nil || !equal {
			return false, err
		}
	}
	return true, nil
}

func (rm *virtualRowMerger) build(ctx *sql.Context, key val.Tuple) (val.Tuple, error) {
	value, err := buildRowValue(ctx, rm.builder, rm.cells, rm.ns)
	if err != nil || rm.generated == nil {
		return value, err
	}
	return generateStoredColumns(ctx, rm.builder, rm.cells, rm.stored, rm.generated, rm.sch, rm.tableName, key, value, rm.ns)
}

// mergeKeylessRowsUnderBaseSchema merges how many copies of each row of keyless table |sides| there are under the
// base's schema |sch|, matching rows by their values under it. A row whose merge bases disagree on its copies keeps the
// base's copies and is conflicted.
func mergeKeylessRowsUnderBaseSchema(ctx *sql.Context, sch schema.Schema, sides virtualMergeSides, ns tree.NodeStore) (*tableEdits, map[string]bool, error) {
	type copies struct {
		value   val.Tuple
		x, y, b uint64
	}
	counts := make(map[string]*copies)
	var order []string
	vd := sch.GetValueDescriptor(ns)
	builder := val.NewTupleBuilder(vd, ns)
	pool := sides.base.Pool()
	for side, rows := range []prolly.Map{sides.x, sides.y, sides.base} {
		srcSch := []schema.Schema{sides.xSch, sides.ySch, sides.baseSch}[side]
		srcVD := srcSch.GetValueDescriptor(ns)
		iter, err := rows.IterAll(ctx)
		if err != nil {
			return nil, nil, err
		}
		for {
			_, value, err := iter.Next(ctx)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return nil, nil, err
			}
			n := val.ReadKeylessCardinality(value)
			if value, err = keylessValueUnder(ctx, builder, sch, srcSch, srcVD, value, ns); err != nil {
				return nil, nil, err
			}
			key := string(val.HashTupleFromValue(pool, value))
			c, ok := counts[key]
			if !ok {
				c = &copies{value: value}
				counts[key] = c
				order = append(order, key)
			}
			*[]*uint64{&c.x, &c.y, &c.b}[side] += n
		}
	}

	edits := newTableEdits(sides.base)
	conflicted := make(map[string]bool)
	for _, key := range order {
		c := counts[key]
		n := c.b
		switch {
		case c.x == c.y || c.y == c.b:
			n = c.x
		case c.x == c.b:
			n = c.y
		default:
			conflicted[key] = true
		}
		var value val.Tuple
		if n > 0 {
			value, _ = val.ModifyKeylessCardinality(pool, c.value, int64(n)-int64(val.ReadKeylessCardinality(c.value)))
		}
		if err := edits.set(ctx, val.Tuple(key), value); err != nil {
			return nil, nil, err
		}
	}
	return edits, conflicted, nil
}

// keylessValueUnder returns keyless row |value| of schema |srcSch| as a row of schema |sch|, with the same copies.
// Columns |srcSch| lacks are NULL, and a value that does not convert to its column's type is NULL.
func keylessValueUnder(ctx *sql.Context, builder *val.TupleBuilder, sch, srcSch schema.Schema, srcVD *val.TupleDesc, value val.Tuple, ns tree.NodeStore) (val.Tuple, error) {
	builder.Recycle()
	builder.PutUint64(0, val.ReadKeylessCardinality(value))
	i := 0
	for _, col := range sch.GetNonPKCols().GetColumns() {
		if col.Virtual {
			continue
		}
		i++
		j := findNonPKColumnMappingByTagOrName(srcSch, col)
		if j == -1 {
			continue
		}
		v, err := tree.GetField(ctx, srcVD, j+1, value, ns)
		if err != nil {
			return nil, err
		}
		if v, _, err = col.TypeInfo.ToSqlType().Convert(ctx, v); err != nil {
			continue
		}
		if err = tree.PutField(ctx, ns, builder, i, v); err != nil {
			return nil, err
		}
	}
	return builder.BuildPermissive(ctx, ns.Pool())
}

// finishVirtualTable returns |tbl| with the rows of |edits|, its secondary indexes updated for them, and no artifacts.
// The rows that |edits| starts from are |tbl|'s, which its secondary indexes match.
func finishVirtualTable(ctx *sql.Context, tbl *doltdb.Table, sch schema.Schema, name doltdb.TableName, edits *tableEdits) (*doltdb.Table, error) {
	ns := tbl.NodeStore()
	newRows, err := edits.mut.Map(ctx)
	if err != nil {
		return nil, err
	}
	if tbl, err = tbl.UpdateRows(ctx, durable.IndexFromProllyMap(newRows)); err != nil {
		return nil, err
	}
	indexes, err := tbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	for _, index := range sch.Indexes().AllIndexes() {
		updated, err := updateSecondaryIndex(ctx, tbl, indexes, sch, name, index, newRows, edits.rows)
		if err != nil {
			return nil, err
		}
		if indexes, err = indexes.PutIndex(ctx, index.Name(), updated); err != nil {
			return nil, err
		}
	}
	if tbl, err = tbl.SetIndexSet(ctx, indexes); err != nil {
		return nil, err
	}
	emptyArtifacts, err := durable.NewEmptyArtifactIndex(ctx, tbl.ValueReadWriter(), ns, sch)
	if err != nil {
		return nil, err
	}
	return tbl.SetArtifacts(ctx, emptyArtifacts)
}

// updateSecondaryIndex returns |index| of |indexes| with the entries of the rows in |edits| replaced. A vector or
// Full-Text index, or one missing from |indexes|, is built again from |rows|.
func updateSecondaryIndex(ctx *sql.Context, tbl *doltdb.Table, indexes durable.IndexSet, sch schema.Schema, name doltdb.TableName, index schema.Index, rows prolly.Map, edits []rowEdit) (durable.Index, error) {
	ok, err := indexes.HasIndex(ctx, index.Name())
	if err != nil {
		return nil, err
	}
	if !ok || index.IsVector() || index.IsFullText() {
		return creation.BuildProllyIndexExternal(ctx, tbl.ValueReadWriter(), tbl.NodeStore(), sch, name.Name, index, rows, nil, nil)
	}
	idx, err := indexes.GetIndex(ctx, sch, nil, index.Name())
	if err != nil {
		return nil, err
	}
	m, err := durable.ProllyMapFromIndex(idx)
	if err != nil {
		return nil, err
	}
	mut, err := NewMutableSecondaryIdx(ctx, m, sch, sch, name.Name, index)
	if err != nil {
		return nil, err
	}
	for _, edit := range edits {
		if err = applyEdit(ctx, mut, edit.key, edit.old, edit.new); err != nil {
			return nil, err
		}
	}
	updated, err := mut.Map(ctx)
	if err != nil {
		return nil, err
	}
	return durable.IndexFromProllyMap(updated), nil
}

func buildRowValue(ctx context.Context, builder *val.TupleBuilder, cells []interface{}, ns tree.NodeStore) (val.Tuple, error) {
	builder.Recycle()
	for i, cell := range cells {
		if err := tree.PutField(ctx, ns, builder, i, cell); err != nil {
			return nil, err
		}
	}
	return builder.BuildPermissive(ctx, ns.Pool())
}

// generateStoredColumns returns the row value of |cells| with each stored generated column computed from the row
// |key| and |value|, as a merge computes them for the rows it merges.
func generateStoredColumns(ctx *sql.Context, builder *val.TupleBuilder, cells []interface{}, stored []schema.Column, generated []sql.Expression, sch schema.Schema, tableName string, key, value val.Tuple, ns tree.NodeStore) (val.Tuple, error) {
	row, err := index.BuildRow(ctx, key, value, sch, ns)
	if err != nil {
		return nil, err
	}
	for i, col := range stored {
		if col.Generated == "" {
			continue
		}
		if cells[i], err = evalColumnExpression(ctx, generated[i], col, row, sch, tableName); err != nil {
			return nil, err
		}
	}
	return buildRowValue(ctx, builder, cells, ns)
}

var errCellConflict = errors.New("cell conflict")

// mergeCell returns the merge of column |i| of rows |x| and |y| against the base row |b|, as a merge of the rows
// merges it, or errCellConflict. A cell |markedInBase| conflicted in the merge base, so it counts as changed on both
// sides.
func mergeCell(ctx *sql.Context, vm *valueMerger, i int, x, y, b val.Tuple, markedInBase bool) (interface{}, error) {
	if markedInBase {
		xv, err := vm.mergedCell(ctx, vm.leftVD, vm.leftMapping, x, i)
		if err != nil {
			return nil, err
		}
		yv, err := vm.mergedCell(ctx, vm.rightVD, vm.rightMapping, y, i)
		if err != nil {
			return nil, err
		}
		if equal, err := vm.mergedCellsEqual(ctx, i, xv, yv); err != nil || equal {
			return xv, err
		}
		return nil, errCellConflict
	}
	cell, conflict, err := vm.processColumn(ctx, i, x, y, b)
	if err != nil {
		return nil, err
	}
	if conflict {
		return nil, errCellConflict
	}
	return cell, nil
}

func getRow(ctx context.Context, rows prolly.Map, key val.Tuple) (val.Tuple, error) {
	var value val.Tuple
	err := rows.Get(ctx, key, func(_, v val.Tuple) error {
		value = v
		return nil
	})
	return value, err
}

// takeTableFromBase replaces table |name| of |root|, and its Full-Text pseudo-index tables, with those of |baseRoot|.
func takeTableFromBase(ctx *sql.Context, root, baseRoot doltdb.RootValue, name doltdb.TableName) (doltdb.RootValue, error) {
	merged, err := fullTextTableNames(ctx, root, name)
	if err != nil {
		return nil, err
	}
	for _, tableName := range append(merged, name) {
		if ok, err := root.HasTable(ctx, tableName); err != nil {
			return nil, err
		} else if ok {
			if root, err = root.RemoveTables(ctx, true, true, tableName); err != nil {
				return nil, err
			}
		}
	}
	base, err := fullTextTableNames(ctx, baseRoot, name)
	if err != nil {
		return nil, err
	}
	for _, tableName := range append(base, name) {
		tbl, ok, err := baseRoot.GetTable(ctx, tableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if root, err = root.PutTable(ctx, tableName, tbl); err != nil {
			return nil, err
		}
	}
	return root, nil
}

// fullTextTableNames returns the Full-Text pseudo-index tables of table |name| in |root|.
func fullTextTableNames(ctx *sql.Context, root doltdb.RootValue, name doltdb.TableName) ([]doltdb.TableName, error) {
	tbl, ok, err := root.GetTable(ctx, name)
	if err != nil || !ok {
		return nil, err
	}
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	var names []doltdb.TableName
	for _, idx := range sch.Indexes().AllIndexes() {
		if !idx.IsFullText() {
			continue
		}
		for _, tableName := range idx.FullTextProperties().TableNameSlice() {
			if !seen[tableName] {
				seen[tableName] = true
				names = append(names, doltdb.TableName{Name: tableName, Schema: name.Schema})
			}
		}
	}
	return names, nil
}

// rebuildVirtualBaseFullTextIndexes builds the Full-Text pseudo-index tables of tables |names| of |root|, a virtual
// merge base, from their rows. The merge that made |root| skips that, since the virtual merge base rewrites rows after
// it.
func rebuildVirtualBaseFullTextIndexes(ctx *sql.Context, root doltdb.RootValue, names map[doltdb.TableName]bool) (doltdb.RootValue, error) {
	for name := range names {
		tbl, ok, err := root.GetTable(ctx, name)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		if !sch.Indexes().ContainsFullTextIndex() {
			continue
		}
		if root, err = rebuildFullTextIndexesForTable(ctx, rebuildableFulltextTable{Name: name.Name, Table: tbl, Schema: sch}, root); err != nil {
			return nil, err
		}
	}
	return root, nil
}

// virtualMergeBaseMeta derives every field from |x| and |y|, so merging the same commits always yields the same hash.
func virtualMergeBaseMeta(ctx context.Context, x, y *doltdb.Commit) (*datas.CommitMeta, error) {
	xDate, err := committerDate(ctx, x)
	if err != nil {
		return nil, err
	}
	yDate, err := committerDate(ctx, y)
	if err != nil {
		return nil, err
	}
	if yDate.After(xDate) {
		xDate = yDate
	}
	ident := datas.CommitIdent{Name: virtualMergeBaseName, Email: virtualMergeBaseEmail, Date: datas.CommitDateAt(xDate)}
	return datas.NewCommitMetaWithAuthorCommitter(ident, ident, virtualMergeBaseDescription)
}
