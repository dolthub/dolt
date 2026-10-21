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

	result, err := MergeRoots(ctx, doltdb.SimpleTableResolver{}, xRoot, yRoot, baseRoot, y, ancestor, editor.Options{}, MergeOpts{KeepSchemaConflicts: true, AlwaysMergeJSON: true})
	if err != nil {
		return nil, nil, err
	}
	mergedRoot, conflicted, err := resolveVirtualBaseConflicts(ctx, result, xRoot, yRoot, baseRoot, baseConflicts)
	if err != nil {
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
// artifacts, which a virtual merge base does not keep.
func resolveVirtualBaseConflicts(ctx *sql.Context, result *Result, xRoot, yRoot, baseRoot doltdb.RootValue, baseConflicts conflictedCells) (doltdb.RootValue, conflictedCells, error) {
	schemaConflicts := make(map[doltdb.TableName]bool)
	for _, name := range SchemaConflictTableNames(result.SchemaConflicts) {
		schemaConflicts[name] = true
	}

	root := result.Root
	conflicted := make(conflictedCells)
	for name, stats := range result.Stats {
		// Full-Text pseudo-index tables are rebuilt from their parent tables.
		if (!stats.HasArtifacts() && !schemaConflicts[name]) || doltdb.IsFullTextTable(name.Name) {
			continue
		}
		var tbl *doltdb.Table
		var rows map[string]conflictedRow
		var err error
		if !schemaConflicts[name] {
			tbl, rows, err = mergeConflictedRowsByCell(ctx, root, xRoot, yRoot, baseRoot, name, baseConflicts[name])
			if err != nil {
				return nil, nil, err
			}
		}
		if tbl == nil {
			if root, err = takeTableFromBase(ctx, root, baseRoot, name); err != nil {
				return nil, nil, err
			}
			continue
		}
		if root, err = root.PutTable(ctx, name, tbl); err != nil {
			return nil, nil, err
		}
		if root, err = rebuildFullTextIndexesOf(ctx, root, name, tbl); err != nil {
			return nil, nil, err
		}
		if len(rows) > 0 {
			conflicted[name] = rows
		}
	}
	for name := range schemaConflicts {
		if _, ok := result.Stats[name]; !ok {
			var err error
			if root, err = takeTableFromBase(ctx, root, baseRoot, name); err != nil {
				return nil, nil, err
			}
		}
	}
	return root, conflicted, nil
}

// mergeConflictedRowsByCell rewrites each conflicted row of table |name| in |root| as a cell-by-cell merge of the row
// in |xRoot| and |yRoot| against |baseRoot|, keeping the base value of each cell that conflicts. Cells that
// |baseConflicts| marks in the merge base count as changed on both sides. Cells are compared as values of the merged
// schema, so the three sides may have different schemas. A conflicted row of a keyless table keeps its copies in
// |baseRoot| whole. It returns the table without artifacts and the conflicted cells, or a nil table when the table is
// missing from |xRoot| or |yRoot|, or is keyless with different schemas.
func mergeConflictedRowsByCell(ctx *sql.Context, root, xRoot, yRoot, baseRoot doltdb.RootValue, name doltdb.TableName, baseConflicts map[string]conflictedRow) (*doltdb.Table, map[string]conflictedRow, error) {
	merged, _, err := root.GetTable(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	sch, err := merged.GetSchema(ctx)
	if err != nil {
		return nil, nil, err
	}
	keyless := schema.IsKeyless(sch)
	ns := merged.NodeStore()
	schemas := make([]schema.Schema, 3)
	sides := make([]prolly.Map, 3)
	for i, sideRoot := range []doltdb.RootValue{xRoot, yRoot, baseRoot} {
		tbl, ok, err := sideRoot.GetTable(ctx, name)
		if err != nil {
			return nil, nil, err
		}
		var idx durable.Index
		if ok {
			if schemas[i], err = tbl.GetSchema(ctx); err != nil {
				return nil, nil, err
			}
			idx, err = tbl.GetRowData(ctx)
		} else if sideRoot == baseRoot {
			schemas[i] = sch
			idx, err = durable.NewEmptyPrimaryIndex(ctx, merged.ValueReadWriter(), ns, sch)
		} else {
			return nil, nil, nil
		}
		if err != nil {
			return nil, nil, err
		}
		if sides[i], err = durable.ProllyMapFromIndex(idx); err != nil {
			return nil, nil, err
		}
	}
	xRows, yRows, baseRows := sides[0], sides[1], sides[2]
	// A keyless row is identified by its values, so the sides of a keyless table must have the same schema.
	if keyless && (!schema.SchemasAreEqual(sch, schemas[0]) || !schema.SchemasAreEqual(sch, schemas[1]) ||
		!schema.SchemasAreEqual(sch, schemas[2])) {
		return nil, nil, nil
	}
	vm := NewValueMerger(ctx, sch, schemas[0], schemas[1], schemas[2], xRows.Pool(), ns)

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

	var stored []schema.Column
	for _, col := range sch.GetNonPKCols().GetColumns() {
		if !col.Virtual {
			stored = append(stored, col)
		}
	}
	var generated []sql.Expression
	if hasStoredGeneratedColumns(sch) {
		if generated, err = resolveDefaults(ctx, name.Name, sch, sch); err != nil {
			return nil, nil, err
		}
	}
	builder := val.NewTupleBuilder(sch.GetValueDescriptor(ns), ns)
	cells := make([]interface{}, len(stored))
	mut := mergedRows.Mutate()
	rows := make(map[string]conflictedRow)
	for {
		art, err := conflicts.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, nil, err
		}
		x, err := getRow(ctx, xRows, art.Key)
		if err != nil {
			return nil, nil, err
		}
		y, err := getRow(ctx, yRows, art.Key)
		if err != nil {
			return nil, nil, err
		}
		b, err := getRow(ctx, baseRows, art.Key)
		if err != nil {
			return nil, nil, err
		}
		// The merge bases of a conflicted keyless row disagree on how many copies of it there are. Like a conflicted
		// line in git's text merge, it keeps its base's copies and is marked.
		if keyless {
			if b == nil {
				err = mut.Delete(ctx, art.Key)
			} else {
				err = mut.Put(ctx, art.Key, b)
			}
			if err != nil {
				return nil, nil, err
			}
			rows[string(art.Key)] = conflictedRow{wholeRow: true}
			continue
		}

		var row conflictedRow
		// A row that one merge base deleted and the other changed takes the base's row unmarked, as git's merge-ort takes
		// the base's version of a file for a modify/delete conflict. A row added twice is marked, as git's text merge of
		// two added files leaves conflict markers.
		wholeRow := x == nil || y == nil || b == nil
		baseRow := baseConflicts[string(art.Key)]
		for i, col := range stored {
			var bv interface{}
			if b != nil {
				if bv, err = vm.mergedCell(ctx, vm.baseVD, vm.baseMapping, b, i); err != nil {
					return nil, nil, err
				}
			}
			cells[i] = bv
			// A generated value is computed from its sources below, so only they conflict.
			if !wholeRow && col.Generated == "" {
				cells[i], err = mergeCell(ctx, vm, i, x, y, b, bv, baseRow.wholeRow || slices.Contains(baseRow.tags, col.Tag))
				if errors.Is(err, errCellConflict) {
					cells[i] = bv
					row.tags = append(row.tags, col.Tag)
				} else if err != nil {
					return nil, nil, err
				}
			}
		}

		if b == nil && wholeRow {
			err = mut.Delete(ctx, art.Key)
		} else {
			var value val.Tuple
			if value, err = buildRowValue(ctx, builder, cells, ns); err == nil && generated != nil && !wholeRow {
				value, err = generateStoredColumns(ctx, builder, cells, stored, generated, sch, art.Key, value, ns)
			}
			if err == nil {
				err = mut.Put(ctx, art.Key, value)
			}
		}
		if err != nil {
			return nil, nil, err
		}
		if wholeRow && b == nil {
			rows[string(art.Key)] = conflictedRow{wholeRow: true}
		} else if len(row.tags) > 0 {
			rows[string(art.Key)] = row
		}
	}

	newRows, err := mut.Map(ctx)
	if err != nil {
		return nil, nil, err
	}
	tbl, err := merged.UpdateRows(ctx, durable.IndexFromProllyMap(newRows))
	if err != nil {
		return nil, nil, err
	}
	indexes, err := tbl.GetIndexSet(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, index := range sch.Indexes().AllIndexes() {
		built, err := creation.BuildProllyIndexExternal(ctx, tbl.ValueReadWriter(), ns, sch, name.Name, index, newRows, nil, nil)
		if err != nil {
			return nil, nil, err
		}
		if indexes, err = indexes.PutIndex(ctx, index.Name(), built); err != nil {
			return nil, nil, err
		}
	}
	if tbl, err = tbl.SetIndexSet(ctx, indexes); err != nil {
		return nil, nil, err
	}
	emptyArtifacts, err := durable.NewEmptyArtifactIndex(ctx, tbl.ValueReadWriter(), ns, sch)
	if err != nil {
		return nil, nil, err
	}
	tbl, err = tbl.SetArtifacts(ctx, emptyArtifacts)
	return tbl, rows, err
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
func generateStoredColumns(ctx *sql.Context, builder *val.TupleBuilder, cells []interface{}, stored []schema.Column, generated []sql.Expression, sch schema.Schema, key, value val.Tuple, ns tree.NodeStore) (val.Tuple, error) {
	row, err := index.BuildRow(ctx, key, value, sch, ns)
	if err != nil {
		return nil, err
	}
	for i, col := range stored {
		if col.Generated == "" {
			continue
		}
		v, err := generated[i].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if cells[i], _, err = col.TypeInfo.ToSqlType().Convert(ctx, v); err != nil {
			return nil, err
		}
	}
	return buildRowValue(ctx, builder, cells, ns)
}

var errCellConflict = errors.New("cell conflict")

// mergeCell returns the merge of column |i| of rows |x| and |y| against the base row |b|, whose value is |bv|, or
// errCellConflict. JSON documents merge as they do in a merge of rows. A cell |markedInBase| conflicted in the merge
// base, so it counts as changed on both sides.
func mergeCell(ctx context.Context, vm *valueMerger, i int, x, y, b val.Tuple, bv interface{}, markedInBase bool) (interface{}, error) {
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
	if markedInBase {
		return nil, errCellConflict
	}
	if equal, err := vm.mergedCellsEqual(ctx, i, yv, bv); err != nil || equal {
		return xv, err
	}
	if equal, err := vm.mergedCellsEqual(ctx, i, xv, bv); err != nil || equal {
		return yv, err
	}
	return mergeJSONCell(ctx, vm, i, x, y, b)
}

// mergeJSONCell returns the merge of the JSON documents in column |i| of rows |x| and |y| against row |b|, or
// errCellConflict when the column is not JSON in all three or the documents do not merge.
func mergeJSONCell(ctx context.Context, vm *valueMerger, i int, x, y, b val.Tuple) (interface{}, error) {
	enc := vm.resultVD.Types[i].Enc
	if enc != val.JsonAdaptiveEnc && enc != val.JSONAddrEnc {
		return nil, errCellConflict
	}
	var fields [3][]byte
	for side, s := range []struct {
		vd      *val.TupleDesc
		mapping val.OrdinalMapping
		row     val.Tuple
	}{{vm.baseVD, vm.baseMapping, b}, {vm.leftVD, vm.leftMapping, x}, {vm.rightVD, vm.rightMapping, y}} {
		j := s.mapping[i]
		if j == -1 || s.vd.Types[j].Enc != enc {
			return nil, errCellConflict
		}
		if fields[side] = s.row.GetField(j); fields[side] == nil {
			return nil, errCellConflict
		}
	}
	var merged interface{}
	var conflict bool
	var err error
	if enc == val.JsonAdaptiveEnc {
		merged, conflict, err = vm.mergeJSONAdaptive(ctx, fields[0], fields[1], fields[2])
	} else {
		merged, conflict, err = vm.mergeJSONAddr(ctx, fields[0], fields[1], fields[2])
	}
	if err == nil && conflict {
		err = errCellConflict
	}
	return merged, err
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

// rebuildFullTextIndexesOf rebuilds the Full-Text pseudo-index tables of |tbl|, table |name| of |root|, from its rows.
func rebuildFullTextIndexesOf(ctx *sql.Context, root doltdb.RootValue, name doltdb.TableName, tbl *doltdb.Table) (doltdb.RootValue, error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	if !sch.Indexes().ContainsFullTextIndex() {
		return root, nil
	}
	return rebuildFullTextIndexesForTable(ctx, rebuildableFulltextTable{Name: name.Name, Table: tbl, Schema: sch}, root)
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
