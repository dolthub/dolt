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
// the returned cells are treated as changed on both sides when merging against the virtual merge base. A keyless table,
// or a table whose schema conflicts, is taken whole from that merge base. Like git's, the virtual merge base is never
// checked out, so it keeps the merged rows even where they violate a constraint, and drops the violations.
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
		if !stats.HasArtifacts() && !schemaConflicts[name] {
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
// schema, so the three sides may have different schemas. It returns the table without artifacts and the conflicted
// cells, or a nil table when the table is keyless or missing from |xRoot| or |yRoot|.
func mergeConflictedRowsByCell(ctx *sql.Context, root, xRoot, yRoot, baseRoot doltdb.RootValue, name doltdb.TableName, baseConflicts map[string]conflictedRow) (*doltdb.Table, map[string]conflictedRow, error) {
	merged, _, err := root.GetTable(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	sch, err := merged.GetSchema(ctx)
	if err != nil {
		return nil, nil, err
	}
	if schema.IsKeyless(sch) {
		return nil, nil, nil
	}
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
	builder := val.NewTupleBuilder(sch.GetValueDescriptor(ns), ns)
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

		builder.Recycle()
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
			cell := bv
			if !wholeRow {
				cell, err = mergeCell(ctx, vm, i, x, y, bv, baseRow.wholeRow || slices.Contains(baseRow.tags, col.Tag))
				if errors.Is(err, errCellConflict) {
					// A generated value is computed from its sources, so only they conflict.
					cell = bv
					if col.Generated == "" {
						row.tags = append(row.tags, col.Tag)
					}
				} else if err != nil {
					return nil, nil, err
				}
			}
			if err = tree.PutField(ctx, ns, builder, i, cell); err != nil {
				return nil, nil, err
			}
		}

		if b == nil && wholeRow {
			err = mut.Delete(ctx, art.Key)
		} else {
			var value val.Tuple
			if value, err = builder.BuildPermissive(ctx, ns.Pool()); err == nil {
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

var errCellConflict = errors.New("cell conflict")

// mergeCell returns the merge of column |i| of rows |x| and |y| against the base value |bv|, or errCellConflict. A
// cell |markedInBase| conflicted in the merge base, so it counts as changed on both sides.
func mergeCell(ctx context.Context, vm *valueMerger, i int, x, y val.Tuple, bv interface{}, markedInBase bool) (interface{}, error) {
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
	return nil, errCellConflict
}

func getRow(ctx context.Context, rows prolly.Map, key val.Tuple) (val.Tuple, error) {
	var value val.Tuple
	err := rows.Get(ctx, key, func(_, v val.Tuple) error {
		value = v
		return nil
	})
	return value, err
}

func takeTableFromBase(ctx *sql.Context, root, baseRoot doltdb.RootValue, name doltdb.TableName) (doltdb.RootValue, error) {
	baseTable, ok, err := baseRoot.GetTable(ctx, name)
	if err != nil {
		return nil, err
	}
	if ok {
		return root.PutTable(ctx, name, baseTable)
	}
	return root.RemoveTables(ctx, true, true, name)
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
