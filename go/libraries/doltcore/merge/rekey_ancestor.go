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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	errorkinds "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var ErrRekeyedAncestorCollision = errorkinds.NewKind("error: cannot merge table %s: both sides changed its primary key, and two rows of the common ancestor have the same new primary key")
var ErrRekeyedAncestorNullKey = errorkinds.NewKind("error: cannot merge table %s: both sides changed its primary key, and a row of the common ancestor has a NULL in the new primary key")

func init() {
	doltdb.RebuildRekeyedMergeBase = rebuildRekeyedMergeBase
}

// rekeyAncestor returns |ancRoot| with each table of |tblNames| whose primary key was changed the same way on both
// sides of the merge rewritten to that new primary key. The merge diffs each side against the ancestor by primary
// key, so an ancestor keyed differently from both sides cannot otherwise be merged against. Tables that cannot be
// rewritten are left alone, and SchemaMerge rejects them as before.
//
// When it rewrites a table, it also returns a rekeyedMergeBase for |ancestor|, so that conflicts reference a base
// keyed like their rows.
func rekeyAncestor(ctx *sql.Context, ourRoot, theirRoot, ancRoot doltdb.RootValue, ancestor doltdb.Rootish, tblNames []doltdb.TableName) (doltdb.RootValue, doltdb.Rootish, error) {
	var conflicted conflictedCells
	if virtualBase, ok := ancestor.(virtualMergeBase); ok {
		conflicted = virtualBase.conflicted
	}
	rekeyedRoot, rekeyedAny, err := rekeyAncestorTables(ctx, ourRoot, theirRoot, ancRoot, conflicted, tblNames, false)
	if err != nil || !rekeyedAny {
		return rekeyedRoot, ancestor, err
	}
	rekeyedBase, err := newRekeyedMergeBase(ctx, rekeyedRoot, ancestor, conflicted)
	if err != nil {
		return nil, nil, err
	}
	return rekeyedRoot, rekeyedBase, nil
}

// rebuildRekeyedMergeBase rebuilds the rekeyed merge base of a conflict with |meta| from |theirRootIsh| by rekeying
// its merge base again. Each table is rekeyed to their primary key, which the merge rekeyed it to. A table that cannot
// be rekeyed is left alone: the merge did not rekey it, and so it has no conflicts against the rekeyed merge base.
func rebuildRekeyedMergeBase(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, meta prolly.ConflictMetadata, theirRootIsh hash.Hash) (doltdb.RootValue, error) {
	from := prolly.ConflictMetadata{BaseRootIsh: meta.RekeyedFrom, MergeBases: meta.MergeBases}
	ancRoot, err := doltdb.LoadConflictBaseRoot(ctx, vrw, ns, from, theirRootIsh)
	if err != nil {
		return nil, err
	}
	theirRoot, err := doltdb.LoadRootValueFromRootIshAddr(ctx, vrw, ns, theirRootIsh)
	if err != nil {
		return nil, err
	}
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		sqlCtx = sql.NewContext(ctx)
	}
	tblNames, err := doltdb.UnionTableNames(sqlCtx, theirRoot)
	if err != nil {
		return nil, err
	}
	rekeyed, _, err := rekeyAncestorTables(sqlCtx, theirRoot, theirRoot, ancRoot, nil, tblNames, true)
	return rekeyed, err
}

// rekeyAncestorTables rekeys the tables of |ancRoot| for rekeyAncestor, and returns whether it rekeyed any. It skips
// the tables with |conflicted| cells. It also skips the tables whose rows collide or have a NULL under the new primary
// key, when |skipUnrekeyable| or when the table is the same on both sides: both sides ran the same migration, so the
// merge takes the table without merging it and must not fail because its ancestor cannot be rekeyed.
func rekeyAncestorTables(ctx *sql.Context, ourRoot, theirRoot, ancRoot doltdb.RootValue, conflicted conflictedCells, tblNames []doltdb.TableName, skipUnrekeyable bool) (doltdb.RootValue, bool, error) {
	rekeyedAny := false
	for _, name := range tblNames {
		// The conflicted cells of a virtual merge base are keyed by the ancestor's primary key.
		if len(conflicted[name]) > 0 {
			continue
		}
		ancTbl, ok, err := ancRoot.GetTable(ctx, name)
		if err != nil || !ok {
			if err != nil {
				return nil, false, err
			}
			continue
		}
		ourTbl, ok, err := ourRoot.GetTable(ctx, name)
		if err != nil || !ok {
			if err != nil {
				return nil, false, err
			}
			continue
		}
		theirTbl, ok, err := theirRoot.GetTable(ctx, name)
		if err != nil || !ok {
			if err != nil {
				return nil, false, err
			}
			continue
		}
		ancSch, err := ancTbl.GetSchema(ctx)
		if err != nil {
			return nil, false, err
		}
		ourSch, err := ourTbl.GetSchema(ctx)
		if err != nil {
			return nil, false, err
		}
		theirSch, err := theirTbl.GetSchema(ctx)
		if err != nil {
			return nil, false, err
		}
		if !needsRekeyedAncestor(ourSch, theirSch, ancSch) {
			continue
		}
		sameOnBothSides, err := sameTable(ourTbl, theirTbl)
		if err != nil {
			return nil, false, err
		}
		rekeyed, ok, err := RekeyTable(ctx, name, ancTbl, ancSch, ourSch)
		if (skipUnrekeyable || sameOnBothSides) && (ErrRekeyedAncestorCollision.Is(err) || ErrRekeyedAncestorNullKey.Is(err)) {
			continue
		} else if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		if ancRoot, err = ancRoot.PutTable(ctx, name, rekeyed); err != nil {
			return nil, false, err
		}
		rekeyedAny = true
	}
	return ancRoot, rekeyedAny, nil
}

const rekeyedMergeBaseDescription = "rekeyed merge base"

// rekeyedMergeBase is the ancestor Rootish of a merge whose ancestor had tables rekeyed to the primary key both sides
// changed them to. It is a dangling commit of the rekeyed ancestor, whose parent is the original ancestor when that
// is a commit. Conflicts hold its address, which keeps it from being garbage collected while they exist.
type rekeyedMergeBase struct {
	*doltdb.Commit
	// from is the merge base that was rekeyed, and mergeBases are its merge bases when it is a virtual merge base.
	from       hash.Hash
	mergeBases []hash.Hash
	// conflicted are the conflicted cells of the virtual merge base that was rekeyed, if any.
	conflicted conflictedCells
}

// newRekeyedMergeBase returns a rekeyedMergeBase of |root|, the rekeyed root of |ancestor|. Its commit takes the date
// of |ancestor| when that is a commit, so that the same merge always builds the same commit.
func newRekeyedMergeBase(ctx *sql.Context, root doltdb.RootValue, ancestor doltdb.Rootish, conflicted conflictedCells) (rekeyedMergeBase, error) {
	from, err := ancestor.HashOf()
	if err != nil {
		return rekeyedMergeBase{}, err
	}
	var parent *doltdb.Commit
	var mergeBases []hash.Hash
	switch a := ancestor.(type) {
	case *doltdb.Commit:
		parent = a
	case virtualMergeBase:
		parent = a.Commit
		mergeBases = a.mergeBases
	}
	var parents []*doltdb.Commit
	date := time.Unix(0, 0)
	if parent != nil {
		parents = []*doltdb.Commit{parent}
		if date, err = committerDate(ctx, parent); err != nil {
			return rekeyedMergeBase{}, err
		}
	}
	ident := datas.CommitIdent{Name: virtualMergeBaseName, Email: virtualMergeBaseEmail, Date: datas.CommitDateAt(date)}
	meta, err := datas.NewCommitMetaWithAuthorCommitter(ident, ident, rekeyedMergeBaseDescription)
	if err != nil {
		return rekeyedMergeBase{}, err
	}
	commit, err := doltdb.NewDanglingCommit(ctx, root, parents, meta)
	if err != nil {
		return rekeyedMergeBase{}, err
	}
	return rekeyedMergeBase{Commit: commit, from: from, mergeBases: mergeBases, conflicted: conflicted}, nil
}

// needsRekeyedAncestor returns whether both sides agree on a primary key that differs from the ancestor's.
func needsRekeyedAncestor(ourSch, theirSch, ancSch schema.Schema) bool {
	if schema.IsKeyless(ourSch) || schema.IsKeyless(ancSch) {
		return false
	}
	return schema.ComparePrimaryKeys(ourSch, theirSch) == schema.PrimaryKeysSameTags &&
		schema.ComparePrimaryKeys(ourSch, ancSch) != schema.PrimaryKeysSameTags
}

// RekeyTable returns |tbl|, whose schema is |sch|, rewritten to the primary key of |target|: the same columns and
// rows, keyed and indexed by the columns of |target|'s primary key. It returns false when |sch| lacks one of those
// columns, or holds it with another type or encoding.
func RekeyTable(ctx *sql.Context, name doltdb.TableName, tbl *doltdb.Table, sch, target schema.Schema) (*doltdb.Table, bool, error) {
	newSch, ok, err := schema.RekeyedSchema(sch, target)
	if err != nil || !ok {
		return nil, false, err
	}
	if hasArtifacts, err := tableHasArtifacts(ctx, tbl); err != nil || hasArtifacts {
		return nil, false, err
	}

	vrw, ns := tbl.ValueReadWriter(), tbl.NodeStore()
	rd, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, false, err
	}
	oldRows, err := durable.ProllyMapFromIndex(rd)
	if err != nil {
		return nil, false, err
	}
	rk, ok := durable.NewRowRekeyer(ns, sch, newSch)
	if !ok {
		return nil, false, nil
	}
	newRows, unpaired, err := rk.RekeyMap(ctx, oldRows)
	if err != nil {
		return nil, false, err
	}
	if err = unpairedRowsError(ctx, name, rk, unpaired); err != nil {
		return nil, false, err
	}

	indexes, err := durable.NewIndexSet(ctx, vrw, ns)
	if err != nil {
		return nil, false, err
	}
	for _, index := range newSch.Indexes().AllIndexes() {
		built, err := creation.BuildProllyIndexExternal(ctx, vrw, ns, newSch, name.Name, index, newRows, nil, nil)
		if err != nil {
			return nil, false, err
		}
		if indexes, err = indexes.PutIndex(ctx, index.Name(), built); err != nil {
			return nil, false, err
		}
	}

	var autoInc types.Value
	if schema.HasAutoIncrement(newSch) {
		v, err := tbl.GetAutoIncrementValue(ctx)
		if err != nil {
			return nil, false, err
		}
		autoInc = types.Uint(v)
	}
	rekeyed, err := doltdb.NewTable(ctx, vrw, ns, newSch, durable.IndexFromProllyMap(newRows), indexes, autoInc)
	if err != nil {
		return nil, false, err
	}
	return rekeyed, true, nil
}

// unpairedRowsError returns the error for the rows of table |name| that |rk|
// kept apart in |unpaired|, or nil when there are none. It reports a NULL in
// the new primary key before a shared key.
//
// A merge needs one ancestor row for each key, so it refuses the rows that a
// diff shows as removed or added.
func unpairedRowsError(ctx context.Context, name doltdb.TableName, rk *durable.RowRekeyer, unpaired prolly.Map) error {
	n, err := unpaired.Count()
	if err != nil || n == 0 {
		return err
	}
	hasNullKey := false
	err = durable.IterRows(ctx, unpaired, func(k, v val.Tuple) error {
		hasNullKey = hasNullKey || rk.HasNullKey(k, v)
		return nil
	})
	switch {
	case err != nil:
		return err
	case hasNullKey:
		return ErrRekeyedAncestorNullKey.New(name)
	}
	return ErrRekeyedAncestorCollision.New(name)
}

func tableHasArtifacts(ctx context.Context, tbl *doltdb.Table) (bool, error) {
	arts, err := tbl.GetArtifacts(ctx)
	if err != nil {
		return false, err
	}
	cnt, err := arts.Count()
	return cnt > 0, err
}

func sameTable(a, b *doltdb.Table) (bool, error) {
	aHash, err := a.HashOf()
	if err != nil {
		return false, err
	}
	bHash, err := b.HashOf()
	if err != nil {
		return false, err
	}
	return aHash == bHash, nil
}
