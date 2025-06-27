// Copyright 2022 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type MergeOpts struct {
	// IsCherryPick is set for cherry-pick operations.
	IsCherryPick bool
	// KeepSchemaConflicts is set when schema conflicts should be stored,
	// otherwise the merge errors out when schema conflicts are detected.
	KeepSchemaConflicts bool
	// ReverifyAllConstraints is set to indicate that a merge should not rely on existing
	// constraint violation artifacts and should instead ensure that all constraints are
	// verified. When this option is not set, merge will use optimizations to short circuit
	// some calculations that aren't needed for merge correctness, but are still needed to
	// correctly verify all constraints.
	ReverifyAllConstraints bool
	// RecordViolationsForTables is an optional map that allows the caller to control which
	// tables will have constraint violations recorded as artifacts in the merged tables. When
	// this field is nil or an empty map, constraint violations will be recorded for all tables,
	// but if the map is populated with any (case-insensitive) table names, then only those tables
	// will have constraint violations recorded. This functionality is primarily used by the
	// dolt_verify_constraints() stored procedure to allow callers to verify constraints for a
	// subset of tables.
	RecordViolationsForTables map[doltdb.TableName]struct{}
}

type TableMerger struct {
	name doltdb.TableName

	leftTbl  *doltdb.Table
	rightTbl *doltdb.Table
	ancTbl   *doltdb.Table

	leftRootObj  doltdb.RootObject
	rightRootObj doltdb.RootObject
	ancRootObj   doltdb.RootObject

	leftSch  schema.Schema
	rightSch schema.Schema
	ancSch   schema.Schema

	rightSrc    doltdb.Rootish
	ancestorSrc doltdb.Rootish

	vrw types.ValueReadWriter
	ns  tree.NodeStore

	// recordViolations controls whether constraint violations should be recorded as table
	// artifacts when merging this table. In almost all cases, this should be set to true. The
	// exception is for the dolt_verify_constraints() stored procedure, which allows callers to
	// only record constraint violations for a specified subset of tables.
	recordViolations bool
}

func (tm TableMerger) GetNewValueMerger(mergeSch schema.Schema, leftRows prolly.Map) *valueMerger {
	return NewValueMerger(mergeSch, tm.leftSch, tm.rightSch, tm.ancSch, leftRows.Pool(), leftRows.NodeStore())
}

func rowsFromTable(ctx context.Context, tbl *doltdb.Table) (prolly.Map, error) {
	rd, err := tbl.GetRowData(ctx)
	if err != nil {
		return prolly.Map{}, err
	}
	rows, err := durable.ProllyMapFromIndex(rd)
	if err != nil {
		return prolly.Map{}, err
	}
	return rows, nil
}

func (tm TableMerger) LeftRows(ctx context.Context) (prolly.Map, error) {
	return rowsFromTable(ctx, tm.leftTbl)
}

func (tm TableMerger) RightRows(ctx context.Context) (prolly.Map, error) {
	return rowsFromTable(ctx, tm.rightTbl)
}

func (tm TableMerger) AncRows(ctx context.Context) (prolly.Map, error) {
	return rowsFromTable(ctx, tm.ancTbl)
}

func (tm TableMerger) InvolvesRootObjects() bool {
	return tm.leftRootObj != nil || tm.rightRootObj != nil || tm.ancRootObj != nil
}

func (tm TableMerger) tableHashes(ctx context.Context) (left, right, anc hash.Hash, err error) {
	if tm.leftTbl != nil {
		if left, err = tm.leftTbl.HashOf(); err != nil {
			return
		}
	} else if tm.leftRootObj != nil {
		if left, err = tm.leftRootObj.HashOf(ctx); err != nil {
			return
		}
	}
	if tm.rightTbl != nil {
		if right, err = tm.rightTbl.HashOf(); err != nil {
			return
		}
	} else if tm.rightRootObj != nil {
		if right, err = tm.rightRootObj.HashOf(ctx); err != nil {
			return
		}
	}
	if tm.ancTbl != nil {
		if anc, err = tm.ancTbl.HashOf(); err != nil {
			return
		}
	} else if tm.ancRootObj != nil {
		if anc, err = tm.ancRootObj.HashOf(ctx); err != nil {
			return
		}
	}
	return
}

func (tm TableMerger) SchemaMerge(ctx *sql.Context, tblName doltdb.TableName) (schema.Schema, SchemaConflict, MergeInfo, tree.ThreeWayDiffInfo, error) {
	return SchemaMerge(ctx, tm.vrw.Format(), tm.leftSch, tm.rightSch, tm.ancSch, tblName)
}

type RootMerger struct {
	left  doltdb.RootValue
	right doltdb.RootValue
	anc   doltdb.RootValue

	rightSrc doltdb.Rootish
	ancSrc   doltdb.Rootish

	vrw types.ValueReadWriter
	ns  tree.NodeStore
}

// NewMerger creates a new merger utility object.
func NewMerger(
	left, right, anc doltdb.RootValue,
	rightSrc, ancestorSrc doltdb.Rootish,
	vrw types.ValueReadWriter,
	ns tree.NodeStore,
) (*RootMerger, error) {
	return &RootMerger{
		left:     left,
		right:    right,
		anc:      anc,
		rightSrc: rightSrc,
		ancSrc:   ancestorSrc,
		vrw:      vrw,
		ns:       ns,
	}, nil
}

// MergedResult returns either the merged table or merged root object. Both fields will never be set simultaneously.
type MergedResult struct {
	table    *doltdb.Table     // If non-nil, represents a merged table (and not a merged root object).
	rootObj  doltdb.RootObject // If non-nil, represents a merged root object (and not a merged table).
	conflict SchemaConflict
}

func getDatabaseSchemaNames(ctx context.Context, dest doltdb.RootValue) (*set.StrSet, error) {
	dbSchemaNames := set.NewEmptyStrSet()
	dbSchemas, err := dest.GetDatabaseSchemas(ctx)
	if err != nil {
		return nil, err
	}
	for _, dbSchema := range dbSchemas {
		dbSchemaNames.Add(dbSchema.Name)
	}
	return dbSchemaNames, nil
}

// MergeTable merges schema and table data for the table tblName.
// TODO: this code will loop infinitely when merging certain schema changes
func (rm *RootMerger) MergeTable(
	ctx *sql.Context,
	tblName doltdb.TableName,
	opts editor.Options,
	mergeOpts MergeOpts,
) (*MergedResult, *MergeStats, error) {
	tm, err := rm.MakeTableMerger(ctx, tblName, mergeOpts)
	if err != nil {
		return nil, nil, err
	}

	// short-circuit here if we can
	finished, finishedRootObj, stats, err := rm.MaybeShortCircuit(ctx, tm, mergeOpts)
	if finished != nil || stats != nil || err != nil {
		return &MergedResult{table: finished, rootObj: finishedRootObj}, stats, err
	}

	// Calculate a merge of the schemas, but don't apply it yet
	mergeSch, schConflicts, mergeInfo, diffInfo, err := tm.SchemaMerge(ctx, tblName)
	if err != nil {
		return nil, nil, err
	}
	if schConflicts.Count() > 0 {
		if !mergeOpts.KeepSchemaConflicts {
			return nil, nil, schConflicts
		}
		// handle schema conflicts above
		mt := &MergedResult{
			table:    tm.leftTbl,
			conflict: schConflicts,
		}
		stats = &MergeStats{
			Operation:       TableModified,
			SchemaConflicts: schConflicts.Count(),
		}
		return mt, stats, nil
	}

	var tbl *doltdb.Table
	var rootObj doltdb.RootObject
	if !tm.InvolvesRootObjects() {
		if types.IsFormat_DOLT(tm.vrw.Format()) {
			tbl, stats, err = mergeProllyTable(ctx, tm, mergeSch, mergeInfo, diffInfo)
		} else {
			tbl, stats, err = mergeNomsTable(ctx, tm, mergeSch, rm.vrw, opts)
		}
		if err != nil {
			return nil, nil, err
		}
	} else {
		rootObj, stats, err = MergeRootObjects(ctx, MergeRootObject{
			Name:            tm.name,
			OurRootObj:      tm.leftRootObj,
			TheirRootObj:    tm.rightRootObj,
			AncestorRootObj: tm.ancRootObj,
			RightSrc:        tm.rightSrc,
			AncestorSrc:     tm.ancestorSrc,
			VRW:             tm.vrw,
			NS:              tm.ns,
		})
		if err != nil {
			return nil, nil, err
		}
	}
	return &MergedResult{table: tbl, rootObj: rootObj}, stats, nil
}

func (rm *RootMerger) MakeTableMerger(ctx context.Context, tblName doltdb.TableName, mergeOpts MergeOpts) (*TableMerger, error) {
	recordViolations := true
	if mergeOpts.RecordViolationsForTables != nil {
		if _, ok := mergeOpts.RecordViolationsForTables[tblName.ToLower()]; !ok {
			recordViolations = false
		}
	}

	tm := TableMerger{
		name:             tblName,
		rightSrc:         rm.rightSrc,
		ancestorSrc:      rm.ancSrc,
		vrw:              rm.vrw,
		ns:               rm.ns,
		recordViolations: recordViolations,
	}

	var err error
	var leftSideTableExists, rightSideTableExists, ancTableExists bool

	tm.leftTbl, leftSideTableExists, err = rm.left.GetTable(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if leftSideTableExists {
		if tm.leftSch, err = tm.leftTbl.GetSchema(ctx); err != nil {
			return nil, err
		}
	} else {
		tm.leftRootObj, _, err = rm.left.GetRootObject(ctx, tblName)
		if err != nil {
			return nil, err
		}
	}

	tm.rightTbl, rightSideTableExists, err = rm.right.GetTable(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if rightSideTableExists {
		if tm.rightSch, err = tm.rightTbl.GetSchema(ctx); err != nil {
			return nil, err
		}
	} else {
		tm.rightRootObj, _, err = rm.right.GetRootObject(ctx, tblName)
		if err != nil {
			return nil, err
		}
	}

	// If we need to re-verify all constraints, then we need to stub out tables
	// that don't exist, so that the diff logic can compare an empty table to
	// the table containing the real data. This is required by dolt_verify_constraints()
	// so that we can run the merge logic on all rows in all tables.
	if mergeOpts.ReverifyAllConstraints && !tm.HasRootObject() {
		if !leftSideTableExists && rightSideTableExists {
			// if left side doesn't have the table... stub it out with an empty table from the right side...
			tm.leftSch = tm.rightSch
			tm.leftTbl, err = doltdb.NewEmptyTable(ctx, rm.vrw, rm.ns, tm.leftSch)
			if err != nil {
				return nil, err
			}
		} else if !rightSideTableExists && leftSideTableExists {
			// if left side doesn't have the table... stub it out with an empty table from the right side...
			tm.rightSch = tm.leftSch
			tm.rightTbl, err = doltdb.NewEmptyTable(ctx, rm.vrw, rm.ns, tm.rightSch)
			if err != nil {
				return nil, err
			}
		}
	}

	tm.ancTbl, ancTableExists, err = rm.anc.GetTable(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if ancTableExists {
		if tm.ancSch, err = tm.ancTbl.GetSchema(ctx); err != nil {
			return nil, err
		}
	} else if schema.SchemasAreEqual(tm.leftSch, tm.rightSch) && tm.leftTbl != nil {
		// If left & right added the same table, fill tm.anc with an empty table
		tm.ancSch = tm.leftSch
		tm.ancTbl, err = doltdb.NewEmptyTable(ctx, rm.vrw, rm.ns, tm.ancSch)
		if err != nil {
			return nil, err
		}
	} else {
		tm.ancRootObj, _, err = rm.anc.GetRootObject(ctx, tblName)
		if err != nil {
			return nil, err
		}
	}

	// TODO: need to determine what to do if we have a mix of both tables and root objects (we'll error for now)
	if tm.HasTable() && tm.HasRootObject() {
		return nil, errors.New("Attempting to merge fundamentally different objects, which has not yet been implemented\n" +
			"Please contact us and share how you ran into this error to better help our development efforts.")
	}
	return &tm, nil
}

func (rm *RootMerger) MaybeShortCircuit(ctx context.Context, tm *TableMerger, opts MergeOpts) (*doltdb.Table, doltdb.RootObject, *MergeStats, error) {
	// If we need to re-verify all constraints as part of this merge, then we can't short
	// circuit considering any tables, so return immediately
	if opts.ReverifyAllConstraints {
		return nil, nil, nil, nil
	}

	leftHash, rightHash, baseHash, err := tm.tableHashes(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	leftExists := tm.leftTbl != nil || tm.leftRootObj != nil
	rightExists := tm.rightTbl != nil || tm.rightRootObj != nil
	ancExists := tm.ancTbl != nil || tm.ancRootObj != nil
	areRootObjs := tm.leftRootObj != nil || tm.rightRootObj != nil || tm.ancRootObj != nil

	// Nothing changed
	if leftExists && rightExists && ancExists && leftHash == rightHash && leftHash == baseHash {
		return tm.leftTbl, tm.leftRootObj, &MergeStats{Operation: TableUnmodified}, nil
	}

	// Both made identical changes
	// For keyless tables, this counts as a conflict
	if leftExists && rightExists && leftHash == rightHash && !schema.IsKeyless(tm.leftSch) {
		return tm.leftTbl, tm.leftRootObj, &MergeStats{Operation: TableUnmodified}, nil
	}

	// One or both added this table
	if !ancExists {
		if rightExists && leftExists {
			if !schema.SchemasAreEqual(tm.leftSch, tm.rightSch) {
				return nil, nil, nil, ErrSameTblAddedTwice.New(tm.name)
			}
		} else if leftExists {
			// fast-forward
			return tm.leftTbl, tm.leftRootObj, &MergeStats{Operation: TableUnmodified}, nil
		} else {
			// fast-forward
			return tm.rightTbl, tm.rightRootObj, &MergeStats{Operation: TableAdded}, nil
		}
	}

	// Deleted in both, fast-forward
	if ancExists && !leftExists && !rightExists {
		return nil, nil, &MergeStats{Operation: TableRemoved}, nil
	}

	// Deleted in root or in merge, either a conflict (if any changes in other root) or else a fast-forward
	if ancExists && (!leftExists || !rightExists) {
		var childTable *doltdb.Table
		var childHash hash.Hash
		if rightExists {
			childTable = tm.rightTbl
			childHash = rightHash
		} else {
			childTable = tm.leftTbl
			childHash = leftHash
		}
		if childHash != baseHash {
			schemasEqual, err := doltdb.SchemaHashesEqual(ctx, childTable, tm.ancTbl)
			if err != nil {
				return nil, nil, nil, err
			}
			if schemasEqual || areRootObjs {
				return nil, nil, nil, ErrTableDeletedAndModified
			} else {
				return nil, nil, nil, ErrTableDeletedAndSchemaModified
			}
		}
		// fast-forward
		return nil, nil, &MergeStats{Operation: TableRemoved}, nil
	}

	// Changes only in root, table unmodified
	if rightHash == baseHash {
		return tm.leftTbl, tm.leftRootObj, &MergeStats{Operation: TableUnmodified}, nil
	}

	// Changes only in merge root, fast-forward
	// TODO : no fast-forward when cherry-picking for now
	if !opts.IsCherryPick && leftHash == baseHash {
		ms := MergeStats{Operation: TableModified}
		if leftHash != rightHash && !areRootObjs {
			ms, err = calcTableMergeStats(ctx, tm.leftTbl, tm.rightTbl)
			if err != nil {
				return nil, nil, nil, err
			}
		}
		return tm.rightTbl, tm.rightRootObj, &ms, nil
	}

	// no short-circuit
	return nil, nil, nil, nil
}

// HasTable returns whether any of the table fields have been set.
func (tm TableMerger) HasTable() bool {
	return tm.leftTbl != nil || tm.rightTbl != nil || tm.ancTbl != nil
}

// HasRootObject returns whether any of the root object fields have been set.
func (tm TableMerger) HasRootObject() bool {
	return tm.leftRootObj != nil || tm.rightRootObj != nil || tm.ancRootObj != nil
}

// MergeRootObject contains all the information needed for MergeRootObjects to perform a merge.
type MergeRootObject struct {
	Name            doltdb.TableName
	OurRootObj      doltdb.RootObject
	TheirRootObj    doltdb.RootObject
	AncestorRootObj doltdb.RootObject
	RightSrc        doltdb.Rootish
	AncestorSrc     doltdb.Rootish
	VRW             types.ValueReadWriter
	NS              tree.NodeStore
}

// MergeRootObjects handles merging root objects, which is primarily used by Doltgres. This is implemented as a function
// pointer due to import cycles, as the `doltdb` package is referenced from within this `merge` package. To change this
// to a proper interface would mean that several items would need to be moved into `doltdb`, creating a sort of
// dual-location for the implementation to reside. Keeping this as a pointer makes it much simpler.
var MergeRootObjects = func(ctx context.Context, mro MergeRootObject) (doltdb.RootObject, *MergeStats, error) {
	return nil, nil, errors.New("Dolt does not operate on root objects")
}

func setConflicts(ctx context.Context, cons durable.ConflictIndex, tbl, mergeTbl, ancTbl, tableToUpdate *doltdb.Table) (*doltdb.Table, error) {
	ancSch, err := ancTbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	mergeSch, err := mergeTbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	cs := conflict.NewConflictSchema(ancSch, sch, mergeSch)

	tableToUpdate, err = tableToUpdate.SetConflicts(ctx, cs, cons)
	if err != nil {
		return nil, err
	}

	return tableToUpdate, nil
}

func calcTableMergeStats(ctx context.Context, tbl *doltdb.Table, mergeTbl *doltdb.Table) (MergeStats, error) {
	ms := MergeStats{Operation: TableModified}

	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	mergeRows, err := mergeTbl.GetRowData(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	mergeSch, err := mergeTbl.GetSchema(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	ae := atomicerr.New()
	ch := make(chan diff.DiffStatProgress)
	go func() {
		defer close(ch)
		err := diff.Stat(ctx, ch, rows, mergeRows, sch, mergeSch)

		ae.SetIfError(err)
	}()

	for p := range ch {
		if ae.IsSet() {
			break
		}

		ms.Adds += int(p.Adds)
		ms.Deletes += int(p.Removes)
		ms.Modifications += int(p.Changes)
	}

	if err := ae.Get(); err != nil {
		return MergeStats{}, err
	}

	return ms, nil
}
