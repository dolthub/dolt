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

package dfunctions

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var ErrConfSchIncompatible = errors.New("the conflict schema's columns are not equal to the current schema's columns, please resolve manually")

// DoltConflictsCatFunc runs a `dolt commit` in the SQL context, committing staged changes to head.
// Deprecated: please use the version in the dprocedures package
type DoltConflictsCatFunc struct {
	children []sql.Expression
}

// NewDoltConflictsResolveFunc creates a new DoltCommitFunc expression whose children represents the args passed in DOLT_CONFLICTS_RESOLVE.
// Deprecated: please use the version in the dprocedures package
func NewDoltConflictsResolveFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltConflictsCatFunc{children: args}, nil
}

func (d DoltConflictsCatFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return 1, err
	}
	return DoDoltConflictsResolve(ctx, args)
}

func getProllyRowMaps(ctx *sql.Context, vrw types.ValueReadWriter, ns tree.NodeStore, hash hash.Hash, tblName string) (prolly.Map, error) {
	rootVal, err := doltdb.LoadRootValueFromRootIshAddr(ctx, vrw, ns, hash)
	tbl, ok, err := rootVal.GetTable(ctx, tblName)
	if err != nil {
		return prolly.Map{}, err
	}
	if !ok {
		return prolly.Map{}, doltdb.ErrTableNotFound
	}

	idx, err := tbl.GetRowData(ctx)
	if err != nil {
		return prolly.Map{}, err
	}

	return durable.ProllyMapFromIndex(idx), nil
}

func resolveProllyConflicts(ctx *sql.Context, tbl *doltdb.Table, tblName string, sch schema.Schema, ours bool) (*doltdb.Table, error) {
	var err error
	artifactIdx, err := tbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}

	artifactMap := durable.ProllyMapFromArtifactIndex(artifactIdx)
	iter, err := artifactMap.IterAllConflicts(ctx)
	if err != nil {
		return nil, err
	}

	// just get first conflict artifact
	cnfArt, err := iter.Next(ctx)
	if err != nil {
		if err == io.EOF {
			panic("no conflicts, should be impossible")
		}
		return nil, err
	}

	ourIdx, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	ourMap := durable.ProllyMapFromIndex(ourIdx)

	baseRootVal, err := doltdb.LoadRootValueFromRootIshAddr(ctx, tbl.ValueReadWriter(), tbl.NodeStore(), cnfArt.Metadata.BaseRootIsh)
	baseTbl, ok, err := baseRootVal.GetTable(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, doltdb.ErrTableNotFound
	}
	baseIdx, err := baseTbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	baseMap := durable.ProllyMapFromIndex(baseIdx)

	theirMap, err := getProllyRowMaps(ctx, tbl.ValueReadWriter(), tbl.NodeStore(), cnfArt.TheirRootIsh, tblName)
	if err != nil {
		return nil, err
	}

	// swap the maps when resolving with theirs
	if !ours {
		ourMap, theirMap = theirMap, ourMap
	}

	// resolve conflicts with left
	var indexEdits []tree.Diff
	merged, _, err := prolly.MergeMaps(ctx, ourMap, theirMap, baseMap, func(left, right tree.Diff) (tree.Diff, bool) {
		if left.From != nil && ((left.To == nil) != (right.To == nil)) {
			indexEdits = append(indexEdits, left)
			return left, true
		}
		if bytes.Compare(left.To, right.To) == 0 {
			indexEdits = append(indexEdits, right)
			return right, true
		} else {
			indexEdits = append(indexEdits, left)
			return left, true
		}
	})
	if err != nil {
		return nil, err
	}

	idx := durable.IndexFromProllyMap(merged)
	newTbl, err := tbl.UpdateRows(ctx, idx)
	if err != nil {
		return nil, err
	}

	// TODO: need to either build from base or delete all the ones that aren't supposed to be there
	idxSet, err := baseTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	secondaryIdxs, err := merge.GetMutableSecondaryIdxs(ctx, sch, idxSet)
	if err != nil {
		return nil, err
	}
	for _, edit := range indexEdits {
		for _, secIdx := range secondaryIdxs {
			if len(edit.From) == 0 {
				err := secIdx.InsertEntry(ctx, val.Tuple(edit.Key), val.Tuple(edit.To))
				if err != nil {
					return nil, err
				}
			} else if len(edit.To) == 0 {
				err := secIdx.DeleteEntry(ctx, val.Tuple(edit.Key), val.Tuple(edit.From))
				if err != nil {
					return nil, err
				}
			} else {
				err := secIdx.UpdateEntry(ctx, val.Tuple(edit.Key), val.Tuple(edit.From), val.Tuple(edit.To))
				if err != nil {
					return nil, err
				}
			}
			m, err := secIdx.Map(ctx)
			if err != nil {
				return nil, err
			}
			idxSet, err = idxSet.PutIndex(ctx, secIdx.Name, durable.IndexFromProllyMap(m))
			if err != nil {
				return nil, err
			}
		}
	}

	newTbl, err = newTbl.SetIndexSet(ctx, idxSet)
	if err != nil {
		return nil, err
	}

	return newTbl, nil
}

func resolvePkConflicts(ctx *sql.Context, tbl *doltdb.Table, tblName string, sch schema.Schema, tblEditor editor.TableEditor, ours bool) (*doltdb.Table, error) {
	// Get conflicts
	_, cnfIdx, err := tbl.GetConflicts(ctx)
	if err != nil {
		return nil, err
	}

	// Iterate over conflicts, resolve by picking either ours or theirs
	conflicts := durable.NomsMapFromConflictIndex(cnfIdx)
	err = conflicts.Iter(ctx, func(key, val types.Value) (stop bool, err error) {
		cnf, err := conflict.ConflictFromTuple(val.(types.Tuple))
		if err != nil {
			return true, err
		}

		k := key.(types.Tuple)
		var newVal types.Value
		if ours {
			newVal = cnf.Value
		} else {
			newVal = cnf.MergeValue
		}

		// resolve by deleting row
		if types.IsNull(newVal) {
			baseRow, err := row.FromNoms(sch, k, cnf.Base.(types.Tuple))
			if err != nil {
				return true, err
			}
			err = tblEditor.DeleteRow(ctx, baseRow)
			if err != nil {
				return true, err
			}
			return false, nil
		}

		newRow, err := row.FromNoms(sch, k, newVal.(types.Tuple))
		if err != nil {
			return true, err
		}

		if isValid, err := row.IsValid(newRow, sch); err != nil {
			return true, err
		} else if !isValid {
			return true, table.NewBadRow(newRow, "error resolving conflicts", fmt.Sprintf("row with primary key %v in table %s does not match constraints or types of the table's schema.", key, tblName))
		}

		// resolve by inserting new row
		if types.IsNull(cnf.Value) {
			err = tblEditor.InsertRow(ctx, newRow, nil)
			if err != nil {
				return true, err
			}
			return false, nil
		}

		// resolve by updating existing row
		oldRow, err := row.FromNoms(sch, k, cnf.Value.(types.Tuple))
		if err != nil {
			return true, err
		}
		err = tblEditor.UpdateRow(ctx, oldRow, newRow, nil)
		if err != nil {
			return true, err
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return tblEditor.Table(ctx)
}

func resolveKeylessConflicts(ctx *sql.Context, tbl *doltdb.Table, tblName string, sch schema.Schema, tblEditor editor.TableEditor, ours bool) (*doltdb.Table, error) {
	// Iterate over conflicts, resolve by picking either ours or theirs
	cnfReader, err := merge.NewConflictReader(ctx, tbl, tblName)
	if err != nil {
		return nil, err
	}
	joiner := cnfReader.GetJoiner()

	// get relevant cardinality tags
	cnfSch := cnfReader.GetSchema()
	tags := cnfSch.GetAllCols().SortedTags
	ourCardinalityTag := tags[len(tags)-2]
	theirCardinalityTag := tags[len(tags)-1]

	for {
		cnfRow, err := cnfReader.NextConflict(ctx)
		if err == io.EOF {
			break
		}
		cnfMap, err := joiner.Split(cnfRow)
		if err != nil {
			return nil, err
		}

		// Get cardinality
		var ourCardinality, theirCardinality uint64
		if ourCardinalityVal, ok := cnfRow.GetColVal(ourCardinalityTag); ok {
			ourCardinality = uint64(ourCardinalityVal.(types.Uint))
		} else {
			panic("shouldn't be possible")
		}
		if theirCardinalityVal, ok := cnfRow.GetColVal(theirCardinalityTag); ok {
			theirCardinality = uint64(theirCardinalityVal.(types.Uint))
		} else {
			panic("shouldn't be possible")
		}

		rowDelta := 0
		var newRow row.Row
		if ours {
			newRow = cnfMap["our"]
		} else {
			newRow = cnfMap["their"]
			rowDelta = int(theirCardinality - ourCardinality)
		}

		if rowDelta > 0 {
			for i := 0; i < rowDelta; i++ {
				tblEditor.InsertRow(ctx, newRow, nil)
			}
		} else {
			rowDelta *= -1
			for i := 0; i < rowDelta; i++ {
				tblEditor.DeleteRow(ctx, cnfMap["our"])
			}
		}
	}

	return tblEditor.Table(ctx)
}

func resolveNomsConflicts(ctx *sql.Context, dEnv *env.DoltEnv, tbl *doltdb.Table, tblName string, sch schema.Schema, ours bool) (*doltdb.Table, error) {
	// Create new table editor
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, err
	}
	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: tmpDir}
	tblEditor, err := editor.NewTableEditor(ctx, tbl, sch, tblName, opts)
	if err != nil {
		return nil, err
	}

	if schema.IsKeyless(sch) {
		return resolveKeylessConflicts(ctx, tbl, tblName, sch, tblEditor, ours)
	} else {
		return resolvePkConflicts(ctx, tbl, tblName, sch, tblEditor, ours)
	}
}

func ResolveConflicts(ctx *sql.Context, dEnv *env.DoltEnv, dSess *dsess.DoltSession, root *doltdb.RootValue, dbName string, ours bool, tblNames []string) error {
	newRoot := root
	for _, tblName := range tblNames {
		tbl, ok, err := newRoot.GetTable(ctx, tblName)
		if err != nil {
			return err
		}
		if !ok {
			return doltdb.ErrTableNotFound
		}

		if has, err := tbl.HasConflicts(ctx); err != nil {
			return err
		} else if !has {
			return nil
		}

		sch, err := tbl.GetSchema(ctx)
		if err != nil {
			return err
		}
		_, ourSch, theirSch, err := tbl.GetConflictSchemas(ctx, tblName)
		if err != nil {
			return err
		}

		if ours && !schema.ColCollsAreEqual(sch.GetAllCols(), ourSch.GetAllCols()) {
			return ErrConfSchIncompatible
		} else if !ours && !schema.ColCollsAreEqual(sch.GetAllCols(), theirSch.GetAllCols()) {
			return ErrConfSchIncompatible
		}

		var newTbl *doltdb.Table
		if tbl.Format() == types.Format_DOLT {
			newTbl, err = resolveProllyConflicts(ctx, tbl, tblName, sch, ours)
		} else {
			newTbl, err = resolveNomsConflicts(ctx, dEnv, tbl, tblName, sch, ours)
		}

		newTbl, err = newTbl.ClearConflicts(ctx)
		if err != nil {
			return err
		}

		newRoot, err = newRoot.PutTable(ctx, tblName, newTbl)
		if err != nil {
			return err
		}
	}
	return dSess.SetRoot(ctx, dbName, newRoot)
}

func DoDoltConflictsResolve(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()
	fmt.Printf("database name: %s", dbName)

	apr, err := cli.CreateConflictsResolveArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	ours := apr.Contains(cli.OursFlag)
	theirs := apr.Contains(cli.TheirsFlag)
	if ours && theirs {
		return 1, fmt.Errorf("specify only either --ours or --theirs")
	} else if !ours && !theirs {
		return 1, fmt.Errorf("--ours or --theirs must be supplied")
	}

	if apr.NArg() == 0 {
		return 1, fmt.Errorf("specify at least one table to resolve conflicts")
	}

	// get all tables in conflict
	root := roots.Working
	tbls := apr.Args
	if len(tbls) == 1 && tbls[0] == "." {
		if allTables, err := root.TablesInConflict(ctx); err != nil {
			return 1, err
		} else {
			tbls = allTables
		}
	}

	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, "does this matter?")
	err = ResolveConflicts(ctx, dEnv, dSess, root, dbName, ours, tbls)
	if err != nil {
		return 1, err
	}

	return 0, nil
}

func (d DoltConflictsCatFunc) String() string {
	childrenStrings := make([]string, len(d.children))

	for _, child := range d.children {
		childrenStrings = append(childrenStrings, child.String())
	}
	return fmt.Sprintf("DOLT_CONFLICTS_RESOLVE(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltConflictsCatFunc) Type() sql.Type {
	return sql.Text
}

func (d DoltConflictsCatFunc) IsNullable() bool {
	return false
}

func (d DoltConflictsCatFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltConflictsResolveFunc(children...)
}

func (d DoltConflictsCatFunc) Resolved() bool {
	for _, child := range d.Children() {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

func (d DoltConflictsCatFunc) Children() []sql.Expression {
	return d.children
}
