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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
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

func resolveNewFormatConflicts(ctx *sql.Context, tbl *doltdb.Table, tblName string, sch schema.Schema, ours bool) (*doltdb.Table, error) {
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

	baseMap, err := getProllyRowMaps(ctx, tbl.ValueReadWriter(), tbl.NodeStore(), cnfArt.Metadata.BaseRootIsh, tblName)
	if err != nil {
		return nil, err
	}

	theirMap, err := getProllyRowMaps(ctx, tbl.ValueReadWriter(), tbl.NodeStore(), cnfArt.TheirRootIsh, tblName)
	if err != nil {
		return nil, err
	}

	// swap the maps when resolving with theirs
	if !ours {
		ourMap, theirMap = theirMap, ourMap
	}

	// resolve conflicts with left
	merged, _, err := prolly.MergeMaps(ctx, ourMap, theirMap, baseMap, func(left, right tree.Diff) (tree.Diff, bool) {
		if left.From != nil && ((left.To == nil) != (right.To == nil)) {
			return left, true
		}
		if bytes.Compare(left.To, right.To) == 0 {
			return right, true
		} else {
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
	return newTbl, nil
}

func resolveOldFormatConflicts(ctx *sql.Context, tbl *doltdb.Table, tblName string, sch schema.Schema, ours bool) (*doltdb.Table, error) {
	cnfReader, err := merge.NewConflictReader(ctx, tbl, tblName)
	if err != nil {
		return nil, err
	}

	joiner := cnfReader.GetJoiner()

	var pkTuples []types.Value
	vrw := tbl.ValueReadWriter()
	for {
		cnfRow, err := cnfReader.NextConflict(ctx)
		if err == io.EOF {
			break
		}
		cnfMap, err := joiner.Split(cnfRow)
		if err != nil {
			return nil, err
		}

		var row row.Row
		var k, v types.Value
		if ours {
			row = cnfMap["our"]
		} else {
			row = cnfMap["their"]
		}

		if row != nil {
			k, err = row.NomsMapKey(sch).Value(ctx)
			if err != nil {
				return nil, err
			}
			v, err = row.NomsMapValue(sch).Value(ctx)
			if err != nil {
				return nil, err
			}
			pkTuples = append(pkTuples, k, v)
		}
	}

	newMap, err := types.NewMap(ctx, vrw, pkTuples...)
	if err != nil {
		return nil, err
	}

	newTbl, err := tbl.UpdateNomsRows(ctx, newMap)
	if err != nil {
		return nil, err
	}
	return newTbl, nil
}

func resolveOldFormatConflicts2(ctx *sql.Context, tbl *doltdb.Table, tblName string, sch schema.Schema, ours bool) (*doltdb.Table, error) {
	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, "0.41.5")
	if dEnv == nil {
	}

	cnfReader, err := merge.NewConflictReader(ctx, tbl, tblName)
	if err != nil {
		return nil, err
	}

	joiner := cnfReader.GetJoiner()
	cnfSch := cnfReader.GetSchema()

	// this matters because only keyless tables will have a cardinality
	isKeyless := schema.IsKeyless(cnfSch)
	if isKeyless {
	}

	// Create new table editor
	edit, err := editor.NewTableEditor(ctx, tbl, sch, tblName, editor.Options{})
	if err != nil {
		return nil, err
	}

	// get relevant cardinality tags
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

		// get cardinality
		var ourCardinality, theirCardinality uint64
		if ourCardinalityVal, ok := cnfRow.GetColVal(ourCardinalityTag); ok {
			ourCardinality = uint64(ourCardinalityVal.(types.Uint))
		} else {
			panic("huh")
		}
		if theirCardinalityVal, ok := cnfRow.GetColVal(theirCardinalityTag); ok {
			theirCardinality = uint64(theirCardinalityVal.(types.Uint))
		} else {
			panic("huh")
		}

		rowDelta := 0
		var row row.Row
		if ours {
			row = cnfMap["our"]
		} else {
			row = cnfMap["their"]
			rowDelta = int(theirCardinality - ourCardinality)
		}

		if rowDelta > 0 {
			for i := 0; i < rowDelta; i++ {
				edit.InsertRow(ctx, row, func(newKeyString, indexName string, existingKey, existingVal types.Tuple, isPk bool) error {
					return nil
				})
			}
		} else {
			rowDelta *= -1
			for i := 0; i < rowDelta; i++ {
				edit.DeleteRow(ctx, row)
			}
		}
	}

	return edit.Table(ctx)
}

func ResolveConflicts(ctx *sql.Context, dSess *dsess.DoltSession, root *doltdb.RootValue, dbName string, ours bool, tblNames []string) error {
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
			newTbl, err = resolveNewFormatConflicts(ctx, tbl, tblName, sch, ours)
		} else {
			newTbl, err = resolveOldFormatConflicts2(ctx, tbl, tblName, sch, ours)
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

	err = ResolveConflicts(ctx, dSess, root, dbName, ours, tbls)
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
