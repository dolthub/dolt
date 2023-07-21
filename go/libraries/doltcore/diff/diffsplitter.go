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

package diff

import (
	"context"
	"errors"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
)

const (
	fromPrefix = "from_"
	toPrefix   = "to_"

	addedStr    = "added"
	modifiedStr = "modified"
	removedStr  = "removed"
)

type DiffSplitter struct {
	// results schema of dolt_diff(...)
	// sql table function
	querySch sql.Schema
	// output schema for CLI diff
	targetSch sql.Schema
	// maps querySch fields to targetSch
	queryToTarget []int
	// divides "from_..." and "to_..." cols
	splitIdx int
}

type RowDiff struct {
	Row      sql.Row
	RowDiff  ChangeType
	ColDiffs []ChangeType
}

// NewDiffSplitter returns a splitter that knows how to split unified diff query rows.
// |querySch| is the result schema from the dolt_dif(...) table function
// it contains "from_..." and "to..." columns corresponding to the "from"
// and "to" schemas used to generate the diff.
// |targetSch| is the output schema used to print the diff and is computed
// as the union schema of the "from" and "to" schemas.

func NewDiffSplitter(querySch sql.Schema, targetSch sql.Schema) (*DiffSplitter, error) {
	split, err := findDiffSchemaSplit(querySch)
	if err != nil {
		return nil, err
	}

	qtt, err := mapQuerySchemaToTargetSchema(querySch, targetSch)
	if err != nil {
		return nil, err
	}

	return &DiffSplitter{
		querySch:      querySch,
		targetSch:     targetSch,
		queryToTarget: qtt,
		splitIdx:      split,
	}, nil
}

func findDiffSchemaSplit(querySch sql.Schema) (int, error) {
	split := -1
	for i, col := range querySch {
		if strings.HasPrefix(col.Name, fromPrefix) {
			if split >= 0 { // seen first "to_..." col
				return 0, errors.New("interleaved 'from' and 'to' cols")
			}
		} else if strings.HasPrefix(col.Name, toPrefix) {
			if split < 0 { // |i| is first "to_..." col
				split = i
			}
		} else if col.Name == "diff_type" {
			if split < 0 {
				split = i
			}
		} else {
			return 0, errors.New("expected column prefix of 'to_' or 'from_' (" + col.Name + ")")
		}
	}
	return split, nil
}

func mapQuerySchemaToTargetSchema(query, target sql.Schema) (mapping []int, err error) {
	last := query[len(query)-1]
	if last.Name != "diff_type" {
		return nil, errors.New("expected last diff column to be 'diff_type'")
	}
	query = query[:len(query)-1]

	mapping = make([]int, len(query))
	for i, col := range query {
		if strings.HasPrefix(col.Name, fromPrefix) {
			base := col.Name[len(fromPrefix):]
			mapping[i] = target.IndexOfColName(base)
		} else if strings.HasPrefix(col.Name, toPrefix) {
			base := col.Name[len(toPrefix):]
			mapping[i] = target.IndexOfColName(base)
		} else {
			return nil, errors.New("expected column prefix of 'to_' or 'from_' (" + col.Name + ")")
		}
	}
	return
}

func mapToAndFromColumns(query sql.Schema) (mapping []int, err error) {
	last := query[len(query)-1]
	if last.Name != "diff_type" {
		return nil, errors.New("expected last diff column to be 'diff_type'")
	}
	query = query[:len(query)-1]

	mapping = make([]int, len(query))
	for i, col := range query {
		if strings.HasPrefix(col.Name, fromPrefix) {
			// map "from_..." column to "to_..." column
			base := col.Name[len(fromPrefix):]
			mapping[i] = query.IndexOfColName(toPrefix + base)
		} else if strings.HasPrefix(col.Name, toPrefix) {
			// map "to_..." column to "from_..." column
			base := col.Name[len(toPrefix):]
			mapping[i] = query.IndexOfColName(fromPrefix + base)
		} else {
			return nil, errors.New("expected column prefix of 'to_' or 'from_' (" + col.Name + ")")
		}
	}
	// |mapping| will contain -1 for unmapped columns
	return
}

func (ds DiffSplitter) SplitDiffResultRow(row sql.Row) (from, to RowDiff, err error) {
	from = RowDiff{ColDiffs: make([]ChangeType, len(ds.targetSch))}
	to = RowDiff{ColDiffs: make([]ChangeType, len(ds.targetSch))}

	diffType := row[len(row)-1]
	row = row[:len(row)-1]

	switch diffType.(string) {
	case removedStr:
		from.Row = make(sql.Row, len(ds.targetSch))
		from.RowDiff = Removed
		for i := 0; i < ds.splitIdx; i++ {
			j := ds.queryToTarget[i]
			// skip any columns that aren't mapped
			if j < 0 {
				continue
			}
			from.Row[j] = row[i]
			from.ColDiffs[j] = Removed
		}

	case addedStr:
		to.Row = make(sql.Row, len(ds.targetSch))
		to.RowDiff = Added
		for i := ds.splitIdx; i < len(row); i++ {
			j := ds.queryToTarget[i]
			// skip any columns that aren't mapped
			if j < 0 {
				continue
			}
			to.Row[j] = row[i]
			to.ColDiffs[j] = Added
		}

	case modifiedStr:
		from.Row = make(sql.Row, len(ds.targetSch))
		from.RowDiff = ModifiedOld
		for i := 0; i < ds.splitIdx; i++ {
			j := ds.queryToTarget[i]
			// skip any columns that aren't mapped
			if j < 0 {
				continue
			}
			from.Row[j] = row[i]
		}
		to.Row = make(sql.Row, len(ds.targetSch))
		to.RowDiff = ModifiedNew
		for i := ds.splitIdx; i < len(row); i++ {
			j := ds.queryToTarget[i]
			to.Row[j] = row[i]
		}
		// now do field-wise comparison
		var cmp int
		for i, col := range ds.targetSch {
			cmp, err = col.Type.Compare(from.Row[i], to.Row[i])
			if err != nil {
				return RowDiff{}, RowDiff{}, err
			} else if cmp != 0 {
				from.ColDiffs[i] = ModifiedOld
				to.ColDiffs[i] = ModifiedNew
			} else {
				from.ColDiffs[i] = None
				to.ColDiffs[i] = None
			}
		}

	default:
		panic("unknown diff type " + diffType.(string))
	}
	return
}

// MaybeResolveRoot returns a root value and true if the a commit exists for given spec string; nil and false if it does not exist.
// todo: distinguish between non-existent CommitSpec and other errors, don't assume non-existent
func MaybeResolveRoot(ctx context.Context, rsr env.RepoStateReader, doltDB *doltdb.DoltDB, spec string) (*doltdb.RootValue, bool) {
	cs, err := doltdb.NewCommitSpec(spec)
	if err != nil {
		// it's non-existent CommitSpec
		return nil, false
	}

	headRef, err := rsr.CWBHeadRef()
	if err != nil {
		return nil, false
	}
	cm, err := doltDB.Resolve(ctx, cs, headRef)
	if err != nil {
		return nil, false
	}

	root, err := cm.GetRootValue(ctx)
	if err != nil {
		return nil, false
	}

	return root, true
}
