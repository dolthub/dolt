// Copyright 2019 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/rowconv"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	oursStr   = "our"
	theirsStr = "their"
	baseStr   = "base"
)

const (
	ConflictDiffTypeAdded    = "added"
	ConflictDiffTypeModified = "modified"
	ConflictDiffTypeRemoved  = "removed"
)

// ConflictReader is a class providing a NextConflict function which can be used in a pipeline as a pipeline.SourceFunc,
// or it can be used to read each conflict
type ConflictReader struct {
	confItr types.MapIterator
	joiner  *rowconv.Joiner
	sch     schema.Schema
	nbf     *types.NomsBinFormat
}

// NewConflictReader returns a new conflict reader for a given table
func NewConflictReader(ctx context.Context, tbl *doltdb.Table) (*ConflictReader, error) {
	base, sch, mergeSch, err := tbl.GetConflictSchemas(ctx, "") // tblName unused by old storage format
	if err != nil {
		return nil, err
	}
	if base == nil || sch == nil || mergeSch == nil {
		base, err = tbl.GetSchema(ctx)
		sch, mergeSch = base, base
	}
	if err != nil {
		return nil, err
	}

	joiner, err := rowconv.NewJoiner(
		[]rowconv.NamedSchema{
			{Name: baseStr, Sch: base},
			{Name: oursStr, Sch: sch},
			{Name: theirsStr, Sch: mergeSch},
		}, map[string]rowconv.ColNamingFunc{
			baseStr:   func(colName string) string { return baseStr + "_" + colName },
			oursStr:   func(colName string) string { return oursStr + "_" + colName },
			theirsStr: func(colName string) string { return theirsStr + "_" + colName },
		},
	)
	if err != nil {
		return nil, err
	}

	readerSch := joiner.GetSchema()
	readerSch, err = readerSch.AddColumn(schema.NewColumn("our_diff_type", schema.DoltConflictsOurDiffTypeTag, types.StringKind, false), nil)
	if err != nil {
		return nil, err
	}
	readerSch, err = readerSch.AddColumn(schema.NewColumn("their_diff_type", schema.DoltConflictsTheirDiffTypeTag, types.StringKind, false), nil)
	if err != nil {
		return nil, err
	}

	_, confIdx, err := tbl.GetConflicts(ctx)
	if err != nil {
		return nil, err
	}

	if confIdx.Format() == types.Format_DOLT_1 {
		panic("conflict reader not implemented for new storage format")
	}

	confData := durable.NomsMapFromConflictIndex(confIdx)
	confItr, err := confData.Iterator(ctx)
	if err != nil {
		return nil, err
	}

	return &ConflictReader{confItr: confItr, joiner: joiner, sch: readerSch, nbf: tbl.Format()}, nil
}

// GetSchema gets the schema of the rows that this reader will return
func (cr *ConflictReader) GetSchema() schema.Schema {
	return cr.sch
}

// GetJoiner returns the joiner used to join a row with its base, and merge versions
func (cr *ConflictReader) GetJoiner() *rowconv.Joiner {
	return cr.joiner
}

// NextConflict can be called successively to retrieve the conflicts in a table.  Once all conflicts have been returned
// io.EOF will be returned in the error field.  This can be used in a pipeline, or to iterate through all the conflicts
// in a table.
func (cr *ConflictReader) NextConflict(ctx context.Context) (row.Row, pipeline.ImmutableProperties, error) {
	key, value, err := cr.confItr.Next(ctx)

	if err != nil {
		return nil, pipeline.NoProps, err
	}

	if key == nil {
		return nil, pipeline.NoProps, io.EOF
	}

	keyTpl := key.(types.Tuple)
	conflict, err := conflict.ConflictFromTuple(value.(types.Tuple))

	if err != nil {
		return nil, pipeline.NoProps, err
	}

	namedRows := make(map[string]row.Row)
	if !types.IsNull(conflict.Base) {
		namedRows[baseStr], err = row.FromNoms(cr.joiner.SchemaForName(baseStr), keyTpl, conflict.Base.(types.Tuple))

		if err != nil {
			return nil, pipeline.NoProps, err
		}
	}

	if !types.IsNull(conflict.Value) {
		namedRows[oursStr], err = row.FromNoms(cr.joiner.SchemaForName(oursStr), keyTpl, conflict.Value.(types.Tuple))

		if err != nil {
			return nil, pipeline.NoProps, err
		}
	}

	if !types.IsNull(conflict.MergeValue) {
		namedRows[theirsStr], err = row.FromNoms(cr.joiner.SchemaForName(theirsStr), keyTpl, conflict.MergeValue.(types.Tuple))

		if err != nil {
			return nil, pipeline.NoProps, err
		}
	}

	joinedRow, err := cr.joiner.Join(namedRows)

	ourDiffType := getDiffType(conflict.Base, conflict.Value)
	theirDiffType := getDiffType(conflict.Base, conflict.MergeValue)
	joinedRow, err = joinedRow.SetColVal(schema.DoltConflictsOurDiffTypeTag, types.String(ourDiffType), cr.sch)
	joinedRow, err = joinedRow.SetColVal(schema.DoltConflictsTheirDiffTypeTag, types.String(theirDiffType), cr.sch)

	if err != nil {
		return nil, pipeline.NoProps, err
	}

	return joinedRow, pipeline.NoProps, nil
}

func getDiffType(base types.Value, other types.Value) string {
	if types.IsNull(base) {
		return ConflictDiffTypeAdded
	} else if types.IsNull(other) {
		return ConflictDiffTypeRemoved
	}

	return ConflictDiffTypeModified
}

// GetKeyForConflicts returns the pk for a conflict row
func (cr *ConflictReader) GetKeyForConflict(ctx context.Context, r row.Row) (types.Value, error) {
	rows, err := cr.joiner.Split(r)

	if err != nil {
		return nil, err
	}

	for rowType, r := range rows {
		key, err := r.NomsMapKey(cr.joiner.SchemaForName(rowType)).Value(ctx)

		if err != nil {
			return nil, err
		}

		return key, nil
	}

	return nil, errors.New("could not determine key")
}

// Close should release resources being held
func (cr *ConflictReader) Close() error {
	return nil
}
