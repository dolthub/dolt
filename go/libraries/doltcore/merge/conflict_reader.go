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
	keyless bool
}

// NewConflictReader returns a new conflict reader for a given table
func NewConflictReader(ctx context.Context, tbl *doltdb.Table, tblName doltdb.TableName) (*ConflictReader, error) {
	base, sch, mergeSch, err := tbl.GetConflictSchemas(ctx, tblName) // tblName unused by old storage format
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

	var keyless bool
	if schema.IsKeyless(sch) {
		keyless = true
		readerSch, err = readerSch.AddColumn(
			schema.NewColumn(
				"base_cardinality",
				schema.DoltConflictsBaseCardinalityTag,
				types.UintKind,
				false),
			nil)
		if err != nil {
			return nil, err
		}
		readerSch, err = readerSch.AddColumn(
			schema.NewColumn(
				"our_cardinality",
				schema.DoltConflictsOurCardinalityTag,
				types.UintKind,
				false),
			nil)
		if err != nil {
			return nil, err
		}
		readerSch, err = readerSch.AddColumn(
			schema.NewColumn(
				"their_cardinality",
				schema.DoltConflictsTheirCardinalityTag,
				types.UintKind,
				false),
			nil)
		if err != nil {
			return nil, err
		}
	}

	_, confIdx, err := tbl.GetConflicts(ctx)
	if err != nil {
		return nil, err
	}

	if confIdx.Format() == types.Format_DOLT {
		panic("conflict reader not implemented for new storage format")
	}

	confData := durable.NomsMapFromConflictIndex(confIdx)
	confItr, err := confData.Iterator(ctx)
	if err != nil {
		return nil, err
	}

	return &ConflictReader{
		confItr: confItr,
		joiner:  joiner,
		sch:     readerSch,
		nbf:     tbl.Format(),
		keyless: keyless,
	}, nil
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
func (cr *ConflictReader) NextConflict(ctx context.Context) (row.Row, error) {
	key, value, err := cr.confItr.Next(ctx)

	if err != nil {
		return nil, err
	}

	if key == nil {
		return nil, io.EOF
	}

	keyTpl := key.(types.Tuple)
	conflict, err := conflict.ConflictFromTuple(value.(types.Tuple))
	if err != nil {
		return nil, err
	}

	var joinedRow row.Row
	if !cr.keyless {
		joinedRow, err = cr.pkJoinedRow(keyTpl, conflict)
	} else {
		joinedRow, err = cr.keylessJoinedRow(keyTpl, conflict)
	}
	if err != nil {
		return nil, err
	}

	ourDiffType := getDiffType(conflict.Base, conflict.Value)
	theirDiffType := getDiffType(conflict.Base, conflict.MergeValue)
	joinedRow, err = joinedRow.SetColVal(schema.DoltConflictsOurDiffTypeTag, types.String(ourDiffType), cr.sch)
	if err != nil {
		return nil, err
	}
	joinedRow, err = joinedRow.SetColVal(schema.DoltConflictsTheirDiffTypeTag, types.String(theirDiffType), cr.sch)
	if err != nil {
		return nil, err
	}

	return joinedRow, nil
}

func (cr *ConflictReader) pkJoinedRow(key types.Tuple, conflict conflict.Conflict) (row.Row, error) {
	var err error
	namedRows := make(map[string]row.Row)

	if !types.IsNull(conflict.Base) {
		namedRows[baseStr], err = row.FromNoms(cr.joiner.SchemaForName(baseStr), key, conflict.Base.(types.Tuple))
		if err != nil {
			return nil, err
		}
	}
	if !types.IsNull(conflict.Value) {
		namedRows[oursStr], err = row.FromNoms(cr.joiner.SchemaForName(oursStr), key, conflict.Value.(types.Tuple))
		if err != nil {
			return nil, err
		}
	}
	if !types.IsNull(conflict.MergeValue) {
		namedRows[theirsStr], err = row.FromNoms(cr.joiner.SchemaForName(theirsStr), key, conflict.MergeValue.(types.Tuple))
		if err != nil {
			return nil, err
		}
	}

	joinedRow, err := cr.joiner.Join(namedRows)
	if err != nil {
		return nil, err
	}

	return joinedRow, nil
}

func (cr *ConflictReader) keylessJoinedRow(key types.Tuple, conflict conflict.Conflict) (row.Row, error) {
	var err error
	namedRows := make(map[string]row.Row)
	var baseCard, ourCard, theirCard uint64

	if !types.IsNull(conflict.Base) {
		namedRows[baseStr], baseCard, err = row.KeylessRowsFromTuples(key, conflict.Base.(types.Tuple))
		if err != nil {
			return nil, err
		}
	}
	if !types.IsNull(conflict.Value) {
		namedRows[oursStr], ourCard, err = row.KeylessRowsFromTuples(key, conflict.Value.(types.Tuple))
		if err != nil {
			return nil, err
		}
	}
	if !types.IsNull(conflict.MergeValue) {
		namedRows[theirsStr], theirCard, err = row.KeylessRowsFromTuples(key, conflict.MergeValue.(types.Tuple))
		if err != nil {
			return nil, err
		}
	}

	joinedRow, err := cr.joiner.Join(namedRows)
	if err != nil {
		return nil, err
	}
	joinedRow, err = setCardinalities(types.Uint(baseCard), types.Uint(ourCard), types.Uint(theirCard), joinedRow, cr.sch)
	if err != nil {
		return nil, err
	}

	return joinedRow, nil
}

func setCardinalities(base, ours, theirs types.Uint, joinedRow row.Row, sch schema.Schema) (row.Row, error) {
	joinedRow, err := joinedRow.SetColVal(schema.DoltConflictsBaseCardinalityTag, base, sch)
	if err != nil {
		return nil, err
	}
	joinedRow, err = joinedRow.SetColVal(schema.DoltConflictsOurCardinalityTag, ours, sch)
	if err != nil {
		return nil, err
	}
	joinedRow, err = joinedRow.SetColVal(schema.DoltConflictsTheirCardinalityTag, theirs, sch)
	if err != nil {
		return nil, err
	}
	return joinedRow, nil
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
