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

package diff

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

// KeyedRows is one side of a diff: the rows of a table and the schema whose
// primary key orders them.
//
// Rekeying leaves out the rows whose new key is shared or NULL. They are kept
// as unpaired rows, so a diff can still report them.
type KeyedRows struct {
	Rows prolly.Map
	Sch  schema.Schema
	// Height is the commit height of Rows, or nil for the working set.
	Height *uint64

	// unpaired holds the rows left out of Rows, as stored before rekeying.
	unpaired prolly.Map
	// unpairedSch is the schema that the rows of unpaired read with.
	unpairedSch schema.Schema
	// unpairedCount is the number of rows in unpaired.
	unpairedCount int
	// rekeyer rekeys the rows of unpaired. It is nil when Rows was not rekeyed.
	rekeyer *durable.RowRekeyer
}

// CountUnpaired returns the number of unpaired rows of |from| and |to|.
func CountUnpaired(from, to KeyedRows) (int, int) {
	return from.unpairedCount, to.unpairedCount
}

// UnpairedSch returns the schema that the unpaired rows read with, or nil
// when Rows was not rekeyed.
func (r KeyedRows) UnpairedSch() schema.Schema {
	return r.unpairedSch
}

// IterUnpaired calls |cb| with each unpaired row, as stored. It calls nothing
// when Rows was not rekeyed.
//
// The rows read with UnpairedSch, so each value keeps its original type.
func (r KeyedRows) IterUnpaired(ctx context.Context, cb func(k, v val.Tuple) error) error {
	if r.rekeyer == nil {
		return nil
	}
	return durable.IterRows(ctx, r.unpaired, cb)
}

// UnpairedKey returns the key under Sch of the unpaired row |k| and |v|, which
// a range over Rows can match. A NULL in the key stays NULL.
func (r KeyedRows) UnpairedKey(ctx context.Context, k, v val.Tuple) (val.Tuple, error) {
	return r.rekeyer.Key(ctx, k, v)
}

// AlignPrimaryKeys returns |from| and |to| keyed by one primary key, rekeying
// at most one side.
//
// [prolly.DiffMaps] pairs rows by key bytes, so rows pair once the sides share
// a key. The rekeyed side leaves out rows the key cannot hold.
//
// The sides are returned unchanged when their columns map by tag or name but
// no key aligns, which makes the diff fuzzy.
func AlignPrimaryKeys(ctx context.Context, from, to KeyedRows) (KeyedRows, KeyedRows, error) {
	newSch, isFromRekeyed, err := AlignedSchema(from.Sch, to.Sch, from.Height, to.Height)
	if errors.Is(err, ErrPrimaryKeySetChanged) {
		if _, _, mapErr := schema.MapSchemaBasedOnTagAndName(from.Sch, to.Sch); mapErr == nil {
			return from, to, nil
		}
	}
	if err != nil || newSch == nil {
		return from, to, err
	}
	if isFromRekeyed {
		from, err = rekeyBy(ctx, from, newSch, to.Sch)
	} else {
		to, err = rekeyBy(ctx, to, newSch, from.Sch)
	}
	return from, to, err
}

// AlignedSchema returns the schema that one side of a diff takes to share the
// other side's primary key. It returns a nil schema when the sides store the
// same key, or when either side is keyless or empty.
//
// The bool reports whether the from side is the one rekeyed. The older side
// is rekeyed to the newer side's key when it stores every column of that key.
// Otherwise the newer side is rekeyed to the older key. The heights order the
// sides, so a diff and its reverse choose the same key.
//
// It returns an error wrapping ErrPrimaryKeySetChanged when neither side
// stores the other's key, or one that names a key column whose type changed.
// IsPrimaryKeyUnalignable reports both.
func AlignedSchema(fromSch, toSch schema.Schema, fromHeight, toHeight *uint64) (schema.Schema, bool, error) {
	if fromSch == nil || toSch == nil || schema.IsKeyless(fromSch) || schema.IsKeyless(toSch) {
		return nil, false, nil
	}
	// An empty schema is the missing side of an added or dropped table.
	isEmpty := fromSch.GetAllCols().Size() == 0 || toSch.GetAllCols().Size() == 0
	if isEmpty || isSameStoredKey(fromSch, toSch) {
		return nil, false, nil
	}
	olderSch, newerSch := fromSch, toSch
	isOlderFrom := heightOrder(fromHeight) <= heightOrder(toHeight)
	if !isOlderFrom {
		olderSch, newerSch = toSch, fromSch
	}
	if newSch, ok, err := schema.Rekeyed(olderSch, newerSch); err != nil || ok {
		return newSch, isOlderFrom, err
	}
	if newSch, ok, err := schema.Rekeyed(newerSch, olderSch); err != nil || ok {
		return newSch, !isOlderFrom, err
	}
	return nil, false, unalignableError(fromSch, toSch)
}

// heightOrder returns the order of commit height |h|. A nil height is the
// working set, which is newer than any commit.
func heightOrder(h *uint64) uint64 {
	if h == nil {
		return math.MaxUint64
	}
	return *h
}

// isSameStoredKey reports whether the primary keys of |fromSch| and |toSch|
// store the same bytes, column by column.
//
// Columns match by tag, or by name when neither tag appears on the other
// side, as when a table is dropped and recreated. A type change such as
// VARCHAR(20) to VARCHAR(30) keeps the bytes. A change that moves ENUM or SET
// members does not, since those bytes encode member positions.
func isSameStoredKey(fromSch, toSch schema.Schema) bool {
	fromCols, toCols := fromSch.GetPKCols().GetColumns(), toSch.GetPKCols().GetColumns()
	if len(fromCols) != len(toCols) {
		return false
	}
	for i, from := range fromCols {
		to := toCols[i]
		_, fromHasToTag := fromSch.GetAllCols().GetByTag(to.Tag)
		_, toHasFromTag := toSch.GetAllCols().GetByTag(from.Tag)
		isSameName := strings.EqualFold(from.Name, to.Name) && !fromHasToTag && !toHasFromTag
		isSameCol := from.Tag == to.Tag || isSameName
		members, isMember := from.MapMembers(to)
		isMoved := isMember && !members.IsIdentity()
		if !isSameCol || from.TypeInfo.Encoding() != to.TypeInfo.Encoding() || isMoved {
			return false
		}
	}
	return true
}

// rekeyBy returns |side| rekeyed to |newSch|, which holds the key of |other|.
//
// It returns an error when |side| stores a key column with another encoding.
func rekeyBy(ctx context.Context, side KeyedRows, newSch, other schema.Schema) (KeyedRows, error) {
	rk, ok := durable.NewRowRekeyer(side.Rows.NodeStore(), side.Sch, newSch)
	if !ok {
		return KeyedRows{}, unalignableError(side.Sch, other)
	}
	rows, unpaired, _, err := rk.RekeyMap(ctx, side.Rows)
	if err != nil {
		return KeyedRows{}, err
	}
	n, err := unpaired.Count()
	if err != nil {
		return KeyedRows{}, err
	}
	return KeyedRows{
		Rows:          rows,
		Sch:           newSch,
		Height:        side.Height,
		unpaired:      unpaired,
		unpairedSch:   side.Sch,
		unpairedCount: n,
		rekeyer:       rk,
	}, nil
}

// alignIndexes aligns the rows of the durable indexes |from| and |to|, which
// are keyed by |fromSch| and |toSch|, as AlignPrimaryKeys does.
func (td TableDelta) alignIndexes(ctx context.Context, from, to durable.Index, fromSch, toSch schema.Schema) (KeyedRows, KeyedRows, error) {
	fromRows, err := durable.ProllyMapFromIndex(from)
	if err != nil {
		return KeyedRows{}, KeyedRows{}, err
	}
	toRows, err := durable.ProllyMapFromIndex(to)
	if err != nil {
		return KeyedRows{}, KeyedRows{}, err
	}
	return AlignPrimaryKeys(ctx,
		KeyedRows{Rows: fromRows, Sch: fromSch, Height: td.FromHeight},
		KeyedRows{Rows: toRows, Sch: toSch, Height: td.ToHeight},
	)
}

// unalignableError returns the error for a diff whose sides share no primary
// key.
//
// It names a retyped key column when one exists, since that is the likeliest
// cause.
func unalignableError(fromSch, toSch schema.Schema) error {
	if from, to, ok := retypedKeyColumn(fromSch, toSch); ok {
		return fmt.Errorf("%w: column %s is %s on the from side and %s on the to side, so rows cannot be paired across the change",
			errPrimaryKeyTypeChanged, to.Name, from.TypeInfo.ToSqlType().String(), to.TypeInfo.ToSqlType().String())
	}
	return fmt.Errorf("%w from %s to %s, and neither side has every column of the other's primary key with the same type",
		ErrPrimaryKeySetChanged, primaryKeyString(fromSch), primaryKeyString(toSch))
}

// retypedKeyColumn returns the |fromSch| and |toSch| versions of the first
// primary key column of |toSch| with a changed type, and whether one exists.
// Columns match by tag, or else by name.
func retypedKeyColumn(fromSch, toSch schema.Schema) (schema.Column, schema.Column, bool) {
	fromCols := fromSch.GetAllCols()
	for _, to := range toSch.GetPKCols().GetColumns() {
		from, ok := fromCols.GetByTag(to.Tag)
		if !ok {
			from, ok = fromCols.GetByNameCaseInsensitive(to.Name)
		}
		if ok && !from.TypeInfo.ToSqlType().Equals(to.TypeInfo.ToSqlType()) {
			return from, to, true
		}
	}
	return schema.Column{}, schema.Column{}, false
}

// primaryKeyString returns the primary key columns of |sch| as "(a, b)".
func primaryKeyString(sch schema.Schema) string {
	return "(" + strings.Join(sch.GetPKCols().GetColumnNames(), ", ") + ")"
}
