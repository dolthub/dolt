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
	"slices"
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
// as unpaired rows, so a diff can still report them. When the sides share no
// primary key column, every row is unpaired.
type KeyedRows struct {
	Rows prolly.Map
	Sch  schema.Schema

	// Unpaired holds the rows left out of Rows, as stored before rekeying.
	Unpaired prolly.Map
	// UnpairedSch is the schema that the rows of Unpaired read with. It is nil
	// when Rows holds every row.
	UnpairedSch schema.Schema
	// UnpairedCount is the number of rows in Unpaired.
	UnpairedCount int
	// IsKeyUnshared reports that the sides share no primary key column, so Rows
	// is empty and Unpaired holds every row. A range over the key of one side
	// then cannot select the unpaired rows of the other.
	IsKeyUnshared bool
	// rekeyer rekeys the rows of Unpaired. It is nil when Rows was not rekeyed.
	rekeyer *durable.RowRekeyer
}

// IterUnpaired calls |cb| with each unpaired row, as stored. It calls nothing
// when no row is unpaired.
//
// The rows read with UnpairedSch, so each value keeps its original type.
func (r KeyedRows) IterUnpaired(ctx context.Context, cb func(k, v val.Tuple) error) error {
	if r.UnpairedCount == 0 {
		return nil
	}
	return durable.IterRows(ctx, r.Unpaired, cb)
}

// UnpairedKey returns the key under Sch of the unpaired row |k| and |v|, which
// a range over Rows can match. A NULL in the key stays NULL. It requires
// IsKeyUnshared to be false.
func (r KeyedRows) UnpairedKey(ctx context.Context, k, v val.Tuple) (val.Tuple, error) {
	return r.rekeyer.Key(ctx, k, v)
}

// AlignPrimaryKeys returns |from| and |to| keyed by the primary key that
// AlignedSchema returns.
//
// A side is rekeyed only when its key is not that one. [prolly.DiffMaps]
// pairs rows by key bytes, so rows pair once both sides share a key. A rekeyed
// side leaves out rows the key cannot hold.
//
// When the sides share no primary key column, no row can pair, so every row of
// both sides is returned unpaired. The sides are returned unchanged when their
// columns map by tag or name but no key aligns, which makes the diff fuzzy.
func AlignPrimaryKeys(ctx context.Context, from, to KeyedRows) (KeyedRows, KeyedRows, error) {
	keySch, err := AlignedSchema(from.Sch, to.Sch)
	if errors.Is(err, errNoSharedPrimaryKey) {
		if from, err = unpairEveryRow(ctx, from); err != nil {
			return KeyedRows{}, KeyedRows{}, err
		}
		to, err = unpairEveryRow(ctx, to)
		return from, to, err
	}
	if errors.Is(err, ErrPrimaryKeySetChanged) {
		if _, _, mapErr := schema.MapSchemaBasedOnTagAndName(from.Sch, to.Sch); mapErr == nil {
			return from, to, nil
		}
	}
	if err != nil || keySch == nil {
		return from, to, err
	}
	fromSch, toSch := from.Sch, to.Sch
	if !IsSameStoredKey(fromSch, keySch) {
		from, err = rekeyTo(ctx, from, keySch, toSch)
		if err != nil {
			return KeyedRows{}, KeyedRows{}, err
		}
	}
	if !IsSameStoredKey(toSch, keySch) {
		to, err = rekeyTo(ctx, to, keySch, fromSch)
		if err != nil {
			return KeyedRows{}, KeyedRows{}, err
		}
	}
	return from, to, nil
}

// rekeyTo returns |side| rekeyed to the primary key of |keySch|, and keeps
// the rows that the new key cannot hold as unpaired. |other| is the opposite
// schema, which the error names when |side| cannot be rekeyed.
func rekeyTo(ctx context.Context, side KeyedRows, keySch, other schema.Schema) (KeyedRows, error) {
	newSch, ok, err := schema.Rekeyed(side.Sch, keySch)
	if err != nil {
		return KeyedRows{}, err
	}
	if !ok {
		return KeyedRows{}, unalignableError(side.Sch, other)
	}
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
		Unpaired:      unpaired,
		UnpairedSch:   side.Sch,
		UnpairedCount: n,
		rekeyer:       rk,
	}, nil
}

// AlignedSchema returns the primary key that rows of |fromSch| and |toSch|
// pair by.
//
// The key holds the primary key columns of |toSch| that |fromSch| also
// stores, in the key order of |toSch|. A key column that |fromSch| lacks is
// left out.
//
// It returns nil when either side is keyless or empty. It returns
// errNoSharedPrimaryKey when no key column is shared, and an error that
// IsPrimaryKeyUnalignable reports when a shared one has another stored form
// or when a key column left out of the key has the name of a renamed column.
func AlignedSchema(fromSch, toSch schema.Schema) (schema.Schema, error) {
	if fromSch == nil || toSch == nil || schema.IsKeyless(fromSch) || schema.IsKeyless(toSch) {
		return nil, nil
	}
	// An empty schema is the missing side of an added or dropped table.
	if fromSch.GetAllCols().Size() == 0 || toSch.GetAllCols().Size() == 0 {
		return nil, nil
	}
	var tags []uint64
	for _, tc := range toSch.GetPKCols().GetColumns() {
		fc, ok := schema.MatchingColumn(fromSch, toSch, tc)
		if !ok {
			// A key column that |fromSch| lacks is new only when no column of
			// |fromSch| has its name. Otherwise that name moved to a column
			// that |toSch| renamed.
			if _, held := fromSch.GetAllCols().GetByNameCaseInsensitive(tc.Name); held {
				return nil, unalignableError(fromSch, toSch)
			}
			continue
		}
		if fc.TypeInfo.Encoding() != tc.TypeInfo.Encoding() {
			return nil, unalignableError(fromSch, toSch)
		}
		tags = append(tags, tc.Tag)
	}
	if len(tags) == 0 {
		return nil, errNoSharedPrimaryKey
	}
	// The key flags cover only the identity columns, since [schema.NewSchema]
	// derives the primary key from them.
	cols := toSch.GetAllCols().GetColumns()
	newCols := make([]schema.Column, len(cols))
	for i, col := range cols {
		col.IsPartOfPK = slices.Contains(tags, col.Tag)
		newCols[i] = col
	}
	allCols := schema.NewColCollection(newCols...)
	ordinals := make([]int, len(tags))
	for i, tag := range tags {
		ordinals[i] = allCols.TagToIdx[tag]
	}
	return schema.NewSchema(allCols, ordinals, toSch.GetCollation(), nil, nil)
}

// IsSameStoredKey reports whether the primary keys of |fromSch| and |toSch|
// store the same bytes, column by column.
//
// Columns match as [schema.MatchingColumn] does. A type change such as
// VARCHAR(20) to VARCHAR(30) keeps the bytes. A change that moves ENUM or SET
// members does not, since those bytes encode member positions.
func IsSameStoredKey(fromSch, toSch schema.Schema) bool {
	fromCols, toCols := fromSch.GetPKCols().GetColumns(), toSch.GetPKCols().GetColumns()
	if len(fromCols) != len(toCols) {
		return false
	}
	for i, to := range toCols {
		from, ok := schema.MatchingColumn(fromSch, toSch, to)
		if !ok || from.Tag != fromCols[i].Tag || from.TypeInfo.Encoding() != to.TypeInfo.Encoding() {
			return false
		}
		if members, isMember := from.MapMembers(to); isMember && !members.IsIdentity() {
			return false
		}
	}
	return true
}

// unpairEveryRow returns |side| with every row unpaired, for sides that share
// no primary key column. Rows is left empty, since [prolly.DiffMaps] would
// pair rows whose unrelated keys have equal bytes.
func unpairEveryRow(ctx context.Context, side KeyedRows) (KeyedRows, error) {
	n, err := side.Rows.Count()
	if err != nil {
		return KeyedRows{}, err
	}
	keyDesc, valDesc := side.Rows.Descriptors()
	empty, err := prolly.NewMapFromTuples(ctx, side.Rows.NodeStore(), keyDesc, valDesc)
	if err != nil {
		return KeyedRows{}, err
	}
	return KeyedRows{
		Rows:          empty,
		Sch:           side.Sch,
		Unpaired:      side.Rows,
		UnpairedSch:   side.Sch,
		UnpairedCount: n,
		IsKeyUnshared: true,
	}, nil
}

// alignIndexes aligns the rows of the durable indexes |from| and |to|, which
// are keyed by |fromSch| and |toSch|, as AlignPrimaryKeys does.
func alignIndexes(ctx context.Context, from, to durable.Index, fromSch, toSch schema.Schema) (KeyedRows, KeyedRows, error) {
	fromRows, err := durable.ProllyMapFromIndex(from)
	if err != nil {
		return KeyedRows{}, KeyedRows{}, err
	}
	toRows, err := durable.ProllyMapFromIndex(to)
	if err != nil {
		return KeyedRows{}, KeyedRows{}, err
	}
	return AlignPrimaryKeys(ctx,
		KeyedRows{Rows: fromRows, Sch: fromSch},
		KeyedRows{Rows: toRows, Sch: toSch},
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
