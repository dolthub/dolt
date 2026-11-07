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
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

// KeyedRows is one side of a diff: a table's row map and the schema it is
// keyed by.
//
// It also holds the rows that share a key under that schema, which Rows
// cannot hold, so that a diff can still report them.
type KeyedRows struct {
	Rows prolly.Map
	Sch  schema.Schema

	// unpaired holds the rows kept apart, as they were stored before rekeying.
	unpaired prolly.Map
	// rekeyer keys unpaired entries like Rows. It is nil when there are none.
	rekeyer *durable.RowRekeyer
}

// UnpairedCount returns how many rows AlignPrimaryKeys kept apart from Rows.
func (r KeyedRows) UnpairedCount() (int, error) {
	if r.rekeyer == nil {
		return 0, nil
	}
	return r.unpaired.Count()
}

// CountUnpaired returns the KeyedRows.UnpairedCount of |from| and of |to|.
func CountUnpaired(from, to KeyedRows) (int, int, error) {
	fromN, err := from.UnpairedCount()
	if err != nil {
		return 0, 0, err
	}
	toN, err := to.UnpairedCount()
	return fromN, toN, err
}

// IterUnpaired calls |cb| with each row kept apart from Rows, keyed as Rows
// is.
//
// A diff reports those rows as removed or added.
func (r KeyedRows) IterUnpaired(ctx context.Context, cb func(k, v val.Tuple) error) error {
	if r.rekeyer == nil {
		return nil
	}
	return durable.IterRows(ctx, r.unpaired, func(k, v val.Tuple) error {
		nk, nv, err := r.rekeyer.Rekey(ctx, k, v)
		if err != nil {
			return err
		}
		return cb(nk, nv)
	})
}

// AlignPrimaryKeys returns |from| and |to| keyed by one primary key. It
// prefers the key of |to|, and uses the key of |from| when only that key fits
// or when it pairs rows the key of |to| cannot. Rows that share a new key are
// kept apart, as KeyedRows.IterUnpaired describes.
//
// [prolly.DiffMaps] pairs rows by key bytes, so a diff across a primary key
// change needs both sides keyed alike. The key of |to| comes first because a
// diff reports the to side.
//
// It returns an error when neither key fits or a new key would be NULL. The
// error wraps ErrPrimaryKeySetChanged unless a key column changed type.
func AlignPrimaryKeys(ctx context.Context, from, to KeyedRows) (KeyedRows, KeyedRows, error) {
	if !needsAlignment(from.Sch, to.Sch) {
		return from, to, nil
	}
	fromAligned, isFromAligned, err := rekeyBy(ctx, from, to.Sch)
	if err != nil {
		return KeyedRows{}, KeyedRows{}, rekeyError(from.Sch, to.Sch, "from", to.Sch, err)
	}
	if isFromAligned && fromAligned.rekeyer == nil {
		return fromAligned, to, nil
	}

	toAligned, isToAligned, err := rekeyBy(ctx, to, from.Sch)
	switch {
	case isFromAligned && errors.Is(err, durable.ErrRekeyNullKey):
		// The from rows still pair by the to key, with some left unpaired.
		return fromAligned, to, nil
	case err != nil:
		return KeyedRows{}, KeyedRows{}, rekeyError(from.Sch, to.Sch, "to", from.Sch, err)
	case isToAligned && (!isFromAligned || toAligned.rekeyer == nil):
		return from, toAligned, nil
	case isFromAligned:
		return fromAligned, to, nil
	}
	return KeyedRows{}, KeyedRows{}, unalignableError(from.Sch, to.Sch)
}

// rekeyBy returns |side| keyed by the primary key of |target|. It returns
// false when |side| lacks a column of that key or stores it with another type.
func rekeyBy(ctx context.Context, side KeyedRows, target schema.Schema) (KeyedRows, bool, error) {
	newSch, ok, err := schema.RekeyedSchema(side.Sch, target)
	if err != nil || !ok {
		return KeyedRows{}, false, err
	}
	rk, ok := durable.NewRowRekeyer(side.Rows.NodeStore(), side.Sch, newSch)
	if !ok {
		return KeyedRows{}, false, nil
	}
	rows, unpaired, err := rk.RekeyMap(ctx, side.Rows)
	if err != nil {
		return KeyedRows{}, false, err
	}
	aligned := KeyedRows{Rows: rows, Sch: newSch}
	n, err := unpaired.Count()
	if err != nil {
		return KeyedRows{}, false, err
	}
	if n > 0 {
		aligned.unpaired, aligned.rekeyer = unpaired, rk
	}
	return aligned, true, nil
}

// rekeyError explains why the |sideName| side of a diff from |fromSch| to
// |toSch| could not take the primary key of |keySch|. It wraps |err| and
// ErrPrimaryKeySetChanged.
func rekeyError(fromSch, toSch schema.Schema, sideName string, keySch schema.Schema, err error) error {
	return fmt.Errorf("%w from %s to %s, and the %s rows cannot be keyed by %s: %w",
		ErrPrimaryKeySetChanged, primaryKeyString(fromSch), primaryKeyString(toSch), sideName, primaryKeyString(keySch), err)
}

// alignIndexes is AlignPrimaryKeys for callers that hold durable indexes, as
// table deltas do.
func alignIndexes(ctx context.Context, from, to durable.Index, fromSch, toSch schema.Schema) (KeyedRows, KeyedRows, error) {
	fromRows, err := durable.ProllyMapFromIndex(from)
	if err != nil {
		return KeyedRows{}, KeyedRows{}, err
	}
	toRows, err := durable.ProllyMapFromIndex(to)
	if err != nil {
		return KeyedRows{}, KeyedRows{}, err
	}
	return AlignPrimaryKeys(ctx, KeyedRows{Rows: fromRows, Sch: fromSch}, KeyedRows{Rows: toRows, Sch: toSch})
}

// CanAlignPrimaryKeys reports whether a diff from |fromSch| to |toSch| can
// pair rows by key. That holds when the keys are the same columns, possibly
// retyped with the same encoding as from VARCHAR(100) to VARCHAR(101), or
// when AlignPrimaryKeys can rekey one side.
//
// It judges from the schemas alone, so that a history walk can decide whether
// to diff a commit pair before loading its rows.
func CanAlignPrimaryKeys(fromSch, toSch schema.Schema) bool {
	if needsAlignment(fromSch, toSch) {
		return isRekeyable(fromSch, toSch) || isRekeyable(toSch, fromSch)
	}
	if schema.ComparePrimaryKeys(fromSch, toSch) != schema.PrimaryKeysDiffer {
		return true
	}
	return keyColumnsMatch(fromSch, toSch, func(from, to schema.Column) bool {
		return from.Tag == to.Tag
	})
}

// needsAlignment reports whether keyed schemas |fromSch| and |toSch| store
// different key bytes, so that one side must be rekeyed before a diff.
//
// Keys with the same columns, by tag or name, and the same encodings store
// the same bytes, even across a type change such as VARCHAR(20) to
// VARCHAR(30).
func needsAlignment(fromSch, toSch schema.Schema) bool {
	if fromSch == nil || toSch == nil || schema.IsKeyless(fromSch) || schema.IsKeyless(toSch) {
		return false
	}
	if schema.ComparePrimaryKeys(fromSch, toSch) != schema.PrimaryKeysDiffer {
		return false
	}
	return !keyColumnsMatch(fromSch, toSch, func(from, to schema.Column) bool {
		return from.Tag == to.Tag || strings.EqualFold(from.Name, to.Name)
	})
}

// keyColumnsMatch reports whether two primary keys hold the same columns, in
// order and with the same encodings, where |isSame| decides column identity.
func keyColumnsMatch(fromSch, toSch schema.Schema, isSame func(from, to schema.Column) bool) bool {
	fromCols, toCols := fromSch.GetPKCols().GetColumns(), toSch.GetPKCols().GetColumns()
	if len(fromCols) != len(toCols) {
		return false
	}
	for i := range fromCols {
		if !isSame(fromCols[i], toCols[i]) || fromCols[i].TypeInfo.Encoding() != toCols[i].TypeInfo.Encoding() {
			return false
		}
	}
	return true
}

// isRekeyable reports, from the schemas alone, whether a table stored with
// |sch| can take the primary key of |target|.
func isRekeyable(sch, target schema.Schema) bool {
	_, ok, err := schema.RekeyedSchema(sch, target)
	return err == nil && ok
}

// unalignableError returns the error for a diff whose sides cannot take each
// other's primary key.
//
// It names a retyped key column when there is one, since that is the likeliest
// cause.
func unalignableError(fromSch, toSch schema.Schema) error {
	if from, to, ok := retypedKeyColumn(fromSch, toSch); ok {
		return fmt.Errorf("%w: column %s is %s on the from side and %s on the to side, so rows cannot be paired across the change",
			errPrimaryKeyTypeChanged, to.Name, from.TypeInfo.ToSqlType().String(), to.TypeInfo.ToSqlType().String())
	}
	return fmt.Errorf("%w from %s to %s, and neither side has every column of the other's primary key with the same type",
		ErrPrimaryKeySetChanged, primaryKeyString(fromSch), primaryKeyString(toSch))
}

// retypedKeyColumn returns both versions of a primary key column of |toSch|
// whose type differs in |fromSch|. Columns match by tag, or else by name.
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
	cols := sch.GetPKCols().GetColumns()
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = col.Name
	}
	return "(" + strings.Join(names, ", ") + ")"
}
